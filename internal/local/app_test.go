package local

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	cachehttp "github.com/vearutop/gocacheprogd/internal/http"
)

func TestApp_IterateInputAndResponses(t *testing.T) {
	resps := make(chan cacheprog.Response, 10)
	proxy, err := NewProxy(t.TempDir(), nil, resps)
	require.NoError(t, err)

	body := []byte("body-1")

	var input bytes.Buffer
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       1,
		Command:  cacheprog.CmdPut,
		ActionID: "actionId1",
		OutputID: "outputId1",
		BodySize: int64(len(body)),
	})
	writeJSONLine(t, &input, body)
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       2,
		Command:  cacheprog.CmdGet,
		ActionID: "actionId1",
	})
	writeJSONLine(t, &input, cacheprog.Request{
		ID:      3,
		Command: cacheprog.CmdClose,
	})

	var output bytes.Buffer
	var logDump bytes.Buffer

	app := NewApp(&input, &output, proxy, resps, &logDump)

	done := make(chan struct{})
	go func() {
		app.IterateResponses()
		close(done)
	}()

	require.NoError(t, app.IterateInput())
	require.NoError(t, proxy.Close())
	close(resps)
	<-done

	dec := json.NewDecoder(bytes.NewReader(output.Bytes()))

	var handshake cacheprog.Response
	require.NoError(t, dec.Decode(&handshake))
	require.Equal(t, []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}, handshake.KnownCommands)

	var putResp cacheprog.Response
	require.NoError(t, dec.Decode(&putResp))
	require.Equal(t, int64(1), putResp.ID)
	require.False(t, putResp.Miss)
	require.Equal(t, "outputId1", putResp.OutputID)
	require.Equal(t, int64(len(body)), putResp.Size)
	require.NotEmpty(t, putResp.DiskPath)

	var getResp cacheprog.Response
	require.NoError(t, dec.Decode(&getResp))
	require.Equal(t, int64(2), getResp.ID)
	require.False(t, getResp.Miss)
	require.Equal(t, "outputId1", getResp.OutputID)
	require.Equal(t, int64(len(body)), getResp.Size)
	require.Equal(t, putResp.DiskPath, getResp.DiskPath)

	var lines []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logDump.Bytes()), []byte{'\n'}) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(line, &entry))
		lines = append(lines, entry)
	}

	require.Len(t, lines, 5)
	require.Equal(t, "put", lines[0]["Command"])
	require.Equal(t, "get", lines[1]["Command"])
	require.Equal(t, "close", lines[2]["Command"])
	require.Equal(t, "outputId1", lines[3]["OutputID"])
	require.Equal(t, "outputId1", lines[4]["OutputID"])
}

func TestApp_IterateInput_RejectsDeclaredBodySizeMismatch(t *testing.T) {
	resps := make(chan cacheprog.Response, 1)
	proxy, err := NewProxy(t.TempDir(), nil, resps)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	var input bytes.Buffer
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       1,
		Command:  cacheprog.CmdPut,
		ActionID: "actionId1",
		OutputID: "outputId1",
		BodySize: 10,
	})
	writeJSONLine(t, &input, []byte("short"))

	app := NewApp(&input, &bytes.Buffer{}, proxy, resps, nil)

	err = app.IterateInput()
	require.Error(t, err)
	require.Contains(t, err.Error(), "only got 5 bytes of declared 10")
}

func TestApp_E2E_DirectWithRemoteCompression(t *testing.T) {
	// Seed a real remote store that keeps cache entries compressed.
	remoteStore, err := NewStore(t.TempDir(), true)
	require.NoError(t, err)

	remoteBody := strings.Repeat("remote-body-", 32)
	now := time.Now()
	remoteItem := cache.ResponseItem{
		ActionID: "actionIdRemote",
		OutputID: "outputIdRemote",
		Size:     int64(len(remoteBody)),
		Time:     &now,
	}
	remoteItem.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(remoteBody)), nil
	})
	require.NoError(t, remoteStore.Put(cache.Response{Items: []cache.ResponseItem{remoteItem}}))

	var remoteSeed cache.ResponseItem
	require.NoError(t, remoteStore.Get(cache.Request{ActionIDs: []string{"actionIdRemote"}}, func(resp cache.ResponseItem) {
		remoteSeed = resp
	}))
	require.False(t, remoteSeed.Miss)
	require.True(t, remoteSeed.IsCompressed)
	require.Equal(t, int64(35), remoteSeed.WireSize)

	// Expose the remote store over HTTP and create a direct-mode local proxy.
	remoteSrv := httptest.NewServer(cachehttp.NewHandler(remoteStore, ""))
	t.Cleanup(remoteSrv.Close)

	upstream, err := cachehttp.NewClient(remoteSrv.URL, "")
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 10)
	proxy, err := NewProxy(t.TempDir(), upstream, resps)
	require.NoError(t, err)

	// Preload the existing remote entry so the app-level "get" stays on the
	// normal direct-mode path without racing an async remote miss and close.
	require.NoError(t, proxy.Preload(cache.PreloadRequest{MaxSize: 1_000_000}))

	putBody := []byte(strings.Repeat("put-body-", 40))[:343]

	// Drive the extracted stdio app with:
	// 1. get a preloaded remote entry
	// 2. put a new compressible local entry
	// 3. get the just-put local entry
	// 4. close the app loop
	var input bytes.Buffer
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       1,
		Command:  cacheprog.CmdGet,
		ActionID: "actionIdRemote",
	})
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       2,
		Command:  cacheprog.CmdPut,
		ActionID: "actionIdPut",
		OutputID: "outputIdPut",
		BodySize: int64(len(putBody)),
	})
	writeJSONLine(t, &input, putBody)
	writeJSONLine(t, &input, cacheprog.Request{
		ID:       3,
		Command:  cacheprog.CmdGet,
		ActionID: "actionIdPut",
	})
	writeJSONLine(t, &input, cacheprog.Request{
		ID:      4,
		Command: cacheprog.CmdClose,
	})

	var output bytes.Buffer
	app := NewApp(&input, &output, proxy, resps, nil)

	// Run the response loop concurrently, like main.go does, then process input.
	done := make(chan struct{})
	go func() {
		app.IterateResponses()
		close(done)
	}()

	require.NoError(t, app.IterateInput())
	require.NoError(t, proxy.Close())
	close(resps)
	<-done

	// Decode the cacheprog protocol output and verify responses by request ID,
	// since get/put replies are allowed to arrive out of order.
	dec := json.NewDecoder(bytes.NewReader(output.Bytes()))

	var handshake cacheprog.Response
	require.NoError(t, dec.Decode(&handshake))
	require.Equal(t, []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}, handshake.KnownCommands)

	var responses []cacheprog.Response
	for i := 0; i < 3; i++ {
		var resp cacheprog.Response
		require.NoError(t, dec.Decode(&resp))
		responses = append(responses, resp)
	}

	byID := map[int64]cacheprog.Response{}
	for _, resp := range responses {
		byID[resp.ID] = resp
	}

	remoteGetResp := byID[1]
	require.Equal(t, int64(1), remoteGetResp.ID)
	require.False(t, remoteGetResp.Miss)
	require.Equal(t, "outputIdRemote", remoteGetResp.OutputID)
	require.Equal(t, int64(len(remoteBody)), remoteGetResp.Size)
	require.NotEmpty(t, remoteGetResp.DiskPath)

	// The remote entry must have been materialized locally as an uncompressed file.
	remoteLocalBody, err := os.ReadFile(remoteGetResp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, remoteBody, string(remoteLocalBody))

	putResp := byID[2]
	require.Equal(t, int64(2), putResp.ID)
	require.False(t, putResp.Miss)
	require.Equal(t, "outputIdPut", putResp.OutputID)
	require.Equal(t, int64(len(putBody)), putResp.Size)
	require.NotEmpty(t, putResp.DiskPath)

	// The new put must be immediately available from local direct storage.
	putLocalBody, err := os.ReadFile(putResp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, putBody, putLocalBody)

	putGetResp := byID[3]
	require.Equal(t, int64(3), putGetResp.ID)
	require.False(t, putGetResp.Miss)
	require.Equal(t, "outputIdPut", putGetResp.OutputID)
	require.Equal(t, int64(len(putBody)), putGetResp.Size)
	require.Equal(t, putResp.DiskPath, putGetResp.DiskPath)

	// After the direct proxy flushes upstream puts on close, the remote store
	// must contain the new entry in compressed form while still round-tripping
	// to the original uncompressed payload.
	var remotePut cache.ResponseItem
	require.NoError(t, remoteStore.Get(cache.Request{ActionIDs: []string{"actionIdPut"}}, func(resp cache.ResponseItem) {
		remotePut = resp
	}))
	require.False(t, remotePut.Miss)
	require.True(t, remotePut.IsCompressed)
	require.Equal(t, int64(32), remotePut.WireSize)

	compressedReader, err := remotePut.CompressedBodyReader()
	require.NoError(t, err)
	defer compressedReader.Close()
	compressedBody, err := io.ReadAll(compressedReader)
	require.NoError(t, err)
	require.Len(t, compressedBody, 32)

	uncompressedReader, err := remotePut.UncompressedBodyReader()
	require.NoError(t, err)
	defer uncompressedReader.Close()
	uncompressedBody, err := io.ReadAll(uncompressedReader)
	require.NoError(t, err)
	require.Len(t, uncompressedBody, len(putBody))
	require.Equal(t, putBody, uncompressedBody)
}

func writeJSONLine(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)
	_, err = buf.Write(append(data, '\n'))
	require.NoError(t, err)
}
