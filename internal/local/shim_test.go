package local

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
	cachehttp "github.com/vearutop/gocacheprog/internal/http"
)

func TestProcessShimSession_SharedProxyRemoteCompression(t *testing.T) {
	remoteStore, err := NewStore(t.TempDir(), WithCompression())
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

	remoteSrv := httptest.NewServer(cachehttp.NewHandler(remoteStore, ""))
	t.Cleanup(remoteSrv.Close)

	upstream, err := cachehttp.NewClient(remoteSrv.URL, "")
	require.NoError(t, err)

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 100)
	proxy, err := NewProxy(store, upstream, resps, ProxyParams{})
	require.NoError(t, err)
	t.Cleanup(func() {
		close(resps)
	})

	shimAddr := startTestShimServer(t, NewShimServer(proxy, resps, "", nil))
	shimClient, err := NewShimClient(shimAddr, "", "shim-a")
	require.NoError(t, err)

	putBody := []byte(strings.Repeat("put-body-", 40))[:343]

	var input bytes.Buffer
	writeJSONLine(t, &input, cacheprog.Request{ID: 1, Command: cacheprog.CmdGet, ActionID: "actionIdRemote"})
	writeJSONLine(t, &input, cacheprog.Request{ID: 2, Command: cacheprog.CmdPut, ActionID: "actionIdPut", OutputID: "outputIdPut", BodySize: int64(len(putBody))})
	writeJSONLine(t, &input, putBody)
	writeJSONLine(t, &input, cacheprog.Request{ID: 3, Command: cacheprog.CmdGet, ActionID: "actionIdPut"})
	writeJSONLine(t, &input, cacheprog.Request{ID: 4, Command: cacheprog.CmdClose})

	var output bytes.Buffer
	var logDump bytes.Buffer

	require.NoError(t, ProcessShimSession(&input, &output, &logDump, shimClient))

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
	require.False(t, remoteGetResp.Miss)
	require.Equal(t, "outputIdRemote", remoteGetResp.OutputID)
	require.Equal(t, int64(len(remoteBody)), remoteGetResp.Size)
	require.NotEmpty(t, remoteGetResp.DiskPath)

	remoteLocalBody, err := os.ReadFile(remoteGetResp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, remoteBody, string(remoteLocalBody))

	putResp := byID[2]
	require.False(t, putResp.Miss)
	require.Equal(t, "outputIdPut", putResp.OutputID)
	require.Equal(t, int64(len(putBody)), putResp.Size)
	require.NotEmpty(t, putResp.DiskPath)

	putLocalBody, err := os.ReadFile(putResp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, putBody, putLocalBody)

	putGetResp := byID[3]
	require.False(t, putGetResp.Miss)
	require.Equal(t, "outputIdPut", putGetResp.OutputID)
	require.Equal(t, int64(len(putBody)), putGetResp.Size)
	require.Equal(t, putResp.DiskPath, putGetResp.DiskPath)

	require.NoError(t, proxy.Close())

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

func TestShimServer_RewritesCollidingRequestIDsAcrossSessions(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	for _, seeded := range []struct {
		actionID string
		outputID string
		body     []byte
	}{
		{actionID: "actionA", outputID: "outputA", body: []byte("body-A")},
		{actionID: "actionB", outputID: "outputB", body: []byte("body-B")},
	} {
		item := cache.ResponseItem{
			ActionID: seeded.actionID,
			OutputID: seeded.outputID,
			Size:     int64(len(seeded.body)),
			Time:     &now,
		}
		body := seeded.body
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		})
		require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{item}}))
	}

	resps := make(chan cacheprog.Response, 100)
	proxy, err := NewProxy(store, nil, resps, ProxyParams{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	shimAddr := startTestShimServer(t, NewShimServer(proxy, resps, "", nil))
	clientA, err := NewShimClient(shimAddr, "", "shim-a")
	require.NoError(t, err)
	clientB, err := NewShimClient(shimAddr, "", "shim-b")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	type result struct {
		resp cacheprog.Response
		err  error
	}
	results := make(chan result, 2)

	go func() {
		defer wg.Done()
		resp, err := clientA.Do(cacheprog.Request{ID: 1, Command: cacheprog.CmdGet, ActionID: "actionA"}, nil)
		results <- result{resp: resp, err: err}
	}()

	go func() {
		defer wg.Done()
		resp, err := clientB.Do(cacheprog.Request{ID: 1, Command: cacheprog.CmdGet, ActionID: "actionB"}, nil)
		results <- result{resp: resp, err: err}
	}()

	wg.Wait()
	close(results)

	var seen []cacheprog.Response
	for r := range results {
		require.NoError(t, r.err)
		seen = append(seen, r.resp)
	}

	require.Len(t, seen, 2)
	require.Equal(t, int64(1), seen[0].ID)
	require.Equal(t, int64(1), seen[1].ID)
	require.ElementsMatch(t, []string{"outputA", "outputB"}, []string{seen[0].OutputID, seen[1].OutputID})
}

func TestNewShimClient_UnixSocket(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 100)
	proxy, err := NewProxy(store, nil, resps, ProxyParams{})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	client, err := NewShimClient(startTestShimServer(t, NewShimServer(proxy, resps, "", nil)), "", "shim-a")
	require.NoError(t, err)

	resp, err := client.Do(cacheprog.Request{
		ID:       1,
		Command:  cacheprog.CmdPut,
		ActionID: "actionIdPut",
		OutputID: "outputIdPut",
		BodySize: int64(len("body-put")),
	}, []byte("body-put"))
	require.NoError(t, err)
	require.False(t, resp.Miss)
	require.Equal(t, "outputIdPut", resp.OutputID)
	require.NotEmpty(t, resp.DiskPath)
}

func startTestShimServer(t *testing.T, server *ShimServer) string {
	t.Helper()

	socketPath := filepath.Join("/tmp", fmt.Sprintf("gocacheprog-shim-%d.sock", time.Now().UnixNano()))
	ln, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go server.serveConn(conn)
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.Remove(socketPath)
	})

	return "unix://" + socketPath
}
