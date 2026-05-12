package local

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
)

func TestApp_IterateInputAndResponses(t *testing.T) {
	resps := make(chan cacheprog.Response, 10)
	proxy, err := NewProxy(t.TempDir(), nil, resps)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

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

func writeJSONLine(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)
	_, err = buf.Write(append(data, '\n'))
	require.NoError(t, err)
}
