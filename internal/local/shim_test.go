package local

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
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
	proxy := NewProxy(store, upstream, resps, ProxyParams{})
	t.Cleanup(func() {
		close(resps)
	})

	shimAddr := startTestShimServer(t, NewShimServer(proxy, resps, "", nil, nil))
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
	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	require.NoError(t, dec.Decode(&handshake))
	require.Equal(t, []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}, handshake.KnownCommands)

	var responses []cacheprog.Response
	for i := 0; i < 3; i++ {
		var resp cacheprog.Response
		//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
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
	defer func() {
		require.NoError(t, compressedReader.Close())
	}()
	compressedBody, err := io.ReadAll(compressedReader)
	require.NoError(t, err)
	require.Len(t, compressedBody, 32)

	uncompressedReader, err := remotePut.UncompressedBodyReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, uncompressedReader.Close())
	}()
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
	proxy := NewProxy(store, nil, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	shimAddr := startTestShimServer(t, NewShimServer(proxy, resps, "", nil, nil))
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
	proxy := NewProxy(store, nil, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	client, err := NewShimClient(startTestShimServer(t, NewShimServer(proxy, resps, "", nil, nil)), "", "shim-a")
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

func TestShimServer_SessionsSeenCountsDistinctClientSessions(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 100)
	proxy := NewProxy(store, nil, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	server := NewShimServer(proxy, resps, "", nil, nil)
	socket := startTestShimServer(t, server)
	require.Equal(t, int64(0), server.SessionsSeen())

	for i := 0; i < 3; i++ {
		client, err := NewShimClient(socket, "", fmt.Sprintf("shim-%d", i))
		require.NoError(t, err)
		require.NoError(t, client.Send(cacheprog.Request{ID: 1, Command: cacheprog.CmdClose}, nil))
		require.NoError(t, client.Close())
	}

	require.Eventually(t, func() bool {
		return server.SessionsSeen() == 3
	}, time.Second, 10*time.Millisecond)
}

func TestProcessShimSession_HangsOnCloseAfterDisconnectWithPendingRequest(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		closePipeConn(t, serverConn)
		closePipeConn(t, clientConn)
	})

	go func() {
		dec := json.NewDecoder(bufio.NewReader(serverConn))

		var hello shimHello
		require.NoError(t, dec.Decode(&hello))

		var env shimEnvelope
		//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
		require.NoError(t, dec.Decode(&env))
		require.Equal(t, cacheprog.CmdGet, env.Request.Command)

		closePipeConn(t, serverConn)
	}()

	client := newTestShimClient(clientConn)
	require.NoError(t, client.enc.Encode(shimHello{SessionID: "shim-a"}))

	var input bytes.Buffer
	writeJSONLine(t, &input, cacheprog.Request{ID: 1, Command: cacheprog.CmdGet, ActionID: "actionId1"})
	writeJSONLine(t, &input, cacheprog.Request{ID: 2, Command: cacheprog.CmdClose})

	done := make(chan error, 1)
	go func() {
		done <- ProcessShimSession(&input, &bytes.Buffer{}, nil, client)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "expected shim session to surface disconnect during close")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ProcessShimSession remained blocked on close after daemon disconnect")
	}
}

func TestShimServer_ClosedReadyProcessesRequestsEvenWhenPreloadFailed(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 10)
	proxy := NewProxy(store, nil, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	ready := make(chan struct{})
	server := NewShimServer(proxy, resps, "", ready, func() error { return errors.New("preload failed") })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		closePipeConn(t, serverConn)
		closePipeConn(t, clientConn)
	})

	server.startConn(serverConn)

	enc := json.NewEncoder(clientConn)
	dec := json.NewDecoder(bufio.NewReader(clientConn))

	require.NoError(t, enc.Encode(shimHello{SessionID: "shim-a"}))

	writeDone := make(chan error, 1)
	go func() {
		//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
		err := enc.Encode(shimEnvelope{
			Request: cacheprog.Request{
				ID:       1,
				Command:  cacheprog.CmdPut,
				ActionID: "actionId1",
				OutputID: "outputId1",
				BodySize: int64(len("body-1")),
			},
			Body: []byte("body-1"),
		})
		writeDone <- err
	}()

	close(ready) // This mirrors the current main.go behavior even when preload failed.

	if err := <-writeDone; err != nil {
		return
	}

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))

	var resp cacheprog.Response
	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	err = dec.Decode(&resp)
	require.Error(t, err, "request unexpectedly succeeded after a failed preload signal")
}

func TestStopShimServer_RequestsDaemonShutdown(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	resps := make(chan cacheprog.Response, 10)
	proxy := NewProxy(store, nil, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		close(resps)
	})

	server := NewShimServer(proxy, resps, "secret", nil, nil)
	socketPath := filepath.Join("/tmp", fmt.Sprintf("gocacheprog-stop-%d.sock", time.Now().UnixNano()))
	done := make(chan error, 1)

	go func() {
		err := server.Serve("unix://"+socketPath, nil)
		if errors.Is(err, ErrStopRequested) {
			server.ReplyStop(ShimStopResponse{Lines: []string{"stopped"}})
			done <- nil
			return
		}
		done <- err
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(socketPath)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	resp, err := StopShimServer("unix://"+socketPath, "secret")
	require.NoError(t, err)
	require.Equal(t, []string{"stopped"}, resp.Lines)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shim server to stop")
	}
}

func TestShimClient_DoCanStealResponsesFromOtherConsumers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		closePipeConn(t, serverConn)
		closePipeConn(t, clientConn)
	})

	go func() {
		dec := json.NewDecoder(bufio.NewReader(serverConn))
		enc := json.NewEncoder(serverConn)

		var hello shimHello
		require.NoError(t, dec.Decode(&hello))

		var req1 shimEnvelope
		//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
		require.NoError(t, dec.Decode(&req1))
		require.Equal(t, int64(1), req1.Request.ID)

		var req2 shimEnvelope
		//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
		require.NoError(t, dec.Decode(&req2))
		require.Equal(t, int64(2), req2.Request.ID)

		//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
		require.NoError(t, enc.Encode(cacheprog.Response{ID: 2, OutputID: "outputId2"}))
		time.Sleep(20 * time.Millisecond)
		//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
		require.NoError(t, enc.Encode(cacheprog.Response{ID: 1, OutputID: "outputId1"}))
		closePipeConn(t, serverConn)
	}()

	client := newTestShimClient(clientConn)
	require.NoError(t, client.enc.Encode(shimHello{SessionID: "shim-a"}))

	done1 := make(chan cacheprog.Response, 1)
	err1 := make(chan error, 1)
	go func() {
		resp, err := client.Do(cacheprog.Request{ID: 1, Command: cacheprog.CmdGet, ActionID: "actionId1"}, nil)
		if err != nil {
			err1 <- err
			return
		}
		done1 <- resp
	}()

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, client.Send(cacheprog.Request{ID: 2, Command: cacheprog.CmdGet, ActionID: "actionId2"}, nil))

	resp1 := <-done1
	require.Equal(t, int64(1), resp1.ID)

	select {
	case resp, ok := <-client.Responses():
		if ok {
			require.Equal(t, int64(2), resp.ID, "expected response for request 2 to remain available")
		} else {
			t.Fatal("response for request 2 was lost when Do consumed the shared response channel")
		}
	case err := <-err1:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for request 2 response")
	}
}

func newTestShimClient(conn net.Conn) *ShimClient {
	client := &ShimClient{
		conn:    conn,
		enc:     json.NewEncoder(conn),
		jd:      json.NewDecoder(bufio.NewReader(conn)),
		resps:   make(chan cacheprog.Response, 100),
		waiters: make(map[int64]chan cacheprog.Response),
	}

	go client.readResponses()

	return client
}

func closePipeConn(t *testing.T, conn net.Conn) {
	t.Helper()

	err := conn.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		require.NoError(t, err)
	}
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

			server.startConn(conn)
		}
	}()

	t.Cleanup(func() {
		err := ln.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
		err = os.Remove(socketPath)
		if err != nil && !os.IsNotExist(err) {
			require.NoError(t, err)
		}
	})

	return "unix://" + socketPath
}
