package http

import (
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/gocache"
)

func TestNewClient_RetriesGatewayTimeout(t *testing.T) {
	prevDelay := gatewayRetryDelay
	gatewayRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() {
		gatewayRetryDelay = prevDelay
	})

	attempts := 0
	srv := httptest.NewServer(nethttp.HandlerFunc(func(rw nethttp.ResponseWriter, r *nethttp.Request) {
		attempts++
		if attempts == 1 {
			nethttp.Error(rw, "upstream timeout", nethttp.StatusGatewayTimeout)
			return
		}
		_, err := rw.Write([]byte("gocacheprog test"))
		require.NoError(t, err)
	}))
	t.Cleanup(srv.Close)

	client, err := NewClient(srv.URL, "")
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, 2, attempts)
}

func TestSaveCache_RetriesFailedFinalizeWithNewUpload(t *testing.T) {
	prevDelay := saveCacheRetryDelay
	saveCacheRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() {
		saveCacheRetryDelay = prevDelay
	})

	finalizeAttempts := 0
	uploads := map[string]int64{}
	srv := httptest.NewServer(nethttp.HandlerFunc(func(rw nethttp.ResponseWriter, r *nethttp.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/save-cache-start":
			uploads[r.URL.Query().Get("upload-id")] = 0
			rw.WriteHeader(nethttp.StatusNoContent)
		case "/save-cache-chunk":
			uploadID := r.URL.Query().Get("upload-id")
			n, err := io.Copy(io.Discard, r.Body)
			require.NoError(t, err)
			uploads[uploadID] += n
			rw.WriteHeader(nethttp.StatusNoContent)
		case "/save-cache-finalize":
			finalizeAttempts++
			if finalizeAttempts == 1 {
				nethttp.Error(rw, "temporary finalize failure", nethttp.StatusInternalServerError)
				return
			}
			rw.WriteHeader(nethttp.StatusNoContent)
		case "/save-cache-abort":
			rw.WriteHeader(nethttp.StatusNoContent)
		default:
			nethttp.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	client, err := NewClient(srv.URL, "")
	require.NoError(t, err)
	client.SetSaveCacheChunkBytes(64)

	body := strings.Repeat("a", 128)
	item := gocache.FileItem{
		Path:     "ab/a",
		Size:     int64(len(body)),
		WireSize: int64(len(body)),
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	})

	stats, err := client.SaveCache(gocache.Request{Commit: "commit123"}, gocache.Batch{Items: []gocache.FileItem{item}})
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files)
	require.Equal(t, 2, finalizeAttempts)
	require.Len(t, uploads, 2)
	for _, bytesUploaded := range uploads {
		require.Positive(t, bytesUploaded)
	}
}
