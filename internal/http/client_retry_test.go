package http

import (
	nethttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
