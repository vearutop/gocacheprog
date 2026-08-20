package http_test

import (
	"bytes"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

// setPreloadLimitBytesForTest sets a build type's server-side preload budget via the real public
// endpoint (see http.Handler.PreloadLimitBytesSettings), the same way an operator would.
func setPreloadLimitBytesForTest(t *testing.T, baseURL, buildType string, bytes int64) {
	t.Helper()

	url := baseURL + "/settings/preload-limit-bytes?build-type=" + buildType + "&bytes=" + strconv.FormatInt(bytes, 10)
	req, err := nethttp.NewRequest(nethttp.MethodPost, url, nil)
	require.NoError(t, err)

	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, nethttp.StatusNoContent, res.StatusCode)
}

// TestPreload_ServerDefaultDropsLargestItem covers the /preload (GOCACHEPROG) path: unlike
// native GOCACHE mode's -restore-limit-bytes, this path has no client-side total-size control of
// its own, so a server-configured default (see WithSettingsPath) is the only thing that can trim
// it -- dropping the largest item first, until what's left fits.
func TestPreload_ServerDefaultDropsLargestItem(t *testing.T) {
	dir := t.TempDir()
	localStore, err := local.NewStore(dir)
	require.NoError(t, err)

	now := time.Now()
	keep := cache.ResponseItem{ActionID: "keep", OutputID: "keep-output", Size: 2, WireSize: 2, Time: &now}
	keep.SetBodyReader(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewBufferString("ok")), nil })
	drop := cache.ResponseItem{ActionID: "drop", OutputID: "drop-output", Size: 10, WireSize: 10, Time: &now}
	drop.SetBodyReader(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewBufferString("1234567890")), nil })

	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{keep, drop}}))

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	setPreloadLimitBytesForTest(t, srv.URL, "unit", 5)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	var got []string
	require.NoError(t, client.Preload(cache.PreloadRequest{MaxSize: 1000, BuildType: "unit"}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	}))
	require.Equal(t, []string{"keep"}, got, "server default must drop the larger item")
}

// TestPreload_NoServerDefaultReturnsEverything covers the no-op case: with no build-type override
// configured, /preload must not trim anything at all.
func TestPreload_NoServerDefaultReturnsEverything(t *testing.T) {
	dir := t.TempDir()
	localStore, err := local.NewStore(dir)
	require.NoError(t, err)

	now := time.Now()
	a := cache.ResponseItem{ActionID: "a", OutputID: "a-output", Size: 2, WireSize: 2, Time: &now}
	a.SetBodyReader(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewBufferString("ok")), nil })
	b := cache.ResponseItem{ActionID: "b", OutputID: "b-output", Size: 10, WireSize: 10, Time: &now}
	b.SetBodyReader(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewBufferString("1234567890")), nil })

	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{a, b}}))

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	var got []string
	require.NoError(t, client.Preload(cache.PreloadRequest{MaxSize: 1000, BuildType: "unit"}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	}))
	require.ElementsMatch(t, []string{"a", "b"}, got)
}
