package http_test

import (
	"bytes"
	"io"
	"math/rand"
	nethttp "net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/gocache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func TestIndex_BasicAuth(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandler(localStore, "secret-token"))
	t.Cleanup(srv.Close)

	get := func(user, pass string) (int, string) {
		req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/", nil)
		require.NoError(t, err)
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}
		res, err := nethttp.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { require.NoError(t, res.Body.Close()) }()
		b, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res.StatusCode, string(b)
	}

	code, _ := get("", "")
	require.Equal(t, nethttp.StatusUnauthorized, code)

	code, _ = get("anyone", "wrong-token")
	require.Equal(t, nethttp.StatusUnauthorized, code)

	code, body := get("anyone", "secret-token")
	require.Equal(t, nethttp.StatusOK, code)
	require.Contains(t, body, "server version")
	require.Contains(t, body, "Objects store")
}

func TestIndex_CleanupTriggersEvictionImmediately(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir(), local.WithMaxDiskBytes(1))
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))

	require.Contains(t, localStore.Stats()["index"], "1")

	req, err := nethttp.NewRequest(nethttp.MethodPost, srv.URL+"/", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	require.Equal(t, nethttp.StatusOK, res.StatusCode) // redirect followed by default client

	require.Equal(t, "0", localStore.Stats()["index"])
}

func TestIndex_ShowsInProgressClientSession(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{
		SessionID: "session-abc",
		PID:       123,
		CacheDir:  "/tmp/build",
		Params: local.ProxyParams{
			ChangesID: "repo/pr-123",
			BuildType: "unit",
		},
	})
	require.NoError(t, err)

	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))

	req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	body := string(b)

	require.True(t, strings.Contains(body, "in progress"))
	require.True(t, strings.Contains(body, "repo/pr-123"))
	require.True(t, strings.Contains(body, "unit"))
	// The raw session ID, pid, and cache dir are tracking keys, not displayed columns.
	require.False(t, strings.Contains(body, "session-abc"))
}

func TestIndex_SessionMarkedDone(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{SessionID: "session-xyz"})
	require.NoError(t, err)

	require.NoError(t, client.MarkSessionDone())

	req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(b), `class="done"`)
}

// TestCombinedBudget_EvictsAcrossStores exercises the actual reported bug: a shared budget must
// cover both stores' combined usage, not let one silently grow unbounded while the other enforces
// its own separate cap. Neither store is given its own maxDiskBytes; only the Handler-level
// combined budget applies.
func TestCombinedBudget_EvictsAcrossStores(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir)
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"))
	require.NoError(t, err)

	// Seed the native store directly (bypassing HTTP/the budget enforcer entirely) so its size
	// is known exactly before the budget is set, instead of guessing at wire-encoding overhead.
	payload := make([]byte, 1000)
	_, err = rand.New(rand.NewSource(1)).Read(payload)
	require.NoError(t, err)

	bigItem := gocache.FileItem{Path: "ab/big", Size: int64(len(payload)), WireSize: int64(len(payload))}
	bigItem.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	})
	require.NoError(t, nativeStore.SaveItem(bigItem))

	nativeSize := nativeStore.DiskBytes()
	require.Greater(t, nativeSize, int64(0))

	// Budget fits the native store alone, but not once the small local item below is added too.
	budget := nativeSize + 3
	h := http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", "", 2, http.WithMaxDiskBytes(budget))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	// This request only touches the local store, yet must still trigger the combined-budget
	// check and evict from the native store, since that's the one currently over budget.
	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))

	require.LessOrEqual(t, localStore.DiskBytes()+nativeStore.DiskBytes(), budget)
	require.Zero(t, nativeStore.DiskBytes(), "the oversized native entry should have been evicted, not the small local one")
}
