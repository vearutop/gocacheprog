package local

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
)

func TestProxyClose_PostsCacheUsed_ReportsDedupedSortedActionIDs(t *testing.T) {
	upstream := &usageRecorderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		Commit:    "commit123",
		ChangesID: "repo/pr-123",
		BuildType: "unit",
	})

	proxy.recordUsedActionID("actionId2")
	proxy.recordUsedActionID("actionId1")
	proxy.recordUsedActionID("actionId2")

	require.NoError(t, proxy.Close())
	require.True(t, upstream.called)
	require.Equal(t, "commit123", upstream.commit)
	require.Equal(t, "repo/pr-123", upstream.changesID)
	require.Equal(t, "unit", upstream.buildType)
	require.True(t, upstream.replaceChanges)
	require.Equal(t, []string{"actionId1", "actionId2"}, upstream.actionIDs)
}

func TestProxyClose_CacheUsedNoOpWithoutUsageRecorder(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, noopStore{}, make(chan cacheprog.Response, 1), ProxyParams{
		Commit:    "commit123",
		ChangesID: "changes123",
		BuildType: "unit",
	})

	proxy.recordUsedActionID("actionId1")

	require.NoError(t, proxy.Close())
}

func TestProxyClose_CacheUsedSkippedWhenDisabled(t *testing.T) {
	upstream := &usageRecorderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		Commit:           "commit123",
		ChangesID:        "repo/pr-123",
		BuildType:        "unit",
		DisableCacheUsed: true,
	})

	proxy.recordUsedActionID("actionId1")

	require.NoError(t, proxy.Close())
	require.False(t, upstream.called)
}

func TestProxyHasLocalEntries(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, noopStore{}, make(chan cacheprog.Response, 1), ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.False(t, proxy.HasLocalEntries())

	proxy.Put(cacheprog.Request{
		ActionID: "actionId1",
		OutputID: "outputId1",
		BodySize: int64(len("body-1")),
	}, []byte("body-1"))

	require.Eventually(t, proxy.HasLocalEntries, time.Second, 10*time.Millisecond)
}

func TestProxyMaybePreload_SkipsWhenDisabled(t *testing.T) {
	upstream := &preloaderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		Preload:     true,
		SkipPreload: true,
	})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.NoError(t, proxy.MaybePreload())
	require.False(t, upstream.called)
}

func TestProxyLookup_SkipsRemoteGetAfterTimeBudget(t *testing.T) {
	upstream := &remoteBudgetStub{getTotalTime: time.Second}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	resps := make(chan cacheprog.Response, 1)
	proxy := NewProxy(store, upstream, resps, ProxyParams{
		MaxRemoteGetTime: 500 * time.Millisecond,
	})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.Lookup(cacheprog.Request{ID: 1, ActionID: "missing"})

	resp := <-resps
	require.True(t, resp.Miss)
	require.False(t, upstream.getCalled)
}

func TestProxyStats_HitBreakdown(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, noopStore{}, make(chan cacheprog.Response, 1), ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.markPreloaded("preloadedAction")
	proxy.recordHitKind("preloadedAction")
	proxy.recordHitKind("regularAction")

	atomic.StoreInt64(&proxy.lookups, 4)
	atomic.StoreInt64(&proxy.hits, 2)
	atomic.StoreInt64(&proxy.misses, 2)

	stats := proxy.Stats()
	require.Equal(t, "2", stats["hits"])
	require.Equal(t, "50.0%", stats["hit_rate"])
	require.Equal(t, "1", stats["preload_hits"])
	require.Equal(t, "25.0%", stats["preload_hit_rate"])
	require.Equal(t, "1", stats["preloaded_items"])
	require.Equal(t, "1", stats["preload_used"])
	require.Equal(t, "0", stats["preload_unused"])
	require.Equal(t, "0.0%", stats["preload_unused_rate"])
	require.Equal(t, "1", stats["regular_hits"])
	require.Equal(t, "25.0%", stats["regular_hit_rate"])
	require.Equal(t, "50.0%", stats["miss_rate"])
}

func TestProxyClose_SkipsRemotePutAboveMaxFileBytes(t *testing.T) {
	upstream := &putRecorderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		MaxFileBytes: 5,
	})

	proxy.Put(cacheprog.Request{
		ActionID: "small",
		OutputID: "small-out",
		BodySize: 5,
	}, []byte("12345"))
	proxy.Put(cacheprog.Request{
		ActionID: "large",
		OutputID: "large-out",
		BodySize: 6,
	}, []byte("123456"))

	require.NoError(t, proxy.Close())
	require.Len(t, upstream.items, 1)
	require.Equal(t, "small", upstream.items[0].ActionID)
	require.Equal(t, "1", proxy.Stats()["skipped_puts"])
}

type usageRecorderStub struct {
	called         bool
	commit         string
	changesID      string
	buildType      string
	replaceChanges bool
	actionIDs      []string
}

func (u *usageRecorderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (u *usageRecorderStub) Put(values cache.Response) error {
	return nil
}

func (u *usageRecorderStub) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error {
	u.called = true
	u.commit = commit
	u.changesID = changesID
	u.buildType = buildType
	u.replaceChanges = replaceChanges
	u.actionIDs = append([]string(nil), actionIDs...)

	return nil
}

type noopStore struct{}

func (noopStore) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (noopStore) Put(values cache.Response) error {
	return nil
}

type putRecorderStub struct {
	items []cache.ResponseItem
}

func (p *putRecorderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (p *putRecorderStub) Put(values cache.Response) error {
	p.items = append(p.items, values.Items...)
	return nil
}

func (p *putRecorderStub) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error {
	return nil
}

type preloaderStub struct {
	called bool
}

func (p *preloaderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (p *preloaderStub) Put(values cache.Response) error {
	return nil
}

func (p *preloaderStub) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	p.called = true
	return nil
}

type remoteBudgetStub struct {
	getCalled    bool
	getTotalTime time.Duration
}

func (r *remoteBudgetStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	r.getCalled = true
	return nil
}

func (r *remoteBudgetStub) Put(values cache.Response) error {
	return nil
}

func (r *remoteBudgetStub) GetTotalTime() time.Duration {
	return r.getTotalTime
}

func (r *remoteBudgetStub) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error {
	return nil
}

func (r *remoteBudgetStub) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (r *remoteBudgetStub) Stats() map[string]string {
	return map[string]string{}
}

func (r *remoteBudgetStub) LastPreloadSources() string {
	return ""
}

func (r *remoteBudgetStub) LastPreloadTimings() (string, string, string) {
	return "", "", ""
}

func (r *remoteBudgetStub) Head(req cache.Request) (cache.Response, error) {
	return cache.Response{}, nil
}

func (r *remoteBudgetStub) Close() error {
	return nil
}

func (r *remoteBudgetStub) ReaderFrom(body io.Reader, cb func(item cache.ResponseItem, body io.Reader) error) (int64, error) {
	return 0, nil
}
