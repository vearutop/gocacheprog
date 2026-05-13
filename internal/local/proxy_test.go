package local

import (
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
