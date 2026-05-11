package local

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
)

func TestProxyPostCacheUsed_ReportsDedupedSortedActionIDs(t *testing.T) {
	upstream := &usageRecorderStub{}

	proxy, err := NewProxy(t.TempDir(), upstream, make(chan cacheprog.Response, 1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.recordUsedActionID("actionId2")
	proxy.recordUsedActionID("actionId1")
	proxy.recordUsedActionID("actionId2")

	require.NoError(t, proxy.PostCacheUsed("commit123", "repo/pr-123", "unit"))
	require.True(t, upstream.called)
	require.Equal(t, "commit123", upstream.commit)
	require.Equal(t, "repo/pr-123", upstream.changesID)
	require.Equal(t, "unit", upstream.buildType)
	require.Equal(t, []string{"actionId1", "actionId2"}, upstream.actionIDs)
}

func TestProxyPostCacheUsed_NoOpWithoutUsageRecorder(t *testing.T) {
	proxy, err := NewProxy(t.TempDir(), noopStore{}, make(chan cacheprog.Response, 1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.recordUsedActionID("actionId1")

	require.NoError(t, proxy.PostCacheUsed("commit123", "", ""))
	require.NoError(t, proxy.PostCacheUsed("", "changes123", ""))
	require.NoError(t, proxy.PostCacheUsed("", "", ""))
}

func TestProxyStats_HitBreakdown(t *testing.T) {
	proxy, err := NewProxy(t.TempDir(), noopStore{}, make(chan cacheprog.Response, 1))
	require.NoError(t, err)
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
	called    bool
	commit    string
	changesID string
	buildType string
	actionIDs []string
}

func (u *usageRecorderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (u *usageRecorderStub) Put(values cache.Response) error {
	return nil
}

func (u *usageRecorderStub) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string) error {
	u.called = true
	u.commit = commit
	u.changesID = changesID
	u.buildType = buildType
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
