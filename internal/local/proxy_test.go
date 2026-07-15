package local

import (
	"errors"
	"fmt"
	"io"
	"sync"
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

func TestProxyMaybePreload_UsesDefaultMaxFileBytesForPreload(t *testing.T) {
	upstream := &preloaderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		Preload: true,
	})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.NoError(t, proxy.MaybePreload())
	require.True(t, upstream.called)
	require.Equal(t, defaultPreloadMaxSize, upstream.req.MaxSize)
}

func TestProxyMaybePreload_UsesMaxFileBytesForPreload(t *testing.T) {
	upstream := &preloaderStub{}

	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, upstream, make(chan cacheprog.Response, 1), ProxyParams{
		Preload:      true,
		MaxFileBytes: 3_000_000,
	})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.NoError(t, proxy.MaybePreload())
	require.True(t, upstream.called)
	require.Equal(t, int64(3_000_000), upstream.req.MaxSize)
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

func TestNewProxy_BatchSemCapacityDefaultsWhenUnset(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, nil, make(chan cacheprog.Response, 1), ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.Equal(t, defaultRemoteBatchConcurrency, cap(proxy.batchSem))
}

func TestNewProxy_BatchSemCapacityHonorsConfiguredValue(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, nil, make(chan cacheprog.Response, 1), ProxyParams{RemoteBatchConcurrency: 3})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	require.Equal(t, 3, cap(proxy.batchSem))
}

// TestProxyResolve_RunsBatchesConcurrently guards against resolveBatch's remote round trip
// running synchronously inside resolve's own goroutine, which would serialize every batch onto
// one remote round trip at a time no matter how many misses are actually outstanding.
func TestProxyResolve_RunsBatchesConcurrently(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	upstream := &concurrencyTrackingStore{delay: 100 * time.Millisecond}
	resps := make(chan cacheprog.Response, 1000)
	proxy := NewProxy(store, upstream, resps, ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	const batches = 4
	const itemsPerBatch = batchBarrierItems

	started := time.Now()
	for b := 0; b < batches; b++ {
		for i := 0; i < itemsPerBatch; i++ {
			proxy.Lookup(cacheprog.Request{ID: int64(b*itemsPerBatch + i + 1), ActionID: fmt.Sprintf("action-%d-%d", b, i)})
		}
	}

	for i := 0; i < batches*itemsPerBatch; i++ {
		<-resps
	}
	elapsed := time.Since(started)

	require.Greater(t, upstream.maxObserved(), 1, "batches should overlap, not serialize one at a time")
	require.Less(t, elapsed, time.Duration(batches)*upstream.delay, "concurrent batches should finish faster than fully serialized ones would")
	require.Equal(t, int64(batches), proxy.StatsSummary().RoundTrips, "each full batch should count as exactly one round trip")
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

func TestProxyStatsSummary_WithoutUpstream(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, nil, make(chan cacheprog.Response, 1), ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	atomic.StoreInt64(&proxy.hits, 3)
	atomic.StoreInt64(&proxy.misses, 1)
	atomic.StoreInt64(&proxy.puts, 2)

	summary := proxy.StatsSummary()
	require.Equal(t, int64(3), summary.Hits)
	require.Equal(t, int64(1), summary.Misses)
	require.Equal(t, int64(2), summary.Puts)
	require.Equal(t, "75.0%", summary.HitRate)
	require.Empty(t, summary.BytesRead)
	require.Equal(t, "hits=3 misses=1 puts=2 hit_rate=75.0%", summary.String())
}

func TestProxyStatsSummary_WithUpstream(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)
	proxy := NewProxy(store, statsUpstream{}, make(chan cacheprog.Response, 1), ProxyParams{})
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	atomic.StoreInt64(&proxy.hits, 1)

	summary := proxy.StatsSummary()
	require.Equal(t, "1.2MB", summary.BytesRead)
	require.Equal(t, "3.4KB", summary.BytesWritten)
	require.Equal(t, "123ms", summary.GetTotalTime)
	require.Contains(t, summary.String(), "bytes_read=1.2MB bytes_written=3.4KB round_trip_time=123ms")
}

func TestStatsSummaryString_RoundTripTimeWithoutBytes(t *testing.T) {
	summary := StatsSummary{Hits: 1, Misses: 0, Puts: 0, HitRate: "100.0%", GetTotalTime: "1.5s"}
	require.Equal(t, "hits=1 misses=0 puts=0 hit_rate=100.0% round_trip_time=1.5s", summary.String())
}

func TestStatsSummaryString_Invocations(t *testing.T) {
	summary := StatsSummary{Hits: 9893, Misses: 802, Puts: 909, HitRate: "92.5%", Invocations: 222}
	require.Equal(t, "hits=9893 misses=802 puts=909 hit_rate=92.5% invocations=222", summary.String())
}

func TestStatsSummaryString_OmitsInvocationsWhenZero(t *testing.T) {
	summary := StatsSummary{Hits: 1, HitRate: "100.0%"}
	require.NotContains(t, summary.String(), "invocations")
}

type statsUpstream struct {
	noopStore
}

func (statsUpstream) Stats() map[string]string {
	return map[string]string{
		"bytes_read":     "1.2MB",
		"bytes_written":  "3.4KB",
		"get_total_time": "123ms",
		"get_cnt":        "5",
		"preload_bytes":  "0B",
	}
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
	req    cache.PreloadRequest
}

func (p *preloaderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (p *preloaderStub) Put(values cache.Response) error {
	return nil
}

func (p *preloaderStub) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	p.called = true
	p.req = req
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

// concurrencyTrackingStore's Get sleeps for delay and records the highest number of Get calls
// observed running at the same time, so a test can assert that batches actually overlap.
type concurrencyTrackingStore struct {
	noopStore
	delay time.Duration

	mu      sync.Mutex
	current int
	max     int
}

func (s *concurrencyTrackingStore) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	s.mu.Lock()
	s.current++
	if s.current > s.max {
		s.max = s.current
	}
	s.mu.Unlock()

	time.Sleep(s.delay)

	for _, actionID := range req.ActionIDs {
		cb(cache.ResponseItem{ActionID: actionID, Miss: true})
	}

	s.mu.Lock()
	s.current--
	s.mu.Unlock()

	return nil
}

func (s *concurrencyTrackingStore) maxObserved() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.max
}

// partialFailureStore's Get calls back for the first respondBeforeFailure action IDs (in
// request order) and then returns an error without calling back for the rest, simulating an
// upstream connection that dies partway through streaming a batched response (or, with
// respondBeforeFailure 0, one that fails before it ever starts responding at all).
type partialFailureStore struct {
	noopStore
	respondBeforeFailure int
}

func (s *partialFailureStore) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	for i, actionID := range req.ActionIDs {
		if i >= s.respondBeforeFailure {
			return errors.New("simulated upstream failure")
		}
		cb(cache.ResponseItem{ActionID: actionID, Miss: true})
	}

	return errors.New("simulated upstream failure")
}

// TestProxyResolveBatch_UpstreamErrorStillRespondsToEveryItem guards against cmd/go hanging
// forever on a lookup whose upstream batch call failed (or timed out) before ever reaching that
// item: every request in the batch must get a response regardless of how far the upstream call
// got before failing.
func TestProxyResolveBatch_UpstreamErrorStillRespondsToEveryItem(t *testing.T) {
	for _, respondBeforeFailure := range []int{0, 1} {
		t.Run(fmt.Sprintf("respondBeforeFailure=%d", respondBeforeFailure), func(t *testing.T) {
			store, err := NewStore(t.TempDir())
			require.NoError(t, err)

			upstream := &partialFailureStore{respondBeforeFailure: respondBeforeFailure}
			resps := make(chan cacheprog.Response, 10)
			proxy := NewProxy(store, upstream, resps, ProxyParams{})
			t.Cleanup(func() {
				require.NoError(t, proxy.Close())
			})

			batch := []cacheprog.Request{
				{ID: 1, ActionID: "action-1"},
				{ID: 2, ActionID: "action-2"},
				{ID: 3, ActionID: "action-3"},
			}
			proxy.resolveBatch(batch)

			got := map[int64]cacheprog.Response{}
			for i := 0; i < len(batch); i++ {
				resp := <-resps
				got[resp.ID] = resp
			}

			require.Len(t, got, len(batch), "every request in the batch must get a response")
			for _, req := range batch {
				resp, ok := got[req.ID]
				require.True(t, ok, "missing response for request ID %d", req.ID)
				require.True(t, resp.Miss)
			}
		})
	}
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
