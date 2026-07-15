package local

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
)

type Proxy struct {
	Verbose bool
	Logf    func(format string, args ...any) // optional alt logger

	disk     *Store
	upstream cache.Store

	wg     sync.WaitGroup
	lookup chan cacheprog.Request
	resps  chan cacheprog.Response
	put    chan cacheprog.Request

	// batchWG/batchSem let resolveBatch's remote round trips run concurrently (bounded by
	// batchConcurrency in flight) instead of serializing one at a time inside resolve's own
	// goroutine; see dispatchBatch.
	batchWG  sync.WaitGroup
	batchSem chan struct{}

	batches     int64
	lookups     int64
	hits        int64
	preloadHits int64
	regularHits int64
	misses      int64
	puts        int64
	putsExist   int64
	batchPuts   int64
	skippedPuts int64

	usedMu               sync.Mutex
	usedActionIDs        map[string]struct{}
	preloadedMu          sync.Mutex
	preloadedActionIDs   map[string]struct{}
	usedPreloadedIDs     map[string]struct{}
	preloadBytesMu       sync.Mutex
	lastPreloadSize      int64
	remoteGetLimitLogged atomic.Bool

	initialLocalEntries bool
	params              ProxyParams
}

const defaultPreloadMaxSize int64 = 1_000_000

type ProxyParams struct {
	Commit                 string
	ChangesID              string
	BuildType              string
	BaseCommit             string
	ParentCommit           string
	Preload                bool
	SkipPreload            bool
	MaxRemoteGetTime       time.Duration
	MaxFileBytes           int64
	DisableCacheUsed       bool
	RemoteBatchConcurrency int
}

func (p ProxyParams) SessionCommit() string {
	return p.Commit
}

func (p ProxyParams) SessionParentCommit() string {
	return p.ParentCommit
}

func (p ProxyParams) SessionChangesID() string {
	return p.ChangesID
}

func (p ProxyParams) SessionBuildType() string {
	return p.BuildType
}

func (p ProxyParams) SessionBaseCommit() string {
	return p.BaseCommit
}

// defaultRemoteBatchConcurrency bounds how many resolveBatch remote round trips can run at
// once when ProxyParams.RemoteBatchConcurrency isn't set.
const defaultRemoteBatchConcurrency = 8

func NewProxy(disk *Store, upstream cache.Store, resps chan cacheprog.Response, params ProxyParams) *Proxy {
	batchConcurrency := params.RemoteBatchConcurrency
	if batchConcurrency <= 0 {
		batchConcurrency = defaultRemoteBatchConcurrency
	}

	c := &Proxy{
		resps:              resps,
		lookup:             make(chan cacheprog.Request, 1000),
		put:                make(chan cacheprog.Request, 1000),
		upstream:           upstream,
		usedActionIDs:      map[string]struct{}{},
		preloadedActionIDs: map[string]struct{}{},
		usedPreloadedIDs:   map[string]struct{}{},
		params:             params,
		batchSem:           make(chan struct{}, batchConcurrency),
	}

	c.disk = disk
	c.initialLocalEntries = disk.HasEntries()

	c.wg.Add(2)

	go c.resolve()
	go c.consumePut()

	return c
}

func (dc *Proxy) Close() error {
	close(dc.lookup)
	close(dc.put)
	dc.wg.Wait()

	actionIDs := dc.UsedActionIDs()
	commit := dc.params.Commit
	changesID := dc.params.ChangesID
	buildType := dc.params.BuildType
	baseCommit := dc.params.BaseCommit
	parentCommit := dc.params.ParentCommit
	replaceChanges := !dc.initialLocalEntries

	if dc.params.DisableCacheUsed {
		dc.logf(
			"cache-used skipped: disabled, commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q action_ids=%d",
			commit,
			changesID,
			buildType,
			baseCommit,
			parentCommit,
			len(actionIDs),
		)
		return dc.disk.Close()
	}

	if commit == "" && changesID == "" {
		dc.logf(
			"cache-used skipped: no commit or changes-id, build_type=%q base_commit=%q parent_commit=%q action_ids=%d",
			buildType,
			baseCommit,
			parentCommit,
			len(actionIDs),
		)
		return dc.disk.Close()
	}

	if _, ok := dc.upstream.(cache.UsageRecorder); !ok {
		dc.logf(
			"cache-used skipped: upstream does not support usage recording, commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q action_ids=%d",
			commit,
			changesID,
			buildType,
			baseCommit,
			parentCommit,
			len(actionIDs),
		)
		return dc.disk.Close()
	}

	dc.logf(
		"cache-used uploading: commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q action_ids=%d replace_changes=%t",
		commit,
		changesID,
		buildType,
		baseCommit,
		parentCommit,
		len(actionIDs),
		replaceChanges,
	)

	startedAt := time.Now()
	if err := dc.postCacheUsed(commit, changesID, buildType, replaceChanges, actionIDs); err != nil {
		dc.logf(
			"cache-used upload failed: commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q action_ids=%d replace_changes=%t err=%s",
			commit,
			changesID,
			buildType,
			baseCommit,
			parentCommit,
			len(actionIDs),
			replaceChanges,
			err.Error(),
		)
		return err
	}

	dc.logf(
		"cache-used uploaded: action_ids=%d duration=%s",
		len(actionIDs),
		time.Since(startedAt).String(),
	)

	return dc.disk.Close()
}

func (dc *Proxy) logf(format string, args ...any) {
	if dc.Logf != nil {
		dc.Logf(format, args...)
	} else if dc.Verbose {
		log.Printf(format, args...)
	}
}

func (dc *Proxy) Lookup(req cacheprog.Request) {
	dc.recordUsedActionID(req.ActionID)

	if dc.upstream == nil {
		dc.resps <- dc.Get(req)
		return
	}

	atomic.AddInt64(&dc.lookups, 1)

	if dc.shouldSkipRemoteGet() {
		resp := dc.Get(req)
		if resp.Miss {
			atomic.AddInt64(&dc.misses, 1)
		} else {
			atomic.AddInt64(&dc.hits, 1)
			dc.recordHitKind(req.ActionID)
		}

		dc.resps <- resp
		return
	}

	dc.lookup <- req
}

func (dc *Proxy) shouldSkipRemoteGet() bool {
	if dc.params.MaxRemoteGetTime <= 0 {
		return false
	}

	timed, ok := dc.upstream.(interface{ GetTotalTime() time.Duration })
	if !ok {
		return false
	}

	total := timed.GetTotalTime()
	if total < dc.params.MaxRemoteGetTime {
		return false
	}

	if dc.remoteGetLimitLogged.CompareAndSwap(false, true) {
		dc.logf(
			"remote get budget exhausted: get_total_time=%s max_remote_get_time=%s; local misses will stop querying remote",
			total.String(),
			dc.params.MaxRemoteGetTime.String(),
		)
	}

	return true
}

func (dc *Proxy) recordUsedActionID(actionID string) {
	if actionID == "" {
		return
	}

	dc.usedMu.Lock()
	dc.usedActionIDs[actionID] = struct{}{}
	dc.usedMu.Unlock()
}

func (dc *Proxy) UsedActionIDs() []string {
	dc.usedMu.Lock()
	defer dc.usedMu.Unlock()

	res := make([]string, 0, len(dc.usedActionIDs))
	for actionID := range dc.usedActionIDs {
		res = append(res, actionID)
	}

	slices.Sort(res)

	return res
}

func (dc *Proxy) postCacheUsed(commit string, changesID string, buildType string, replaceChanges bool, actionIDs []string) error {
	if dc.params.DisableCacheUsed {
		return nil
	}

	if commit == "" && changesID == "" {
		return nil
	}

	recorder, ok := dc.upstream.(cache.UsageRecorder)
	if !ok {
		return nil
	}

	return recorder.PostCacheUsed(commit, changesID, buildType, actionIDs, replaceChanges)
}

func (dc *Proxy) MaybePreload() error {
	maxSize := dc.params.MaxFileBytes
	if maxSize == 0 {
		maxSize = defaultPreloadMaxSize
	}

	req := cache.PreloadRequest{
		MaxSize:      maxSize,
		Commit:       dc.params.Commit,
		ChangesID:    dc.params.ChangesID,
		BuildType:    dc.params.BuildType,
		BaseCommit:   dc.params.BaseCommit,
		ParentCommit: dc.params.ParentCommit,
	}

	if dc.params.SkipPreload {
		log.Printf(
			"preload skipped: -skip-preload is set, commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q max_size=%d",
			req.Commit,
			req.ChangesID,
			req.BuildType,
			req.BaseCommit,
			req.ParentCommit,
			req.MaxSize,
		)
		return nil
	}

	if !dc.params.Preload &&
		dc.params.Commit == "" &&
		dc.params.ChangesID == "" &&
		dc.params.BuildType == "" &&
		dc.params.BaseCommit == "" &&
		dc.params.ParentCommit == "" {
		log.Printf("preload skipped: no preload flag and no scope hints")
		return nil
	}

	if dc.HasLocalEntries() {
		log.Printf(
			"preload skipped: local cache dir is already populated, commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q max_size=%d",
			req.Commit,
			req.ChangesID,
			req.BuildType,
			req.BaseCommit,
			req.ParentCommit,
			req.MaxSize,
		)
		return nil
	}

	st := time.Now()
	log.Printf(
		"preload starting: commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q max_size=%d",
		req.Commit,
		req.ChangesID,
		req.BuildType,
		req.BaseCommit,
		req.ParentCommit,
		req.MaxSize,
	)
	if err := dc.Preload(req); err != nil {
		return fmt.Errorf("preload cache: %w", err)
	}

	sources := "unavailable"
	if s, ok := dc.upstream.(interface{ LastPreloadSources() string }); ok {
		if lastSources := s.LastPreloadSources(); lastSources != "" {
			sources = lastSources
		} else {
			sources = "none"
		}
	}

	preloadBytes := "unknown"
	if s, ok := dc.upstream.(interface{ Stats() map[string]string }); ok {
		if stats := s.Stats(); stats != nil {
			if v := stats["preload_bytes"]; v != "" {
				preloadBytes = v
			}
		}
	}

	queueWait, prepareTime, totalTime := "unknown", "unknown", "unknown"
	if s, ok := dc.upstream.(interface {
		LastPreloadTimings() (string, string, string)
	}); ok {
		queueWait, prepareTime, totalTime = s.LastPreloadTimings()
		if queueWait == "" {
			queueWait = "unknown"
		}
		if prepareTime == "" {
			prepareTime = "unknown"
		}
		if totalTime == "" {
			totalTime = "unknown"
		}
	}

	uncompressedBytes := humanBytes(dc.lastPreloadSizeBytes())

	log.Printf(
		"preload done: sources=%s items=%d bytes=%s uncompressed_bytes=%s duration=%s queue_wait=%s prepare_time=%s total_time=%s",
		sources,
		dc.preloadedCount(),
		preloadBytes,
		uncompressedBytes,
		time.Since(st).String(),
		queueWait,
		prepareTime,
		totalTime,
	)

	return nil
}

func (dc *Proxy) HasLocalEntries() bool {
	return dc.disk.HasEntries()
}

func (dc *Proxy) preloadedCount() int {
	dc.preloadedMu.Lock()
	defer dc.preloadedMu.Unlock()

	return len(dc.preloadedActionIDs)
}

func (dc *Proxy) lastPreloadSizeBytes() int64 {
	dc.preloadBytesMu.Lock()
	defer dc.preloadBytesMu.Unlock()

	return dc.lastPreloadSize
}

func humanBytes(bytes int64) string {
	if bytes < 1000 {
		return fmt.Sprintf("%dB", bytes)
	}

	units := []string{"B", "KB", "MB", "GB", "TB"}
	v := float64(bytes)
	unit := 0
	for v >= 1000 && unit < len(units)-1 {
		v /= 1000
		unit++
	}

	return fmt.Sprintf("%.1f%s", v, units[unit])
}

const (
	batchBarrierTick  = 20 * time.Millisecond
	batchBarrierItems = 100  // Number of items to flush the queue.
	batchBarrierSize  = 10e7 // Total size of items to flush the queue.
)

func (dc *Proxy) resolve() {
	defer func() {
		// Waits for any batches dispatchBatch spawned to finish sending their responses
		// before this goroutine (and thus dc.wg) is done, so Close doesn't return - and
		// dc.resps doesn't get closed - while one is still in flight.
		dc.batchWG.Wait()
		dc.wg.Done()
	}()

	batch := make([]cacheprog.Request, 0, batchBarrierItems)
	t := time.NewTicker(batchBarrierTick)

	for {
		select {
		case req := <-dc.lookup:
			if req.ID == 0 {
				t.Stop()

				return
			}

			resp := dc.Get(req)
			if resp.Miss {
				batch = append(batch, req)
				if len(batch) >= batchBarrierItems {
					dc.dispatchBatch(batch)
					batch = make([]cacheprog.Request, 0, batchBarrierItems)
				}
			} else {
				atomic.AddInt64(&dc.hits, 1)
				dc.recordHitKind(req.ActionID)
				dc.resps <- resp
			}

		case <-t.C:
			if len(batch) > 0 {
				dc.dispatchBatch(batch)
				batch = make([]cacheprog.Request, 0, batchBarrierItems)
			}
		}
	}
}

// dispatchBatch resolves batch against the upstream on its own goroutine, bounded by
// batchSem's capacity concurrent batches at a time. Without this, resolveBatch's remote HTTP
// round trip would run synchronously inside resolve's own goroutine, so only one batch's worth
// of misses could ever be in flight at once - serializing every remote round trip in the job
// onto a single one-at-a-time queue regardless of how many are genuinely outstanding.
func (dc *Proxy) dispatchBatch(batch []cacheprog.Request) {
	dc.batchSem <- struct{}{}
	dc.batchWG.Add(1)

	go func() {
		defer func() {
			<-dc.batchSem
			dc.batchWG.Done()
		}()

		dc.resolveBatch(batch)
	}()
}

func (dc *Proxy) consumePut() {
	defer dc.wg.Done()

	puts := make([]cache.ResponseItem, 0, batchBarrierItems)
	sumSize := 0

	for req := range dc.put {
		resp := dc.Get(req)

		item := cache.ResponseItem{}
		item.ActionID = req.ActionID
		item.OutputID = resp.OutputID
		item.Size = resp.Size
		item.WireSize = resp.Size
		item.Time = resp.Time
		item.IsCompressed = false
		item.DiskPath = resp.DiskPath

		if req.Body != nil {
			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(req.Body)), nil
			})
		}

		if dc.params.MaxFileBytes > 0 && item.Size > dc.params.MaxFileBytes {
			atomic.AddInt64(&dc.skippedPuts, 1)
			dc.logf("skip upstream put for %s: size=%d exceeds max-file-bytes=%d", item.ActionID, item.Size, dc.params.MaxFileBytes)
			continue
		}

		puts = append(puts, item)
		sumSize += int(item.Size)

		if len(puts) >= batchBarrierItems || sumSize >= batchBarrierSize {
			sumSize = 0
			if dc.upstream != nil {
				atomic.AddInt64(&dc.batchPuts, 1)

				if err := dc.upstream.Put(cache.Response{Items: puts}); err != nil {
					dc.logf("upstream put failed: %s", err.Error())
				}
			}
			puts = puts[:0]
		}
	}

	if len(puts) > 0 && dc.upstream != nil {
		if err := dc.upstream.Put(cache.Response{Items: puts}); err != nil {
			dc.logf("upstream final put failed: %s", err.Error())
		}
	}
}

func (dc *Proxy) Preload(req cache.PreloadRequest) error {
	if dc.upstream == nil {
		return errors.New("upstream is not set")
	}

	p, ok := dc.upstream.(cache.Preloader)
	if !ok {
		return errors.New("upstream is not preloader")
	}

	items := 0
	var uncompressedBytes int64

	err := p.Preload(req, func(resp cache.ResponseItem) {
		items++
		uncompressedBytes += resp.Size

		br, err := resp.WireBodyReader()
		if err != nil {
			log.Printf("prepare uncompressed body %v: %s", resp, err.Error())
			return
		}

		var b []byte
		if br != nil {
			defer func() {
				if err := br.Close(); err != nil {
					dc.logf("close preload body reader: %s", err.Error())
				}
			}()

			b, err = io.ReadAll(br)
			if err != nil {
				log.Printf("read uncompressed body %v: %s", resp, err.Error())
				return
			}
		}

		if err := dc.putRespItem(resp, b); err != nil {
			log.Printf("put resp item: %s", err.Error())
			return
		}

		dc.markPreloaded(resp.ActionID)
	})

	dc.preloadBytesMu.Lock()
	dc.lastPreloadSize = uncompressedBytes
	dc.preloadBytesMu.Unlock()

	return err
}

//nolint:nestif // batching remote lookups is inherently branchy and clearer inline.
func (dc *Proxy) resolveBatch(batch []cacheprog.Request) {
	atomic.AddInt64(&dc.batches, 1)

	if dc.upstream != nil {
		// Grouped by ActionID, not deduplicated to one request each: multiple concurrent
		// sessions (or the same session) can legitimately look up the same ActionID close
		// enough in time to land in the same batch, and every one of those original
		// requests - each with its own ID - needs its own response. A plain map keyed by
		// ActionID would silently keep only the last request for a repeated ActionID,
		// permanently losing the response for every earlier one sharing it.
		reqsByAction := make(map[string][]cacheprog.Request, len(batch))
		actionIDs := make([]string, 0, len(batch))
		answered := make(map[string]bool, len(batch))

		for _, req := range batch {
			if _, ok := reqsByAction[req.ActionID]; !ok {
				actionIDs = append(actionIDs, req.ActionID)
			}
			reqsByAction[req.ActionID] = append(reqsByAction[req.ActionID], req)
		}

		r := cache.Request{ActionIDs: actionIDs}

		respondAll := func(actionID string, rs cacheprog.Response) {
			for _, req := range reqsByAction[actionID] {
				out := rs
				out.ID = req.ID
				dc.resps <- out
			}
		}

		err := dc.upstream.Get(r, func(resp cache.ResponseItem) {
			answered[resp.ActionID] = true
			count := int64(len(reqsByAction[resp.ActionID]))

			if resp.Miss {
				atomic.AddInt64(&dc.misses, count)
				respondAll(resp.ActionID, cacheprog.Response{Miss: true})
				return
			}

			br, err := resp.UncompressedBodyReader()
			if err != nil {
				respondAll(resp.ActionID, cacheprog.Response{Err: err.Error()})
				return
			}

			var b []byte
			if br != nil {
				defer func() {
					if err := br.Close(); err != nil {
						dc.logf("close upstream body reader: %s", err.Error())
					}
				}()

				b, err = io.ReadAll(br)
				if err != nil {
					dc.logf("read item uncompressed body %v: %s", resp, err.Error())

					respondAll(resp.ActionID, cacheprog.Response{Err: err.Error()})
					return
				}
			}

			atomic.AddInt64(&dc.hits, count)
			atomic.AddInt64(&dc.regularHits, count)

			// One local write per ActionID regardless of how many original requests
			// share it; respondAll then answers every one of them with that same result.
			req := reqsByAction[resp.ActionID][0]
			req.Command = cacheprog.CmdPut
			req.OutputID = resp.OutputID
			req.BodySize = resp.Size

			respondAll(resp.ActionID, dc.putOne(req, b))
		})
		if err != nil {
			dc.logf("upstream get failed: %s", err.Error())
		}

		// If the upstream call errored before (or partway through) streaming results, some
		// or all of this batch's ActionIDs never reached the callback above and so never
		// got a response. Without this, cmd/go would hang forever waiting on those specific
		// request IDs instead of just seeing a (recoverable) cache miss.
		for actionID, reqs := range reqsByAction {
			if !answered[actionID] {
				atomic.AddInt64(&dc.misses, int64(len(reqs)))
				respondAll(actionID, cacheprog.Response{Miss: true})
			}
		}

		return
	}

	for _, req := range batch {
		atomic.AddInt64(&dc.misses, 1)

		dc.resps <- cacheprog.Response{
			ID:   req.ID,
			Miss: true,
		}
	}
}

func (dc *Proxy) Get(req cacheprog.Request) cacheprog.Response {
	rs := dc.disk.getOne(req.ActionID)

	resp := cacheprog.Response{ID: req.ID}
	resp.OutputID = rs.OutputID
	resp.Size = rs.Size
	resp.Time = rs.Time
	resp.DiskPath = rs.DiskPath
	resp.Miss = rs.Miss

	if rs.Size > 0 && rs.DiskPath == "" {
		log.Printf("disk path is empty for %s", req.ActionID)
	}

	return resp
}

func (dc *Proxy) PrintStats() {
	st := dc.Stats()

	var sb strings.Builder
	for _, k := range slices.Sorted(maps.Keys(st)) {
		v := st[k]
		fmt.Fprintf(&sb, " %s: %s", k, v)
	}

	if dc.upstream != nil {
		if s, ok := dc.upstream.(interface{ Stats() map[string]string }); ok {
			sb.WriteString("\nupstream:")

			st := s.Stats()
			for _, k := range slices.Sorted(maps.Keys(st)) {
				if k == "preloaded" {
					continue
				}

				fmt.Fprintf(&sb, " %s: %s", k, st[k])
			}
		}
	}

	sb.WriteString("\ndisk:")
	st = dc.disk.Stats()
	for _, k := range slices.Sorted(maps.Keys(st)) {
		fmt.Fprintf(&sb, " %s: %s", k, st[k])
	}

	dc.logf("%s", sb.String())
}

// StatsSummary is a final, job-level cache report: hits/misses/puts plus, when a remote
// upstream is configured, bytes transferred and time spent on remote round trips.
type StatsSummary struct {
	Hits    int64  `json:"hits"`
	Misses  int64  `json:"misses"`
	Puts    int64  `json:"puts"`
	HitRate string `json:"hit_rate"`

	// Invocations is the number of distinct GOCACHEPROG invocations these stats cover, when
	// known: the shim daemon's session count, or (in direct mode) the number of run-stats
	// records aggregated. Omitted when there's exactly one invocation, i.e. nothing to count.
	Invocations int64 `json:"invocations,omitempty"`

	// RoundTrips is the number of batched remote Get round trips actually made, as distinct
	// from Misses: many misses are typically resolved by a single round trip once they're
	// batched through resolveBatch's barrier (see batchBarrierTick/batchBarrierItems).
	RoundTrips int64 `json:"round_trips,omitempty"`

	BytesRead      string `json:"bytes_read,omitempty"`
	BytesWritten   string `json:"bytes_written,omitempty"`
	GetTotalTime   string `json:"get_total_time,omitempty"`
	GetCount       string `json:"get_count,omitempty"`
	PreloadedBytes string `json:"preloaded_bytes,omitempty"`

	// TotalTime is the wall-clock time elapsed since -github-actions-init started, when known.
	TotalTime string `json:"total_time,omitempty"`

	// ForcedCloses counts shim clients (shim mode only) that hit shimCloseWaitTimeout and closed
	// without waiting for every pending response. Normally zero; a nonzero value means the
	// safety net fired and is worth investigating even though it prevented a hang.
	ForcedCloses int64 `json:"forced_closes,omitempty"`
}

func (s StatsSummary) String() string {
	str := fmt.Sprintf("hits=%d misses=%d puts=%d hit_rate=%s", s.Hits, s.Misses, s.Puts, s.HitRate)
	if s.Invocations > 0 {
		str += fmt.Sprintf(" invocations=%d", s.Invocations)
	}
	if s.BytesRead != "" || s.BytesWritten != "" {
		str += fmt.Sprintf(" bytes_read=%s bytes_written=%s", s.BytesRead, s.BytesWritten)
	}
	if s.RoundTrips > 0 {
		str += fmt.Sprintf(" round_trips=%d", s.RoundTrips)
	}
	if s.ForcedCloses > 0 {
		str += fmt.Sprintf(" forced_closes=%d", s.ForcedCloses)
	}
	if s.GetTotalTime != "" {
		str += " round_trip_time=" + s.GetTotalTime
	}
	if s.TotalTime != "" {
		str += " total_time=" + s.TotalTime
	}

	return str
}

// StatsSummary returns the current cumulative StatsSummary for this Proxy.
func (dc *Proxy) StatsSummary() StatsSummary {
	hits := atomic.LoadInt64(&dc.hits)
	misses := atomic.LoadInt64(&dc.misses)

	summary := StatsSummary{
		Hits:       hits,
		Misses:     misses,
		Puts:       atomic.LoadInt64(&dc.puts),
		HitRate:    percent(hits, hits+misses),
		RoundTrips: atomic.LoadInt64(&dc.batches),
	}

	if dc.upstream != nil {
		if s, ok := dc.upstream.(interface{ Stats() map[string]string }); ok {
			st := s.Stats()
			summary.BytesRead = st["bytes_read"]
			summary.BytesWritten = st["bytes_written"]
			summary.GetTotalTime = st["get_total_time"]
			summary.GetCount = st["get_cnt"]
			summary.PreloadedBytes = st["preload_bytes"]
		}
	}

	return summary
}

func (dc *Proxy) putRespItem(item cache.ResponseItem, body []byte) error {
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	})

	if err := dc.disk.Put(cache.Response{
		Items: []cache.ResponseItem{item},
	}); err != nil {
		return fmt.Errorf("write resp %+v: %w", item, err)
	}

	return nil
}

func (dc *Proxy) putOne(req cacheprog.Request, body []byte) cacheprog.Response {
	atomic.AddInt64(&dc.puts, 1)

	item := cache.ResponseItem{
		ActionID: req.ActionID,
		OutputID: req.OutputID,
		Size:     req.BodySize,
	}

	if err := dc.putRespItem(item, body); err != nil {
		return cacheprog.Response{
			ID:  req.ID,
			Err: fmt.Sprintf("write file %+v: %s", req, err),
		}
	}

	return dc.Get(req)
}

func (dc *Proxy) Put(req cacheprog.Request, body []byte) cacheprog.Response {
	if resp := dc.Get(req); !resp.Miss {
		atomic.AddInt64(&dc.putsExist, 1)

		return resp
	}

	resp := dc.putOne(req, body)

	if len(body) < 1e5 {
		req.Body = body
	}

	if dc.upstream != nil {
		dc.put <- req
	}

	return resp
}

func (dc *Proxy) Stats() map[string]string {
	lookups := atomic.LoadInt64(&dc.lookups)
	hits := atomic.LoadInt64(&dc.hits)
	preloadHits := atomic.LoadInt64(&dc.preloadHits)
	regularHits := atomic.LoadInt64(&dc.regularHits)
	misses := atomic.LoadInt64(&dc.misses)
	preloadedItems, preloadUsed := dc.preloadUsageStats()
	preloadUnused := preloadedItems - preloadUsed

	stats := map[string]string{
		// "batchGets": strconv.FormatInt(atomic.LoadInt64(&dc.batches), 10),
		// "batchPuts": strconv.FormatInt(atomic.LoadInt64(&dc.batchPuts), 10),
		"lookups":             strconv.FormatInt(lookups, 10),
		"hits":                strconv.FormatInt(hits, 10),
		"hit_rate":            percent(hits, lookups),
		"preload_hits":        strconv.FormatInt(preloadHits, 10),
		"preload_hit_rate":    percent(preloadHits, lookups),
		"preloaded_items":     strconv.Itoa(preloadedItems),
		"preload_used":        strconv.Itoa(preloadUsed),
		"preload_unused":      strconv.Itoa(preloadUnused),
		"preload_unused_rate": percentInt(preloadUnused, preloadedItems),
		"regular_hits":        strconv.FormatInt(regularHits, 10),
		"regular_hit_rate":    percent(regularHits, lookups),
		"misses":              strconv.FormatInt(misses, 10),
		"miss_rate":           percent(misses, lookups),
		"puts":                strconv.FormatInt(atomic.LoadInt64(&dc.puts), 10),
		"putsExist":           strconv.FormatInt(atomic.LoadInt64(&dc.putsExist), 10),
		"skipped_puts":        strconv.FormatInt(atomic.LoadInt64(&dc.skippedPuts), 10),
	}

	return stats
}

func (dc *Proxy) markPreloaded(actionID string) {
	if actionID == "" {
		return
	}

	dc.preloadedMu.Lock()
	dc.preloadedActionIDs[actionID] = struct{}{}
	dc.preloadedMu.Unlock()
}

func (dc *Proxy) isPreloaded(actionID string) bool {
	dc.preloadedMu.Lock()
	_, ok := dc.preloadedActionIDs[actionID]
	dc.preloadedMu.Unlock()

	return ok
}

func (dc *Proxy) recordHitKind(actionID string) {
	if dc.isPreloaded(actionID) {
		dc.markUsedPreloaded(actionID)
		atomic.AddInt64(&dc.preloadHits, 1)
		return
	}

	atomic.AddInt64(&dc.regularHits, 1)
}

func percent(num, denom int64) string {
	if denom == 0 {
		return "0.0%"
	}

	return fmt.Sprintf("%.1f%%", 100*float64(num)/float64(denom))
}

func percentInt(num, denom int) string {
	if denom == 0 {
		return "0.0%"
	}

	return fmt.Sprintf("%.1f%%", 100*float64(num)/float64(denom))
}

func (dc *Proxy) markUsedPreloaded(actionID string) {
	if actionID == "" {
		return
	}

	dc.preloadedMu.Lock()
	if _, ok := dc.preloadedActionIDs[actionID]; ok {
		dc.usedPreloadedIDs[actionID] = struct{}{}
	}
	dc.preloadedMu.Unlock()
}

func (dc *Proxy) preloadUsageStats() (preloadedItems int, preloadUsed int) {
	dc.preloadedMu.Lock()
	defer dc.preloadedMu.Unlock()

	return len(dc.preloadedActionIDs), len(dc.usedPreloadedIDs)
}
