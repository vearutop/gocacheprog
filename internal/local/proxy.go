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

	batches     int64
	lookups     int64
	hits        int64
	preloadHits int64
	regularHits int64
	misses      int64
	puts        int64
	putsExist   int64
	batchPuts   int64

	usedMu             sync.Mutex
	usedActionIDs      map[string]struct{}
	preloadedMu        sync.Mutex
	preloadedActionIDs map[string]struct{}
	usedPreloadedIDs   map[string]struct{}

	initialLocalEntries bool
	params              ProxyParams
}

type ProxyParams struct {
	Commit           string
	ChangesID        string
	BuildType        string
	BaseCommit       string
	ParentCommit     string
	Preload          bool
	SkipPreload      bool
	PreloadSize      int64
	DisableCacheUsed bool
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

func NewProxy(disk *Store, upstream cache.Store, resps chan cacheprog.Response, params ProxyParams) *Proxy {
	c := &Proxy{
		resps:              resps,
		lookup:             make(chan cacheprog.Request, 1000),
		put:                make(chan cacheprog.Request, 1000),
		upstream:           upstream,
		usedActionIDs:      map[string]struct{}{},
		preloadedActionIDs: map[string]struct{}{},
		usedPreloadedIDs:   map[string]struct{}{},
		params:             params,
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
	dc.lookup <- req
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
	req := cache.PreloadRequest{
		MaxSize:      dc.params.PreloadSize,
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

	log.Printf(
		"preload done: sources=%s items=%d bytes=%s duration=%s",
		sources,
		dc.preloadedCount(),
		preloadBytes,
		time.Since(st).String(),
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

const (
	batchBarrierTick  = 20 * time.Millisecond
	batchBarrierItems = 100  // Number of items to flush the queue.
	batchBarrierSize  = 10e7 // Total size of items to flush the queue.
)

func (dc *Proxy) resolve() {
	defer func() {
		dc.wg.Done()
	}()

	batch := make([]cacheprog.Request, 0, 100)
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
					dc.resolveBatch(batch)
					batch = batch[:0]
				}
			} else {
				atomic.AddInt64(&dc.hits, 1)
				dc.recordHitKind(req.ActionID)
				dc.resps <- resp
			}

		case <-t.C:
			if len(batch) > 0 {
				dc.resolveBatch(batch)
				batch = batch[:0]
			}
		}
	}
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

	err := p.Preload(req, func(resp cache.ResponseItem) {
		items++

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

	return err
}

//nolint:nestif // batching remote lookups is inherently branchy and clearer inline.
func (dc *Proxy) resolveBatch(batch []cacheprog.Request) {
	atomic.AddInt64(&dc.batches, 1)

	if dc.upstream != nil {
		m := make(map[string]cacheprog.Response, len(batch))
		r := cache.Request{ActionIDs: make([]string, 0, len(batch))}
		reqs := map[string]cacheprog.Request{}

		for _, req := range batch {
			m[req.ActionID] = cacheprog.Response{ID: req.ID, Miss: true}
			r.ActionIDs = append(r.ActionIDs, req.ActionID)
			reqs[req.ActionID] = req
		}

		err := dc.upstream.Get(r, func(resp cache.ResponseItem) {
			rs := m[resp.ActionID]
			defer func() {
				dc.resps <- rs
			}()

			if resp.Miss {
				rs.Miss = true

				atomic.AddInt64(&dc.misses, 1)
				return
			}

			br, err := resp.UncompressedBodyReader()
			if err != nil {
				rs.Err = err.Error()
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

					rs.Err = err.Error()
					return
				}
			}

			atomic.AddInt64(&dc.hits, 1)
			atomic.AddInt64(&dc.regularHits, 1)

			req := reqs[resp.ActionID]
			req.Command = cacheprog.CmdPut
			req.OutputID = resp.OutputID
			req.BodySize = resp.Size

			rs = dc.putOne(req, b)
		})
		if err != nil {
			dc.logf("upstream get failed: %s", err.Error())
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
