package local

import (
	"bytes"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"io"
	"log"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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

	batches   int64
	lookups   int64
	hits      int64
	misses    int64
	puts      int64
	putsExist int64
	batchPuts int64
}

func NewProxy(dir string, upstream cache.Store, resps chan cacheprog.Response) (*Proxy, error) {
	c := &Proxy{
		resps:    resps,
		lookup:   make(chan cacheprog.Request, 1000),
		put:      make(chan cacheprog.Request, 1000),
		upstream: upstream,
	}

	disk, err := NewStore(dir, false)
	if err != nil {
		return nil, err
	}

	c.disk = disk

	c.wg.Add(2)

	go c.resolve()
	go c.consumePut()

	return c, nil
}

func (dc *Proxy) Close() error {
	close(dc.lookup)
	close(dc.put)
	dc.wg.Wait()

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
	if dc.upstream == nil {
		dc.resps <- dc.Get(req)
		return
	}

	atomic.AddInt64(&dc.lookups, 1)
	dc.lookup <- req
}

const (
	batchBarrierTick  = 50 * time.Millisecond
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
		return fmt.Errorf("upstream is not set")
	}

	p, ok := dc.upstream.(cache.Preloader)
	if !ok {
		return fmt.Errorf("upstream is not preloader")
	}

	items := 0

	err := p.Preload(req, func(resp cache.ResponseItem) {
		items++

		br, err := resp.UncompressedBodyReader()
		if err != nil {
			log.Printf("prepare uncompressed body %v: %s", resp, err.Error())
			return
		}

		var b []byte
		if br != nil {
			defer br.Close()

			b, err = io.ReadAll(br)
			if err != nil {
				log.Printf("read uncompressed body %v: %s", resp, err.Error())
				return
			}
		}

		if err := dc.putRespItem(resp, b); err != nil {
			log.Printf("put resp item: %s", err.Error())
		}
	})

	println("preloaded", items, "items")
	return err
}

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
				defer br.Close()

				b, err = io.ReadAll(br)
				if err != nil {
					dc.logf("read item uncompressed body %v: %s", resp, err.Error())

					rs.Err = err.Error()
					return
				}
			}

			atomic.AddInt64(&dc.hits, 1)

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

	var res string
	for _, k := range slices.Sorted(maps.Keys(st)) {
		v := st[k]
		res += fmt.Sprintf(" %s: %s", k, v)
	}

	if dc.upstream != nil {
		if s, ok := dc.upstream.(interface{ Stats() map[string]string }); ok {
			res += "\nupstream:"

			st := s.Stats()
			for _, k := range slices.Sorted(maps.Keys(st)) {
				if k == "preloaded" {
					continue
				}

				res += fmt.Sprintf(" %s: %s", k, st[k])
			}
		}
	}

	res += "\ndisk:"
	st = dc.disk.Stats()
	for _, k := range slices.Sorted(maps.Keys(st)) {
		res += fmt.Sprintf(" %s: %s", k, st[k])
	}

	log.Println(res)
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
	stats := map[string]string{
		//"batchGets": strconv.FormatInt(atomic.LoadInt64(&dc.batches), 10),
		//"batchPuts": strconv.FormatInt(atomic.LoadInt64(&dc.batchPuts), 10),
		"lookups":   strconv.FormatInt(atomic.LoadInt64(&dc.lookups), 10),
		"hits":      strconv.FormatInt(atomic.LoadInt64(&dc.hits), 10),
		"misses":    strconv.FormatInt(atomic.LoadInt64(&dc.misses), 10),
		"puts":      strconv.FormatInt(atomic.LoadInt64(&dc.puts), 10),
		"putsExist": strconv.FormatInt(atomic.LoadInt64(&dc.putsExist), 10),
	}

	return stats
}
