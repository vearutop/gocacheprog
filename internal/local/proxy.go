package local

import (
	"bytes"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// indexEntry is the metadata that Proxy stores on disk for an ActionID.
type indexEntry struct {
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeMicro int64  `json:"t"`
}

type Proxy struct {
	Verbose bool
	Logf    func(format string, args ...any) // optional alt logger

	disk     *Store
	upstream cache.Store

	wg     sync.WaitGroup
	lookup chan cacheprog.Request
	resps  chan cacheprog.Response
	put    chan cacheprog.Request

	batches int64
	lookups int64
	hits    int64
	misses  int64
	puts    int64
}

func NewProxy(dir string, resps chan cacheprog.Response) (*Proxy, error) {
	c := &Proxy{
		resps:  resps,
		lookup: make(chan cacheprog.Request, 1000),
		put:    make(chan cacheprog.Request, 1000),
	}

	disk, err := NewStore(dir)
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
	atomic.AddInt64(&dc.lookups, 1)
	dc.lookup <- req
}

const (
	batchBarrierTick = 50 * time.Millisecond
	batchBarrierSize = 100
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
				if len(batch) >= batchBarrierSize {
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

	puts := make([]cache.ResponseItem, 0, batchBarrierSize)

	for req := range dc.put {
		resp := dc.Get(req)

		item := cache.ResponseItem{}
		item.ActionID = req.ActionID
		item.OutputID = resp.OutputID
		item.Size = resp.Size
		item.WireSize = resp.Size
		item.Time = resp.Time
		item.IsCompressed = false
		item.SetBodyReader(func() io.ReadCloser {
			f, err := os.Open(resp.DiskPath)
			if err != nil {
				return nil
			}

			return f
		})

		puts = append(puts, item)

		if len(puts) >= batchBarrierSize {
			if dc.upstream != nil {
				if err := dc.upstream.Put(cache.Response{Items: puts}); err != nil {
					dc.logf("upstream put failed: %s", err.Error())
				}
			}
			puts = puts[:0]
		}
	}

	if len(puts) > 0 && dc.upstream != nil {
		if err := dc.upstream.Put(cache.Response{Items: puts}); err != nil {
			dc.logf("upstream put failed: %s", err.Error())
		}
	}
}

func (dc *Proxy) resolveBatch(batch []cacheprog.Request) {
	atomic.AddInt64(&dc.batches, 1)

	if dc.upstream != nil {
		m := make(map[string]cacheprog.Response, len(batch))
		r := cache.Request{ActionIDs: make([]string, 0, len(batch))}

		for _, req := range batch {
			m[req.ActionID] = cacheprog.Response{ID: req.ID, Miss: true}
			r.ActionIDs = append(r.ActionIDs, req.ActionID)
		}

		resp, err := dc.upstream.Get(r)
		if err != nil {
			dc.logf("upstream get failed: %s", err.Error())
		}

		for _, res := range resp.Items {
			r := m[res.ActionID]
			r.Miss = res.Miss
			r.OutputID = res.OutputID
			r.Size = res.Size
			r.Time = res.Time
			r.DiskPath = res.DiskPath

			dc.resps <- r
		}
	} else {
		for _, req := range batch {
			atomic.AddInt64(&dc.misses, 1)

			dc.resps <- cacheprog.Response{
				ID:   req.ID,
				Miss: true,
			}
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

	return resp
}

func (dc *Proxy) PrintStats() {
	println("batches:", atomic.LoadInt64(&dc.batches), ""+
		"lookups:", atomic.LoadInt64(&dc.lookups),
		"hits:", atomic.LoadInt64(&dc.hits),
		"misses:", atomic.LoadInt64(&dc.misses),
		"puts:", atomic.LoadInt64(&dc.puts))
}

func (dc *Proxy) Put(req cacheprog.Request, body []byte) cacheprog.Response {
	atomic.AddInt64(&dc.puts, 1)

	item := cache.ResponseItem{
		ActionID: req.ActionID,
		OutputID: req.OutputID,
		Size:     req.BodySize,
	}

	item.SetBodyReader(func() io.ReadCloser {
		return io.NopCloser(bytes.NewReader(body))
	})

	resp := cacheprog.Response{
		ID: req.ID,
	}

	if err := dc.disk.Put(cache.Response{
		Items: []cache.ResponseItem{item},
	}); err != nil {
		resp.Err = fmt.Sprintf("write file: %s", err.Error())
	}

	dc.put <- req

	return dc.Get(req)
}
