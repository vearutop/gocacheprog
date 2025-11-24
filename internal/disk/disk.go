package disk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// indexEntry is the metadata that Cache stores on disk for an ActionID.
type indexEntry struct {
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeMicro int64  `json:"t"`
}

type Cache struct {
	Dir     string
	Verbose bool
	Logf    func(format string, args ...any) // optional alt logger

	mu    sync.Mutex
	index map[string]indexEntry

	wg     sync.WaitGroup
	lookup chan cacheprog.Request
	resps  chan cacheprog.Response

	batches int
	lookups int
	hits    int
	misses  int
	puts    int
}

func NewCache(dir string, resps chan cacheprog.Response) (*Cache, error) {
	c := &Cache{
		Dir:    dir,
		resps:  resps,
		lookup: make(chan cacheprog.Request, 1000),
		index:  make(map[string]indexEntry),
	}

	d, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err == nil {
		err = json.Unmarshal(d, &c.index)
		if err != nil {
			return nil, err
		}
	}

	c.wg.Add(1)
	go c.resolve()

	return c, nil
}

func (dc *Cache) Close() error {
	close(dc.lookup)
	dc.wg.Wait()

	d, err := json.Marshal(dc.index)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dc.Dir, "index.json"), d, 0600)
}

func (dc *Cache) logf(format string, args ...any) {
	if dc.Logf != nil {
		dc.Logf(format, args...)
	} else if dc.Verbose {
		log.Printf(format, args...)
	}
}

func (dc *Cache) Lookup(req cacheprog.Request) {
	dc.lookups++
	dc.lookup <- req
}

const (
	batchBarrierTick = 50 * time.Millisecond
	batchBarrierSize = 100
)

func (dc *Cache) resolve() {
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

			batch = append(batch, req)
			if len(batch) >= batchBarrierSize {
				dc.resolveBatch(batch)
				batch = batch[:0]
			}
		case <-t.C:
			if len(batch) > 0 {
				dc.resolveBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (dc *Cache) resolveBatch(batch []cacheprog.Request) {
	dc.batches++

	for _, req := range batch {
		res := dc.Get(req)

		if res.Miss {
			dc.misses++
		} else {
			dc.hits++
		}

		dc.resps <- res
	}
}

func (dc *Cache) Get(req cacheprog.Request) cacheprog.Response {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	resp := cacheprog.Response{ID: req.ID}

	ie, ok := dc.index[req.ActionID]
	if !ok {
		resp.Miss = true
		return resp
	}

	resp.OutputID = ie.OutputID
	resp.Size = ie.Size
	resp.DiskPath = dc.OutputFilename(ie.OutputID)
	t := time.UnixMicro(ie.TimeMicro)
	resp.Time = &t

	return resp
}

func (dc *Cache) OutputFilename(outputID string) string {
	return filepath.Join(dc.Dir, strings.Replace(outputID, "/", "_", -1))
}

func (dc *Cache) PrintStats() {
	println("batches:", dc.batches, "lookups:", dc.lookups, "hits:", dc.hits, "misses:", dc.misses, "puts:", dc.puts)
}

func (dc *Cache) Put(actionID, outputID string, size int64, body []byte) (diskPath string, _ error) {
	dc.puts++

	if len(actionID) < 4 || len(outputID) < 4 {
		return "", fmt.Errorf("actionID and outputID must be at least 4 characters long")
	}

	outputFile := dc.OutputFilename(outputID)

	err := os.WriteFile(outputFile, body, 0600)
	if err != nil {
		return "", err
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.index[actionID] = indexEntry{
		OutputID:  outputID,
		Size:      size,
		TimeMicro: time.Now().UnixMicro(),
	}

	return outputFile, nil
}
