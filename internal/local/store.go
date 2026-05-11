package local

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/gocacheprogd/internal/cache"
)

type Store struct {
	dir      string
	compress bool

	mu    sync.Mutex
	index map[string]indexEntry

	prevStats     string
	hits          int64
	misses        int64
	puts          int64
	putsExist     int64
	putsCompleted int64
	errors        int64
}

// indexEntry is the metadata that Store stores on disk for an ActionID.
type indexEntry struct {
	OutputID   string `json:"o"`
	Size       int64  `json:"n"`
	TimeMicro  int64  `json:"t,omitempty"`
	Compressed int64  `json:"c,omitempty"`
	WireSize   int64  `json:"w,omitempty"`
}

func NewStore(dir string, withCompression bool) (*Store, error) {
	dir, err := toAbsPath(dir)
	if err != nil {
		return nil, err
	}

	dc := &Store{
		dir:      dir,
		compress: withCompression,
		index:    make(map[string]indexEntry),
	}

	indexPath := filepath.Join(dir, "index.json")
	d, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return dc, nil
		}
		return nil, fmt.Errorf("read %s: %w", indexPath, err)
	}

	err = json.Unmarshal(d, &dc.index)
	if err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", indexPath, err)
	}

	return dc, nil
}

func toAbsPath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("empty path")
	}

	// If it's already absolute, return it (cleaned)
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}

	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Join and clean the path
	abs := filepath.Join(cwd, path)
	return filepath.Clean(abs), nil
}

func (dc *Store) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	for _, resp := range dc.get(req).Items {
		cb(resp)
	}

	return nil
}

func (dc *Store) get(req cache.Request) cache.Response {
	resp := cache.Response{Items: make([]cache.ResponseItem, 0, len(req.ActionIDs))}

	for _, actionID := range req.ActionIDs {
		resp.Items = append(resp.Items, dc.getOne(actionID))
	}

	return resp
}

func (dc *Store) getOne(actionID string) cache.ResponseItem {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	ie, ok := dc.index[actionID]
	if !ok {
		atomic.AddInt64(&dc.misses, 1)

		return cache.ResponseItem{ActionID: actionID, Miss: true}
	}

	atomic.AddInt64(&dc.hits, 1)

	return dc.responseItem(actionID, ie)
}

func (dc *Store) responseItem(actionID string, ie indexEntry) cache.ResponseItem {
	res := cache.ResponseItem{ActionID: actionID}

	res.OutputID = ie.OutputID
	res.Size = ie.Size
	res.DiskPath = dc.OutputFilename(ie.OutputID)
	res.IsCompressed = ie.Compressed == 1
	res.WireSize = ie.WireSize

	t := time.UnixMicro(ie.TimeMicro)
	res.Time = &t

	return res
}

func (dc *Store) Put(values cache.Response) error {
	for _, item := range values.Items {
		if err := dc.putOne(item); err != nil {
			return err
		}
	}

	return nil
}

func (dc *Store) putOne(item cache.ResponseItem) error {
	outputFile := dc.OutputFilename(item.OutputID)
	now := time.Now().UTC()

	if item.OutputID == "" {
		atomic.AddInt64(&dc.errors, 1)
		println("empty output id:", fmt.Sprintf("%+v", item))
		return fmt.Errorf("empty output id: %+v", item)
	}

	existing := dc.getOne(item.ActionID)
	if existing.DiskPath != "" && !existing.Miss {
		atomic.AddInt64(&dc.putsExist, 1)
	}

	atomic.AddInt64(&dc.puts, 1)

	ie := indexEntry{
		OutputID:  item.OutputID,
		Size:      item.Size,
		TimeMicro: now.UnixMicro(),
	}

	var (
		rd  io.Reader
		err error
	)

	if item.IsCompressed {
		if !dc.compress {
			// Decompress a compressed body.
			rd, err = item.UncompressedBodyReader()
		} else {
			// Pass compressed body as is.
			rd, err = item.WireBodyReader()
			ie.Compressed = 1
			ie.WireSize = item.WireSize
		}
	} else {
		if dc.compress && item.Size >= cache.MinCompressionSize {
			// Enable compression if it is not there.
			rd, err = item.CompressedBodyReader()
			ie.Compressed = 1
			ie.WireSize = item.WireSize
		} else {
			// Pass uncompressed body as is.
			rd, err = item.UncompressedBodyReader()
		}
	}

	if err != nil {
		atomic.AddInt64(&dc.errors, 1)
		println("get reader for put:", err.Error())
		return fmt.Errorf("get reader for put: %w", err)
	}

	if err := writeAtomic(outputFile, rd); err != nil {
		atomic.AddInt64(&dc.errors, 1)
		println("atomic write:", err.Error())
		return fmt.Errorf("atomic write: %w", err)
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.index[item.ActionID] = ie

	atomic.AddInt64(&dc.putsCompleted, 1)

	return nil
}

func writeAtomic(outputFile string, rd io.Reader) (err error) {
	f, err := os.Create(outputFile + ".tmp")
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if rd != nil {
		_, err = io.Copy(f, rd)
		if err != nil {
			f.Close()
			return fmt.Errorf("copy to file: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return os.Rename(outputFile+".tmp", outputFile)
}

func (dc *Store) Close() error {
	d, err := json.Marshal(dc.index)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dc.dir, "index.json"), d, 0o600)
}

func (dc *Store) OutputFilename(outputID string) string {
	return filepath.Join(dc.dir, strings.ReplaceAll(outputID, "/", "_"))
}

func (dc *Store) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	var res []cache.ResponseItem

	if req.MaxSize == 0 {
		req.MaxSize = 600_000
	}

	dc.mu.Lock()
	for k, v := range dc.index {
		if v.WireSize > req.MaxSize {
			continue
		}

		res = append(res, dc.responseItem(k, v))
	}
	dc.mu.Unlock()

	for _, item := range res {
		cb(item)
	}

	return nil
}

func (dc *Store) Stats() map[string]string {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return map[string]string{
		"hits":          fmt.Sprintf("%d", dc.hits),
		"misses":        fmt.Sprintf("%d", dc.misses),
		"puts":          fmt.Sprintf("%d", dc.puts),
		"putsExist":     fmt.Sprintf("%d", dc.putsExist),
		"putsCompleted": fmt.Sprintf("%d", dc.putsCompleted),
		"index":         fmt.Sprintf("%d", len(dc.index)),
		"errors":        fmt.Sprintf("%d", dc.errors),
	}
}

func (dc *Store) PrintStats() {
	st := dc.Stats()

	stats := ""
	for _, k := range slices.Sorted(maps.Keys(st)) {
		v := st[k]

		stats += fmt.Sprintf("%s: %s ", k, v)
	}

	if stats != dc.prevStats {
		log.Printf(stats)
		dc.prevStats = stats
	}
}
