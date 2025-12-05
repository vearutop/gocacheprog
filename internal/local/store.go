package local

import (
	"encoding/json"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Store struct {
	dir string

	mu    sync.Mutex
	index map[string]indexEntry
}

// indexEntry is the metadata that Store stores on disk for an ActionID.
type indexEntry struct {
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeMicro int64  `json:"t"`
}

func NewStore(dir string) (*Store, error) {
	dir, err := toAbsPath(dir)
	if err != nil {
		return nil, err
	}

	dc := &Store{
		dir:   dir,
		index: make(map[string]indexEntry),
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

	res := cache.ResponseItem{ActionID: actionID}

	ie, ok := dc.index[actionID]
	if !ok {
		res.Miss = true

		return res
	}

	res.OutputID = ie.OutputID
	res.Size = ie.Size
	res.DiskPath = dc.OutputFilename(ie.OutputID)

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

	rd, err := item.UncompressedBodyReader()
	if err != nil {
		return fmt.Errorf("get reader for put: %w", err)
	}

	if err := writeAtomic(outputFile, rd); err != nil {
		return fmt.Errorf("atomic write: %w", err)
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.index[item.ActionID] = indexEntry{
		OutputID:  item.OutputID,
		Size:      item.Size,
		TimeMicro: now.UnixMicro(),
	}

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

	return os.WriteFile(filepath.Join(dc.dir, "index.json"), d, 0600)
}

func (dc *Store) OutputFilename(outputID string) string {
	return filepath.Join(dc.dir, strings.ReplaceAll(outputID, "/", "_"))
}
