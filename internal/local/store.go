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
	Dir string

	mu    sync.Mutex
	index map[string]indexEntry
}

func NewStore(dir string) (*Store, error) {
	dc := &Store{
		Dir:   dir,
		index: make(map[string]indexEntry),
	}

	d, err := os.ReadFile(filepath.Join(dir, "index.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	err = json.Unmarshal(d, &dc.index)
	if err != nil {
		return nil, err
	}

	return dc, nil
}

func (dc *Store) Get(req cache.Request) (cache.Response, error) {
	return dc.get(req), nil
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

	rd := item.UncompressedBodyReader()
	defer rd.Close()

	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(f, rd)
	if err != nil {
		return fmt.Errorf("copy to file: %w", err)
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

func (dc *Store) Close() error {
	d, err := json.Marshal(dc.index)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dc.Dir, "index.json"), d, 0600)
}

func (dc *Store) OutputFilename(outputID string) string {
	return filepath.Join(dc.Dir, strings.Replace(outputID, "/", "_", -1))
}
