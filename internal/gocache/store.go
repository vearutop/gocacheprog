package gocache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/recordpool"
)

const (
	jobStartMarkerName    = ".gocacheprog-job-start-unixnano"
	restoredPathsListName = ".gocacheprog-restored-paths"
)

var validScopedKeyName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

const maxManifestKeyLen = 100

type Request struct {
	Commit            string
	ChangesID         string
	BuildType         string
	BaseCommit        string
	ParentCommit      string
	MaxFileBytes      int64
	RestoreLimitBytes int64
}

type FileItem struct {
	Path         string     `json:",omitempty"`
	Size         int64      `json:",omitempty"`
	ModTime      *time.Time `json:",omitempty"`
	Mode         uint32     `json:",omitempty"`
	IsCompressed bool       `json:",omitempty"`
	WireSize     int64      `json:",omitempty"`
	DiskPath     string     `json:",omitempty"`

	bodyReader func() (io.ReadCloser, error)
}

type Batch struct {
	Items []FileItem `json:",omitempty"`
}

type StreamWriter struct {
	w io.Writer
}

type TransferStats struct {
	Files             int           `json:"files"`
	CompressedBytes   int64         `json:"compressed_bytes"`
	UncompressedBytes int64         `json:"uncompressed_bytes"`
	Duration          time.Duration `json:"duration"`
}

type ClearStats struct {
	ManifestsDeleted int `json:"manifests_deleted"`
	ObjectsDeleted   int `json:"objects_deleted"`
	ObjectsKept      int `json:"objects_kept"`
}

type InspectStats struct {
	ManifestsCount           int   `json:"manifests_count"`
	FilesCount               int   `json:"files_count"`
	CompressedBytes          int64 `json:"compressed_bytes"`
	UncompressedBytes        int64 `json:"uncompressed_bytes"`
	MaxFileSize              int64 `json:"max_file_size"`
	MaxBandFilesCount        int   `json:"max_band_files_count"`
	MaxBandCompressedBytes   int64 `json:"max_band_compressed_bytes"`
	MaxBandUncompressedBytes int64 `json:"max_band_uncompressed_bytes"`
}

type Store struct {
	dir            string
	compress       bool
	maxDiskBytes   int64
	maxFileBytes   int64
	maxAge         time.Duration
	manifestMaxAge time.Duration
	evictionDelay  time.Duration

	mu                    sync.Mutex
	index                 map[string]indexEntry
	dirty                 bool
	ready                 bool
	currentDiskBytes      int64
	evictionScheduled     bool
	lastEvictionUnixMicro int64

	// pool packs small records (see recordpool.Pool) instead of one file per object, once a
	// given size has demonstrably earned it. Rebuilt from s.index and its own directory at
	// startup (see reconcilePoolLocked); nothing pool-related is persisted separately.
	pool *recordpool.Pool

	prevStats string
	hits      int64
	misses    int64
	puts      int64
	putsExist int64
	errors    int64
}

type indexEntry struct {
	Size            int64  `json:"n"`
	Mode            uint32 `json:"p,omitempty"`
	WireSize        int64  `json:"w,omitempty"`
	Compressed      int64  `json:"c,omitempty"`
	ModTimeMicro    int64  `json:"m,omitempty"`
	AccessTimeMicro int64  `json:"a,omitempty"`

	// PoolPage == 0 means "plain file at objectPath(relPath)" -- today's behavior, untouched.
	// The record size isn't stored here: it's always entryStoredSize(ie), so it can't drift out
	// of sync with Size/WireSize.
	PoolPage uint32 `json:"pp,omitempty"`
	PoolSlot uint32 `json:"ps,omitempty"`
}

type restoreEntry struct {
	path string
	ie   indexEntry
}

type StoreOption func(*Store)

func WithCompression() StoreOption {
	return func(s *Store) {
		s.compress = true
	}
}

func WithMaxDiskBytes(maxDiskBytes int64) StoreOption {
	return func(s *Store) {
		s.maxDiskBytes = maxDiskBytes
	}
}

func WithMaxFileBytes(maxFileBytes int64) StoreOption {
	return func(s *Store) {
		s.maxFileBytes = maxFileBytes
	}
}

// MaxFileBytes returns the configured single-file size limit, or 0 if unlimited.
func (s *Store) MaxFileBytes() int64 {
	return s.maxFileBytes
}

func WithMaxAge(maxAge time.Duration) StoreOption {
	return func(s *Store) {
		s.maxAge = maxAge
	}
}

// WithManifestMaxAge sets how long a manifest file may go unwritten before it's deleted by
// collectStaleManifests. Unlike cache objects, manifests are never touched again once their
// commit/PR/changes stream goes cold (loadManifest's self-heal only runs when something actually
// restores from that manifest), so nothing else ever prunes them.
func WithManifestMaxAge(manifestMaxAge time.Duration) StoreOption {
	return func(s *Store) {
		s.manifestMaxAge = manifestMaxAge
	}
}

func WithEvictionDelay(evictionDelay time.Duration) StoreOption {
	return func(s *Store) {
		s.evictionDelay = evictionDelay
	}
}

func NewStore(dir string, opts ...StoreOption) (*Store, error) {
	dir, err := toAbsPath(dir)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dir:            dir,
		maxAge:         48 * time.Hour,
		manifestMaxAge: 5 * 24 * time.Hour,
		evictionDelay:  5 * time.Minute,
		index:          make(map[string]indexEntry),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.pool = recordpool.New(filepath.Join(dir, "records"))

	data, err := os.ReadFile(s.indexPath())
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read index: %w", err)
		}
	} else if err := json.Unmarshal(data, &s.index); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	if err := s.reconcilePoolLocked(); err != nil {
		return nil, fmt.Errorf("reconcile record pool: %w", err)
	}

	for path, ie := range s.index {
		if ie.PoolPage != 0 {
			s.currentDiskBytes += s.entryStoredSize(ie)
			continue
		}
		if !s.objectExistsLocked(path) {
			delete(s.index, path)
			s.dirty = true
			continue
		}
		if s.maxFileBytes > 0 && ie.Size > s.maxFileBytes {
			delete(s.index, path)
			s.dirty = true
			if err := os.Remove(s.objectPath(path)); err != nil && !os.IsNotExist(err) {
				log.Printf("remove oversize native cache object %s: %v", path, err)
			}
			continue
		}
		s.currentDiskBytes += s.entryStoredSize(ie)
	}
	s.evictIfNeededLocked()
	s.ready = true

	return s, nil
}

func toAbsPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty path")
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	return filepath.Clean(filepath.Join(cwd, path)), nil
}

func (fi *FileItem) SetBodyReader(bodyReader func() (io.ReadCloser, error)) {
	fi.bodyReader = bodyReader
}

func (fi FileItem) FileMode() os.FileMode {
	if fi.Mode == 0 {
		return 0
	}

	return os.FileMode(fi.Mode)
}

func (fi *FileItem) PrepareBodyReader() {
	if fi.bodyReader != nil {
		return
	}
	if fi.DiskPath == "" {
		return
	}

	diskPath := fi.DiskPath
	fi.bodyReader = func() (io.ReadCloser, error) {
		if fi.WireSize > 0 && fi.WireSize < 1e6 {
			data, err := os.ReadFile(diskPath) //nolint:gosec // path is derived from controlled storage.
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		}

		f, err := os.Open(diskPath) //nolint:gosec // path is derived from controlled storage.
		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

func (fi *FileItem) UncompressedBodyReader() (io.ReadCloser, error) {
	fi.PrepareBodyReader()

	if fi.Size == 0 && fi.WireSize == 0 {
		return nil, nil
	}
	if fi.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", fi)
	}

	if fi.IsCompressed {
		rd, err := fi.bodyReader()
		if err != nil {
			return nil, err
		}

		zrd, err := zstd.NewReader(rd)
		if err != nil {
			if closeErr := rd.Close(); closeErr != nil {
				log.Printf("close compressed body reader after zstd init failure: %s", closeErr.Error())
			}
			return nil, err
		}

		return &zstdReadCloser{ReadCloser: zrd.IOReadCloser(), inner: rd}, nil
	}

	return fi.bodyReader()
}

func (fi *FileItem) CompressedBodyReader() (io.ReadCloser, error) {
	fi.PrepareBodyReader()

	if fi.Size == 0 && fi.WireSize == 0 {
		return nil, nil
	}
	if fi.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", fi)
	}
	if fi.IsCompressed {
		return fi.bodyReader()
	}

	rd, err := fi.bodyReader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rd.Close(); closeErr != nil {
			log.Printf("close body reader before compression: %s", closeErr.Error())
		}
	}()

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	buf := zstd.EncodeTo(make([]byte, 0, len(data)/2), data)
	fi.WireSize = int64(len(buf))
	fi.IsCompressed = true

	return io.NopCloser(bytes.NewReader(buf)), nil
}

func (fi *FileItem) WireBodyReader() (io.ReadCloser, error) {
	fi.PrepareBodyReader()

	if fi.Size == 0 && fi.WireSize == 0 {
		return nil, nil
	}
	if fi.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", fi)
	}

	return fi.bodyReader()
}

func (fi *FileItem) WriteTo(w io.Writer) (int64, error) {
	if fi.Size == 0 && fi.WireSize == 0 {
		return 0, nil
	}

	rd, err := fi.WireBodyReader()
	if err != nil {
		return 0, err
	}
	if rd == nil {
		return 0, nil
	}
	defer func() {
		if closeErr := rd.Close(); closeErr != nil {
			log.Printf("close file item body reader: %s", closeErr.Error())
		}
	}()

	wireSize := fi.WireSize
	if wireSize == 0 {
		wireSize = fi.Size
	}

	buf := make([]byte, wireSize)
	n, err := io.ReadFull(rd, buf)
	if err != nil {
		return int64(n), fmt.Errorf("read item body: %w", err)
	}

	n, err = w.Write(buf)
	if err != nil {
		return int64(n), fmt.Errorf("write item body: %w", err)
	}
	if int64(n) != wireSize {
		return int64(n), fmt.Errorf("unexpected item write: %d != %d", n, wireSize)
	}

	return int64(n), nil
}

type zstdReadCloser struct {
	io.ReadCloser
	inner io.Closer
}

func (z *zstdReadCloser) Close() error {
	err1 := z.ReadCloser.Close()
	err2 := z.inner.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type pagesReader struct {
	next func() ([]byte, error)
	buf  []byte
}

func (r *pagesReader) Read(p []byte) (n int, err error) {
	for len(r.buf) < len(p) {
		if r.next == nil {
			break
		}

		page, err := r.next()
		r.buf = append(r.buf, page...)
		if err != nil && errors.Is(err, io.EOF) {
			r.next = nil
			break
		}
		if err != nil {
			copy(p, r.buf)
			return len(r.buf), err
		}
	}

	n = copy(p, r.buf)
	remaining := r.buf[n:]
	r.buf = r.buf[:len(remaining)]
	copy(r.buf, remaining)

	if len(r.buf) == 0 && r.next == nil {
		return n, io.EOF
	}

	return n, nil
}

func (b *Batch) Reader() (io.Reader, error) {
	rd := &pagesReader{}

	jsonData, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	jsonLength, err := checkedJSONLength(jsonData)
	if err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, jsonLength); err != nil {
		return nil, fmt.Errorf("write head: %w", err)
	}
	if _, err := buf.Write(jsonData); err != nil {
		return nil, err
	}

	rd.buf = buf.Bytes()
	idx := 0
	rd.next = func() ([]byte, error) {
		if idx >= len(b.Items) {
			return nil, io.EOF
		}

		item := b.Items[idx]
		idx++

		buf.Reset()
		if _, err := item.WriteTo(buf); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	return rd, nil
}

func (b *Batch) ReaderNaive() (io.Reader, error) {
	buf := bytes.NewBuffer(nil)

	cl, err := b.ContentLength()
	if err != nil {
		return nil, fmt.Errorf("content length: %w", err)
	}

	n, err := b.WriteTo(buf)
	if err != nil {
		return nil, fmt.Errorf("write to buffer: %w", err)
	}

	if n != cl {
		return nil, fmt.Errorf("unexpected content length: %d bytes, expected %d", n, cl)
	}

	return buf, nil
}

func (b *Batch) WriteTo(w io.Writer) (int64, error) {
	var total int64

	jsonData, err := json.Marshal(b)
	if err != nil {
		return 0, err
	}

	jsonLength, err := checkedJSONLength(jsonData)
	if err != nil {
		return total, err
	}
	if err := binary.Write(w, binary.BigEndian, jsonLength); err != nil {
		return total, fmt.Errorf("write head: %w", err)
	}
	total += 4

	n, err := w.Write(jsonData)
	total += int64(n)
	if err != nil {
		return total, err
	}

	for i, item := range b.Items {
		n, err := item.WriteTo(w)
		total += n
		if err != nil {
			return total, fmt.Errorf("write item %d/%d: %w", i, len(b.Items), err)
		}
	}

	return total, nil
}

func (b *Batch) ReaderFrom(rd io.Reader, read func(item FileItem, body io.Reader) error) (int64, error) {
	var total int64

	var jsonLength int32
	if err := binary.Read(rd, binary.BigEndian, &jsonLength); err != nil {
		return total, err
	}
	total += 4

	jsonData := make([]byte, jsonLength)
	n, err := io.ReadFull(rd, jsonData)
	total += int64(n)
	if err != nil {
		return total, err
	}

	if err := json.Unmarshal(jsonData, b); err != nil {
		return total, err
	}

	for _, item := range b.Items {
		if item.WireSize == 0 {
			if err := read(item, nil); err != nil {
				return total, err
			}

			continue
		}

		consumed, err := readItemBody(rd, item.WireSize, func(cr io.Reader) error { return read(item, cr) })
		total += consumed

		if err != nil {
			return total, fmt.Errorf("item %+v: %w", item, err)
		}
	}

	return total, nil
}

// ErrShortRead marks a ReaderFrom/ReadStream item whose actual bytes on the wire fell short of
// its declared WireSize - most likely a server-side index entry that doesn't match the object it
// actually streamed. Callers can check for this via errors.Is to tell "this stream desynced
// mid-transfer" apart from other failures, and to treat it as a data-integrity anomaly worth
// surfacing rather than a fatal failure.
var ErrShortRead = errors.New("short read")

// readItemBody hands read a wireSize-bounded view of rd, then drains any part of it read left
// unconsumed, so the caller's stream offset stays aligned to this item's declared boundary
// regardless of what read did with the body. It returns an error if the actual byte count on
// the wire fell short of wireSize: failing loudly here, instead of silently continuing, matters
// because past a short item every later item in the same stream would be read from the wrong
// offset, corrupting each of them in turn rather than just this one.
func readItemBody(rd io.Reader, wireSize int64, read func(io.Reader) error) (int64, error) {
	cr := &countingReader{r: io.LimitReader(rd, wireSize)}

	if err := read(cr); err != nil {
		return 0, err
	}

	if _, err := io.Copy(io.Discard, cr); err != nil {
		return cr.n, fmt.Errorf("drain remaining body: %w", err)
	}

	if cr.n != wireSize {
		return cr.n, fmt.Errorf("%w: got %d bytes, want %d", ErrShortRead, cr.n, wireSize)
	}

	return cr.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.r == nil {
		return 0, io.EOF
	}
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

func (b *Batch) ContentLength() (int64, error) {
	jsonData, err := json.Marshal(b)
	if err != nil {
		return 0, err
	}

	total := int64(4 + len(jsonData))
	for _, item := range b.Items {
		size := item.Size
		if item.WireSize != 0 {
			size = item.WireSize
		}
		total += size
	}

	return total, nil
}

func checkedJSONLength(jsonData []byte) (int32, error) {
	if len(jsonData) > math.MaxInt32 {
		return 0, fmt.Errorf("response header too large: %d bytes", len(jsonData))
	}
	return int32(len(jsonData)), nil //nolint:gosec // checked above.
}

func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{w: w}
}

func (sw *StreamWriter) WriteItem(item FileItem) error {
	jsonData, err := json.Marshal(item)
	if err != nil {
		return err
	}

	jsonLength, err := checkedJSONLength(jsonData)
	if err != nil {
		return err
	}
	if err := binary.Write(sw.w, binary.BigEndian, jsonLength); err != nil {
		return fmt.Errorf("write item head: %w", err)
	}
	if _, err := sw.w.Write(jsonData); err != nil {
		return fmt.Errorf("write item meta: %w", err)
	}

	_, err = item.WriteTo(sw.w)
	return err
}

func (sw *StreamWriter) Close() error {
	return binary.Write(sw.w, binary.BigEndian, int32(0))
}

func ReadStream(rd io.Reader, read func(item FileItem, body io.Reader) error) (int64, error) { //nolint:unparam // total is used by callers (e.g. save_cache.go) to annotate error messages.
	var total int64

	for {
		var jsonLength int32
		if err := binary.Read(rd, binary.BigEndian, &jsonLength); err != nil {
			if errors.Is(err, io.EOF) {
				return total, nil
			}
			return total, err
		}
		total += 4

		if jsonLength == 0 {
			return total, nil
		}
		if jsonLength < 0 {
			return total, fmt.Errorf("negative item header length: %d", jsonLength)
		}

		jsonData := make([]byte, jsonLength)
		n, err := io.ReadFull(rd, jsonData)
		total += int64(n)
		if err != nil {
			return total, err
		}

		var item FileItem
		if err := json.Unmarshal(jsonData, &item); err != nil {
			return total, err
		}

		wireSize := item.WireSize
		if wireSize == 0 {
			wireSize = item.Size
		}

		if wireSize > 0 {
			consumed, err := readItemBody(rd, wireSize, func(cr io.Reader) error { return read(item, cr) })
			total += consumed

			if err != nil {
				return total, fmt.Errorf("item %+v: %w", item, err)
			}
			continue
		}

		if err := read(item, nil); err != nil {
			return total, err
		}
	}
}

func (s *Store) Restore(req Request, cb func(FileItem)) ([]string, error) {
	paths, sources, err := s.restorePaths(req)
	if err != nil {
		return nil, err
	}

	nowUnixMicro := time.Now().UTC().UnixMicro()
	entries := make([]restoreEntry, 0, len(paths))

	s.mu.Lock()
	for _, relPath := range paths {
		ie, ok := s.index[relPath]
		if !ok {
			continue
		}

		s.index[relPath] = indexEntry{
			Size:            ie.Size,
			Mode:            ie.Mode,
			WireSize:        ie.WireSize,
			Compressed:      ie.Compressed,
			ModTimeMicro:    ie.ModTimeMicro,
			AccessTimeMicro: nowUnixMicro,
			PoolPage:        ie.PoolPage,
			PoolSlot:        ie.PoolSlot,
		}
		s.dirty = true

		entries = append(entries, restoreEntry{path: relPath, ie: ie})
	}
	s.mu.Unlock()

	entries = s.selectRestoreEntries(req, entries)

	for _, entry := range entries {
		// Plain-file entries (not pool-backed) predate storePutBody validating a write's actual
		// length -- a pre-existing truncated-but-indexed object could still be on disk. Catch it
		// here, at serve time, rather than handing a client bytes it can't decompress: skip it
		// and drop it from the store so the next restore doesn't hit it either.
		if entry.ie.PoolPage == 0 {
			expected := s.entryStoredSize(entry.ie)
			fi, statErr := os.Stat(s.objectPath(entry.path))
			if statErr != nil || fi.Size() != expected {
				log.Printf("restore: skipping broken object path=%q expected_size=%d actual_size_err=%v", entry.path, expected, statErr)
				s.removeBrokenEntry(entry.path, entry.ie)
				continue
			}
		}

		item := s.responseItem(entry.path, entry.ie)
		cb(item)
		atomic.AddInt64(&s.hits, 1)
	}

	return sources, nil
}

// removeBrokenEntry drops a corrupted/truncated on-disk object from the index and deletes its
// file, so a future restore skips it instead of repeatedly serving something a client can't use.
func (s *Store) removeBrokenEntry(relPath string, ie indexEntry) {
	s.mu.Lock()
	if current, ok := s.index[relPath]; ok {
		delete(s.index, relPath)
		s.currentDiskBytes -= s.entryStoredSize(current)
		s.dirty = true
	}
	s.mu.Unlock()

	if err := s.removeObjectLocked(relPath, ie); err != nil {
		log.Printf("remove broken object %s: %s", relPath, err.Error())
	}
}

func (s *Store) RestoreSources(req Request) ([]string, error) {
	_, sources, err := s.restorePaths(req)
	return sources, err
}

func (s *Store) Save(req Request, batch Batch) error {
	paths := make([]string, 0, len(batch.Items))

	for _, item := range batch.Items {
		path, err := cleanRelativePath(item.Path)
		if err != nil {
			return err
		}

		item.Path = path
		if err := s.putOne(item); err != nil {
			return err
		}
		paths = append(paths, item.Path)
	}

	return s.MergeSavedPaths(req, paths)
}

func (s *Store) SaveItem(item FileItem) error {
	return s.putOne(item)
}

func (s *Store) Clear(req Request) (ClearStats, error) {
	targetManifests, err := s.targetManifestPaths(req)
	if err != nil {
		return ClearStats{}, err
	}
	if len(targetManifests) == 0 {
		return ClearStats{}, errors.New("at least one of build-type, commit, or changes-id must be set")
	}

	targetSet := make(map[string]struct{}, len(targetManifests))
	targetRefs := make(map[string]struct{})
	stats := ClearStats{}

	for _, manifestPath := range targetManifests {
		targetSet[manifestPath] = struct{}{}
		paths, err := readManifestPaths(manifestPath)
		if err != nil && !os.IsNotExist(err) {
			return ClearStats{}, err
		}
		for _, relPath := range paths {
			targetRefs[relPath] = struct{}{}
		}
		if err := os.Remove(manifestPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return ClearStats{}, fmt.Errorf("remove manifest: %w", err)
		}
		stats.ManifestsDeleted++
	}

	keepRefs, err := s.scanManifestRefs(targetSet)
	if err != nil {
		return ClearStats{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for relPath := range targetRefs {
		if _, keep := keepRefs[relPath]; keep {
			stats.ObjectsKept++
			continue
		}

		ie, ok := s.index[relPath]
		if ok {
			delete(s.index, relPath)
			s.currentDiskBytes -= s.entryStoredSize(ie)
			s.dirty = true
			if err := s.removeObjectLocked(relPath, ie); err != nil {
				return stats, fmt.Errorf("remove object %s: %w", relPath, err)
			}
		} else if err := os.Remove(s.objectPath(relPath)); err != nil && !os.IsNotExist(err) {
			return stats, fmt.Errorf("remove object %s: %w", relPath, err)
		}
		stats.ObjectsDeleted++
	}

	return stats, nil
}

func (s *Store) Inspect(req Request) (InspectStats, error) {
	targetManifests, err := s.targetManifestPaths(req)
	if err != nil {
		return InspectStats{}, err
	}
	if len(targetManifests) == 0 {
		return InspectStats{}, errors.New("at least one of build-type, commit, or changes-id must be set")
	}

	refs := make(map[string]struct{})
	stats := InspectStats{ManifestsCount: len(targetManifests)}

	for _, manifestPath := range targetManifests {
		paths, err := readManifestPaths(manifestPath)
		if err != nil && !os.IsNotExist(err) {
			return InspectStats{}, err
		}
		for _, relPath := range paths {
			refs[relPath] = struct{}{}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for relPath := range refs {
		ie, ok := s.index[relPath]
		if !ok || !s.objectExistsLocked(relPath) {
			continue
		}
		stats.FilesCount++
		stats.UncompressedBytes += ie.Size
		stats.CompressedBytes += s.entryStoredSize(ie)
		if ie.Size > stats.MaxFileSize {
			stats.MaxFileSize = ie.Size
		}
	}

	if stats.MaxFileSize == 0 {
		return stats, nil
	}

	maxBandMin := stats.MaxFileSize - stats.MaxFileSize/10
	for relPath := range refs {
		ie, ok := s.index[relPath]
		if !ok || !s.objectExistsLocked(relPath) || ie.Size < maxBandMin {
			continue
		}
		stats.MaxBandFilesCount++
		stats.MaxBandUncompressedBytes += ie.Size
		stats.MaxBandCompressedBytes += s.entryStoredSize(ie)
	}

	return stats, nil
}

func (s *Store) AppendUploadPaths(uploadID string, paths []string) error {
	uploadPath, err := s.uploadSessionPath(uploadID)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(uploadPath), 0o750); err != nil {
		return fmt.Errorf("create upload session dir: %w", err)
	}

	f, err := os.OpenFile(uploadPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return fmt.Errorf("open upload session: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close upload session %s: %s", uploadPath, closeErr.Error())
		}
	}()

	for _, relPath := range paths {
		relPath = strings.TrimSpace(relPath)
		if relPath == "" {
			continue
		}
		if _, err := io.WriteString(f, relPath+"\n"); err != nil {
			return fmt.Errorf("append upload session: %w", err)
		}
	}

	return nil
}

func (s *Store) FinalizeUpload(req Request, uploadID string) error {
	uploadPath, err := s.uploadSessionPath(uploadID)
	if err != nil {
		return err
	}

	paths, _, err := s.loadManifest(uploadPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := s.MergeSavedPaths(req, paths); err != nil {
		return err
	}

	if err := os.Remove(uploadPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove upload session: %w", err)
	}

	return nil
}

func (s *Store) MergeSavedPaths(req Request, paths []string) error {
	if req.Commit != "" {
		manifestPath, err := s.commitManifestPath(req.Commit, req.BuildType)
		if err != nil {
			return err
		}
		if err := s.mergeManifest(manifestPath, paths); err != nil {
			return err
		}
	}

	if req.ChangesID != "" {
		manifestPath, err := s.changesManifestPath(req.ChangesID, req.BuildType)
		if err != nil {
			return err
		}
		if err := s.mergeManifest(manifestPath, paths); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) putOne(item FileItem) error {
	if item.Path == "" {
		return errors.New("empty path")
	}
	if s.maxFileBytes > 0 && item.Size > s.maxFileBytes {
		return nil
	}

	atomic.AddInt64(&s.puts, 1)

	s.mu.Lock()
	existing, hasExisting := s.index[item.Path]
	s.mu.Unlock()
	if hasExisting {
		atomic.AddInt64(&s.putsExist, 1)
	}

	modTime := time.Now().UTC()
	ie := indexEntry{
		Size:            item.Size,
		Mode:            item.Mode,
		ModTimeMicro:    modTime.UnixMicro(),
		AccessTimeMicro: modTime.UnixMicro(),
	}

	rd, err := bodyReaderForPut(item, s.compress, &ie)
	if err != nil {
		atomic.AddInt64(&s.errors, 1)
		return fmt.Errorf("get reader for save: %w", err)
	}
	if rd != nil {
		defer func() {
			if closeErr := rd.Close(); closeErr != nil {
				log.Printf("close save reader: %s", closeErr.Error())
			}
		}()
	}

	if err := s.storePutBody(item, &ie, rd, modTime); err != nil {
		atomic.AddInt64(&s.errors, 1)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if hasExisting {
		s.currentDiskBytes -= s.entryStoredSize(existing)
		s.releaseSupersededLocked(item.Path, existing, ie)
	}
	s.currentDiskBytes += s.entryStoredSize(ie)
	s.index[item.Path] = ie
	s.dirty = true
	s.scheduleEvictionLocked()

	return nil
}

// bodyReaderForPut picks the right reader for item given whether the store compresses, setting
// ie.Compressed/ie.WireSize when the body ends up compressed on disk.
func bodyReaderForPut(item FileItem, compress bool, ie *indexEntry) (io.ReadCloser, error) {
	switch {
	case item.IsCompressed && compress:
		ie.Compressed = 1
		ie.WireSize = item.WireSize
		return item.WireBodyReader()
	case item.IsCompressed && !compress:
		return item.UncompressedBodyReader()
	case !item.IsCompressed && compress && item.Size >= cache.MinCompressionSize:
		return compressNowBodyReader(item, ie)
	default:
		return item.UncompressedBodyReader()
	}
}

// compressNowBodyReader compresses item's body immediately, rather than handing back a reader
// that would compress lazily on read, so ie.WireSize reflects the real compressed length by the
// time this returns. item.WireSize can't be used here the way the item.IsCompressed branches
// above do: this item arrived uncompressed, so its WireSize field was never meaningful, and
// there's no way to know the real compressed length without actually compressing first -- pool
// eligibility and disk accounting both need that number to be trustworthy.
func compressNowBodyReader(item FileItem, ie *indexEntry) (io.ReadCloser, error) {
	rd, err := item.CompressedBodyReader()
	if err != nil {
		return nil, err
	}

	compressed, err := io.ReadAll(rd)
	if closeErr := rd.Close(); closeErr != nil {
		log.Printf("close compressed body reader: %s", closeErr.Error())
	}
	if err != nil {
		return nil, err
	}

	ie.Compressed = 1
	ie.WireSize = int64(len(compressed))

	return io.NopCloser(bytes.NewReader(compressed)), nil
}

// storePutBody writes rd's bytes to wherever ie belongs -- a pool slot if eligible and promoted,
// a plain file otherwise -- setting ie.PoolPage/PoolSlot when it lands in the pool.
func (s *Store) storePutBody(item FileItem, ie *indexEntry, rd io.Reader, modTime time.Time) error {
	recordSize := s.entryStoredSize(*ie)
	poolEligible := recordSize > 0 && recordSize <= s.pool.MaxRecordBytes()

	var body []byte
	if poolEligible {
		var err error
		body, err = io.ReadAll(rd)
		if err != nil {
			return fmt.Errorf("read save body: %w", err)
		}
		if int64(len(body)) != recordSize {
			return fmt.Errorf("save body length %d does not match expected %d", len(body), recordSize)
		}

		loc, ok, poolErr := s.pool.Put(recordSize, body)
		if poolErr != nil {
			return fmt.Errorf("write pooled record: %w", s.handleWriteErr(poolErr))
		}
		if ok {
			ie.PoolPage, ie.PoolSlot = loc.Page, loc.Slot
			return nil
		}
	}

	// Not pool-eligible, or the size hasn't crossed the promotion threshold yet -- plain file,
	// same as before the record pool existed.
	mode := item.FileMode()
	if mode == 0 {
		mode = 0o600
	}

	objectPath := s.objectPath(item.Path)

	var src io.Reader
	var counted *countingReader
	if poolEligible {
		src = bytes.NewReader(body)
	} else {
		// Unlike the pool path above, rd hasn't been read yet, so its length can't be checked
		// before writing. io.Copy inside writeAtomic treats a short read from rd (e.g. an
		// interrupted upload) as a normal, successful end of input -- count what's actually
		// written and verify it after, so a truncated body is caught here rather than silently
		// indexed as a complete, valid object that fails to decompress whenever it's later read.
		counted = &countingReader{r: rd}
		src = counted
	}

	if err := writeAtomic(objectPath, src, mode); err != nil {
		return fmt.Errorf("write object: %w", s.handleWriteErr(err))
	}

	if counted != nil && counted.n != recordSize {
		if rmErr := os.Remove(objectPath); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("remove truncated object %s: %s", objectPath, rmErr.Error())
		}
		return fmt.Errorf("save body length %d does not match expected %d", counted.n, recordSize)
	}

	if err := os.Chtimes(objectPath, modTime, modTime); err != nil {
		return fmt.Errorf("set object mtime: %w", err)
	}

	return nil
}

// releaseSupersededLocked frees whatever existing occupied when ie now lives somewhere different
// -- same key, rewritten in place. Called with s.mu held.
func (s *Store) releaseSupersededLocked(relPath string, existing indexEntry, ie indexEntry) {
	if existing.PoolPage != 0 {
		s.pool.Free(s.entryStoredSize(existing), recordpool.Loc{Page: existing.PoolPage, Slot: existing.PoolSlot})
		return
	}
	if ie.PoolPage != 0 {
		// The old plain file is superseded by a pool slot now, rather than overwritten in place
		// by storePutBody's writeAtomic -- remove it explicitly.
		if err := os.Remove(s.objectPath(relPath)); err != nil && !os.IsNotExist(err) {
			log.Printf("remove superseded plain object %s: %s", relPath, err.Error())
		}
	}
}

func (s *Store) responseItem(relPath string, ie indexEntry) FileItem {
	item := FileItem{
		Path:         relPath,
		Size:         ie.Size,
		Mode:         ie.Mode,
		WireSize:     ie.WireSize,
		IsCompressed: ie.Compressed == 1,
	}
	if item.WireSize == 0 {
		item.WireSize = item.Size
	}
	if ie.ModTimeMicro != 0 {
		t := time.UnixMicro(ie.ModTimeMicro)
		item.ModTime = &t
	}

	if ie.PoolPage != 0 {
		recordSize := s.entryStoredSize(ie)
		loc := recordpool.Loc{Page: ie.PoolPage, Slot: ie.PoolSlot}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			buf, err := s.pool.Read(recordSize, loc)
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(buf)), nil
		})
	} else {
		item.DiskPath = s.objectPath(relPath)
	}

	return item
}

func (s *Store) selectRestoreEntries(req Request, entries []restoreEntry) []restoreEntry {
	filtered := entries[:0]
	for _, entry := range entries {
		if req.MaxFileBytes > 0 && entry.ie.Size > req.MaxFileBytes {
			continue
		}
		if s.maxFileBytes > 0 && entry.ie.Size > s.maxFileBytes {
			continue
		}
		filtered = append(filtered, entry)
	}

	if req.RestoreLimitBytes <= 0 || len(filtered) < 2 {
		if req.RestoreLimitBytes > 0 && len(filtered) == 1 && s.entryStoredSize(filtered[0].ie) > req.RestoreLimitBytes {
			return filtered[:0]
		}
		return filtered
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].ie.ModTimeMicro != filtered[j].ie.ModTimeMicro {
			return filtered[i].ie.ModTimeMicro > filtered[j].ie.ModTimeMicro
		}
		if filtered[i].ie.Size != filtered[j].ie.Size {
			return filtered[i].ie.Size < filtered[j].ie.Size
		}
		return filtered[i].path < filtered[j].path
	})

	total := int64(0)
	limit := 0
	for _, entry := range filtered {
		size := s.entryStoredSize(entry.ie)
		if total+size > req.RestoreLimitBytes {
			break
		}
		total += size
		limit++
	}

	return filtered[:limit]
}

func (s *Store) restorePaths(req Request) ([]string, []string, error) {
	if strings.TrimSpace(req.Commit) == "" &&
		strings.TrimSpace(req.ParentCommit) == "" &&
		strings.TrimSpace(req.ChangesID) == "" &&
		strings.TrimSpace(req.BaseCommit) == "" {
		return nil, []string{"none"}, nil
	}

	seen := map[string]struct{}{}
	result := make([]string, 0)
	sources := make([]string, 0, 4)

	for _, candidate := range []struct {
		name string
		load func() ([]string, bool, string, error)
	}{
		{name: "commit", load: func() ([]string, bool, string, error) { return s.loadCommitManifest(req.Commit, req.BuildType) }},
		{name: "parent", load: func() ([]string, bool, string, error) { return s.loadCommitManifest(req.ParentCommit, req.BuildType) }},
		{name: "changes", load: func() ([]string, bool, string, error) { return s.loadChangesManifest(req.ChangesID, req.BuildType) }},
		{name: "base", load: func() ([]string, bool, string, error) { return s.loadCommitManifest(req.BaseCommit, req.BuildType) }},
	} {
		paths, changed, manifestPath, err := candidate.load()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, nil, err
		}
		if changed {
			body := strings.Join(paths, "\n")
			if body != "" {
				body += "\n"
			}
			// Self-heal write-back is opportunistic housekeeping (dedup/prune stale entries);
			// paths already loaded are good regardless, so a failure here (e.g. disk full)
			// shouldn't fail the restore.
			if err := s.writeManifest(manifestPath, body); err != nil {
				log.Printf("restore-cache: self-heal manifest %s: %s; serving in-memory result", manifestPath, err.Error())
			}
		}
		sources = append(sources, candidate.name)
		for _, relPath := range paths {
			if _, ok := seen[relPath]; ok {
				continue
			}
			seen[relPath] = struct{}{}
			result = append(result, relPath)
		}
	}

	if len(sources) > 0 {
		if err := s.supplementSmallResultWithNewest(req.BuildType, &result, &sources, seen); err != nil {
			return nil, nil, err
		}
	}

	if len(sources) == 0 {
		newest, err := s.newestFallbackPaths(req.BuildType, seen)
		if err != nil {
			return nil, nil, err
		}

		if len(newest) > 0 {
			sources = []string{"newest"}
			result = append(result, newest...)
		}
	}

	if len(sources) == 0 {
		sources = []string{"none"}
	}

	return result, sources, nil
}

// smallRestoreRatio: when a resolved commit/parent/changes/base result has fewer paths than this
// fraction of the newest manifest for the same build type, treat it as likely hollowed out by
// eviction (which has no awareness of which manifests still reference an object) and supplement
// it with the newest manifest's paths too, rather than silently serving a shrunken result. Builds
// tend to migrate forward to newer code, so the newest manifest for a build type is usually still
// broadly relevant even to an older commit/PR (same reasoning as newestFallbackPaths below).
const smallRestoreRatio = 0.5

// supplementSmallResultWithNewest tops up result/sources in place with the newest manifest's
// paths for buildType, if the result resolved so far looks abnormally small next to it (see
// smallRestoreRatio). A no-op if there's no newest manifest, or the result isn't small.
func (s *Store) supplementSmallResultWithNewest(buildType string, result *[]string, sources *[]string, seen map[string]struct{}) error {
	newestPaths, changed, manifestPath, err := s.loadNewestManifest(buildType)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	if len(newestPaths) == 0 || float64(len(*result)) >= float64(len(newestPaths))*smallRestoreRatio {
		return nil
	}

	if changed {
		body := strings.Join(newestPaths, "\n")
		if body != "" {
			body += "\n"
		}
		// See restorePaths: self-heal write-back failing shouldn't fail the restore.
		if err := s.writeManifest(manifestPath, body); err != nil {
			log.Printf("restore-cache: self-heal manifest %s: %s; serving in-memory result", manifestPath, err.Error())
		}
	}

	added := false
	for _, relPath := range newestPaths {
		if _, ok := seen[relPath]; ok {
			continue
		}
		seen[relPath] = struct{}{}
		*result = append(*result, relPath)
		added = true
	}
	if added {
		*sources = append(*sources, "newest")
	}

	return nil
}

// newestFallbackPaths is restorePaths' last-resort source: when no commit/parent/changes/base
// manifest matched anything, it loads whichever manifest for this build type was written most
// recently (see loadNewestManifest), self-heals it if needed, and returns whatever of its paths
// aren't already in seen. Returns (nil, nil) - not an error - when no manifest exists at all yet.
func (s *Store) newestFallbackPaths(buildType string, seen map[string]struct{}) ([]string, error) {
	paths, changed, manifestPath, err := s.loadNewestManifest(buildType)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	if changed {
		body := strings.Join(paths, "\n")
		if body != "" {
			body += "\n"
		}

		// See restorePaths: self-heal write-back failing shouldn't fail the restore.
		if err := s.writeManifest(manifestPath, body); err != nil {
			log.Printf("restore-cache: self-heal manifest %s: %s; serving in-memory result", manifestPath, err.Error())
		}
	}

	result := make([]string, 0, len(paths))
	for _, relPath := range paths {
		if _, ok := seen[relPath]; ok {
			continue
		}

		seen[relPath] = struct{}{}
		result = append(result, relPath)
	}

	return result, nil
}

// loadNewestManifest is the last-resort restore source: when no commit/parent/changes/base
// manifest matched - a long pause with nothing relevant built on the target branch since, or the
// very first build of a new build type - it falls back to whichever manifest for this build type
// was written most recently, from any commit or PR. In practice, cache entries are usually still
// largely relevant across unrelated PRs of the same build type, so this beats every PR starting
// completely cold until the target branch catches up. Returns os.ErrNotExist if no manifest
// exists yet for this build type at all.
func (s *Store) loadNewestManifest(buildType string) ([]string, bool, string, error) {
	scopeDir, err := manifestScopeDir(buildType)
	if err != nil {
		return nil, false, "", err
	}

	files, err := listManifestFiles(filepath.Join(s.dir, "manifests", scopeDir))
	if err != nil {
		return nil, false, "", err
	}

	var (
		newestPath string
		newestTime time.Time
	)

	for _, path := range files {
		info, statErr := os.Stat(path)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}

			return nil, false, "", statErr
		}

		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newestPath = path
		}
	}

	if newestPath == "" {
		return nil, false, "", os.ErrNotExist
	}

	paths, changed, err := s.loadManifest(newestPath)

	return paths, changed, newestPath, err
}

func (s *Store) loadCommitManifest(commit string, buildType string) ([]string, bool, string, error) {
	if strings.TrimSpace(commit) == "" {
		return nil, false, "", os.ErrNotExist
	}
	manifestPath, err := s.commitManifestPath(commit, buildType)
	if err != nil {
		return nil, false, "", err
	}

	paths, changed, err := s.loadManifest(manifestPath)
	return paths, changed, manifestPath, err
}

func (s *Store) loadChangesManifest(changesID string, buildType string) ([]string, bool, string, error) {
	if strings.TrimSpace(changesID) == "" {
		return nil, false, "", os.ErrNotExist
	}
	manifestPath, err := s.changesManifestPath(changesID, buildType)
	if err != nil {
		return nil, false, "", err
	}

	paths, changed, err := s.loadManifest(manifestPath)
	return paths, changed, manifestPath, err
}

func (s *Store) loadManifest(manifestPath string) ([]string, bool, error) {
	f, err := os.Open(manifestPath) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close manifest %s: %s", manifestPath, closeErr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	res := make([]string, 0)
	seen := map[string]struct{}{}
	changed := false

	for scanner.Scan() {
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			changed = true
			continue
		}

		relPath, err := cleanRelativePath(raw)
		if err != nil {
			changed = true
			continue
		}
		if _, ok := seen[relPath]; ok {
			changed = true
			continue
		}
		seen[relPath] = struct{}{}

		s.mu.Lock()
		live := s.objectExistsLocked(relPath)
		s.mu.Unlock()
		if !live {
			changed = true
			continue
		}

		res = append(res, relPath)
	}

	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("scan manifest %s: %w", manifestPath, err)
	}

	return res, changed, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	poolErr := s.pool.Close()

	if !s.ready || !s.dirty {
		return poolErr
	}

	data, err := json.Marshal(s.index)
	if err != nil {
		return errors.Join(poolErr, err)
	}

	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return errors.Join(poolErr, err)
	}

	if err := writeFileAtomic(s.indexPath(), data, 0o600); err != nil {
		return errors.Join(poolErr, err)
	}

	s.dirty = false
	return poolErr
}

func (s *Store) objectPath(relPath string) string {
	return filepath.Join(s.dir, "objects", filepath.FromSlash(relPath))
}

func (s *Store) objectExistsLocked(relPath string) bool {
	ie, ok := s.index[relPath]
	if !ok {
		return false
	}
	if ie.PoolPage != 0 {
		// Pool-backed entries have no per-item file to stat; validity was already established by
		// reconcilePoolLocked at startup and is maintained incrementally from then on.
		return true
	}
	_, err := os.Stat(s.objectPath(relPath))
	return err == nil
}

// ExistingPaths returns the subset of paths already present in the store, so a caller about to
// upload a batch (e.g. save-cache) can skip re-compressing and re-uploading what the server
// already has instead of finding out only after paying that cost. Index lookups happen under
// s.mu, but the (potentially many) os.Stat calls for plain-file entries run unlocked, so a large
// batch doesn't hold up concurrent puts/evictions for the duration of the check.
func (s *Store) ExistingPaths(paths []string) []string {
	s.mu.Lock()
	existing := make([]string, 0, len(paths))
	var plainFiles []string
	for _, p := range paths {
		ie, ok := s.index[p]
		if !ok {
			continue
		}
		if ie.PoolPage != 0 {
			existing = append(existing, p)
			continue
		}
		plainFiles = append(plainFiles, p)
	}
	s.mu.Unlock()

	for _, p := range plainFiles {
		if _, err := os.Stat(s.objectPath(p)); err == nil {
			existing = append(existing, p)
		}
	}

	return existing
}

func (s *Store) entryStoredSize(ie indexEntry) int64 {
	if ie.WireSize > 0 {
		return ie.WireSize
	}
	return ie.Size
}

func (s *Store) scheduleEvictionLocked() {
	if s.evictionScheduled {
		return
	}
	if s.maxAge <= 0 && s.manifestMaxAge <= 0 && (s.maxDiskBytes <= 0 || s.currentDiskBytes <= s.maxDiskBytes) {
		return
	}

	s.evictionScheduled = true
	delay := s.evictionDelay
	go func() {
		time.Sleep(delay)

		s.mu.Lock()
		s.evictionScheduled = false
		s.evictIfNeededLocked()
		s.mu.Unlock()

		// Pure filesystem work over dir/manifests, unrelated to the in-memory index/
		// currentDiskBytes that evictIfNeededLocked guards, so it runs without s.mu held.
		s.collectStaleManifests()
	}()
}

// collectStaleManifests deletes manifest files under dir/manifests whose mtime is older than
// manifestMaxAge. Manifests accumulate forever otherwise: a PR or branch that goes cold is never
// restored from again, so loadManifest's self-heal (which only runs on an actual restore) never
// gets a chance to even shrink it, let alone remove it.
func (s *Store) collectStaleManifests() {
	if s.manifestMaxAge <= 0 {
		return
	}

	files, err := listManifestFiles(filepath.Join(s.dir, "manifests"))
	if err != nil {
		log.Printf("collect stale manifests: list %s: %s", s.dir, err.Error())
		return
	}

	cutoff := time.Now().UTC().Add(-s.manifestMaxAge)
	removed := 0

	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Printf("collect stale manifests: stat %s: %s", path, err.Error())
			}
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("collect stale manifests: remove %s: %s", path, err.Error())
			continue
		}
		removed++
	}

	if removed > 0 {
		log.Printf("collect stale manifests: removed %d manifest(s) older than %s", removed, humanDuration(s.manifestMaxAge))
	}
}

// humanDuration renders d without Duration.String()'s trailing zero minute/second units for the
// common case of a whole-hour config value (e.g. "120h" instead of "120h0m0s"). Trimming the
// literal "0m0s" suffix is only safe because the guard above guarantees minutes are exactly zero
// whenever it fires -- unconditionally, it could also strip a real "30m0s" down to "3".
func humanDuration(d time.Duration) string {
	if d > 0 && d%time.Hour == 0 {
		return strings.TrimSuffix(d.String(), "0m0s")
	}
	return d.String()
}

// EvictNow forces an eviction pass immediately, bypassing evictionDelay.
func (s *Store) EvictNow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictIfNeededLocked()
}

// DiskBytes reports current tracked on-disk usage, for an external combined-budget enforcer.
func (s *Store) DiskBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.currentDiskBytes
}

// EvictOne evicts a single least-recently-used entry regardless of maxDiskBytes, for use by an
// external combined-budget enforcer spanning multiple stores. Reports whether anything was
// evicted.
func (s *Store) EvictOne() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.evictOneLocked()
}

func (s *Store) evictIfNeededLocked() {
	if s.maxAge > 0 {
		cutoff := time.Now().UTC().Add(-s.maxAge).UnixMicro()
		for relPath, ie := range s.index {
			if ie.ModTimeMicro == 0 || ie.ModTimeMicro >= cutoff {
				continue
			}
			delete(s.index, relPath)
			s.currentDiskBytes -= s.entryStoredSize(ie)
			s.lastEvictionUnixMicro = time.Now().UTC().UnixMicro()
			s.dirty = true
			if err := s.removeObjectLocked(relPath, ie); err != nil {
				log.Printf("remove expired native cache object %s: %v", relPath, err)
			}
		}
	}

	if s.maxDiskBytes <= 0 {
		return
	}

	for s.currentDiskBytes > s.maxDiskBytes && s.evictOneLocked() {
	}
}

// evictOneLocked removes the single least-recently-used cache object regardless of maxDiskBytes
// and reports whether one was evicted (false once the index is empty). Shared by the
// budget-based loop above and evictOnDiskFullLocked below.
func (s *Store) evictOneLocked() bool {
	var (
		evictPath string
		evictIE   indexEntry
		found     bool
	)

	for relPath, ie := range s.index {
		if !found || lruTimeMicro(ie) < lruTimeMicro(evictIE) {
			evictPath = relPath
			evictIE = ie
			found = true
		}
	}
	if !found {
		return false
	}

	delete(s.index, evictPath)
	s.currentDiskBytes -= s.entryStoredSize(evictIE)
	s.lastEvictionUnixMicro = time.Now().UTC().UnixMicro()
	s.dirty = true
	if err := s.removeObjectLocked(evictPath, evictIE); err != nil {
		log.Printf("remove native cache object %s: %v", evictPath, err)
	}

	return true
}

// diskFullEvictFraction is the share of the index force-evicted, oldest first, whenever a write
// hits ENOSPC. maxDiskBytes accounting only tracks this Store's own cache objects, not manifests,
// uploads, or whatever else shares the disk, so a full disk doesn't imply currentDiskBytes >
// maxDiskBytes. Evicting an LRU slice regardless of budget gives the next write a chance to
// succeed instead of every request failing until an operator notices.
const diskFullEvictFraction = 10

func (s *Store) evictOnDiskFullLocked() {
	n := len(s.index) / diskFullEvictFraction
	if n == 0 {
		n = 1
	}

	evicted := 0
	for ; evicted < n; evicted++ {
		if !s.evictOneLocked() {
			break
		}
	}

	log.Printf("disk full: force-evicted %d native cache object(s) to free space", evicted)
}

// handleWriteErr forces an eviction pass when err looks like the disk is out of space, then
// returns err unchanged so callers can keep wrapping/propagating it as before.
func (s *Store) handleWriteErr(err error) error {
	if err != nil && errors.Is(err, syscall.ENOSPC) {
		s.mu.Lock()
		s.evictOnDiskFullLocked()
		s.mu.Unlock()
	}

	return err
}

// removeObjectLocked releases whatever storage ie occupies: a pool slot, or a plain file.
func (s *Store) removeObjectLocked(relPath string, ie indexEntry) error {
	if ie.PoolPage != 0 {
		s.pool.Free(s.entryStoredSize(ie), recordpool.Loc{Page: ie.PoolPage, Slot: ie.PoolSlot})
		return nil
	}
	if err := os.Remove(s.objectPath(relPath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// reconcilePoolLocked runs once, from NewStore: it discovers existing pool pages, then makes one
// pass over the already-loaded index to (a) drop entries whose PoolPage/PoolSlot no longer
// resolves to anything on disk (the same treatment objectExistsLocked already gives a plain file
// that's gone missing), and (b) feed everything else to the pool so it can derive free lists and
// promotion counters. Nothing pool-related is persisted separately -- it's all rebuilt from the
// index and the pool's own directory every time.
func (s *Store) reconcilePoolLocked() error {
	if err := s.pool.DiscoverPages(); err != nil {
		return err
	}

	for relPath, ie := range s.index {
		size := s.entryStoredSize(ie)
		loc := recordpool.Loc{Page: ie.PoolPage, Slot: ie.PoolSlot}

		if !s.pool.Valid(size, loc) {
			delete(s.index, relPath)
			s.dirty = true
			continue
		}

		if loc.IsZero() {
			s.pool.NoteUnpromoted(size)
		} else {
			s.pool.NoteOccupied(size, loc)
		}
	}

	s.pool.FinishReconcile()

	return nil
}

func lruTimeMicro(ie indexEntry) int64 {
	if ie.AccessTimeMicro != 0 {
		return ie.AccessTimeMicro
	}
	return ie.ModTimeMicro
}

// writeAtomicSeq disambiguates concurrent writers' temp file names. Two writers can
// legitimately target the same relative path (e.g. concurrent uploads of an identical
// native cache file); a shared ".tmp" suffix would let one writer's rename consume the
// other's temp file, failing with ENOENT.
var writeAtomicSeq atomic.Int64

func writeAtomic(outputFile string, rd io.Reader, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(outputFile), 0o750); err != nil {
		return fmt.Errorf("mkdir output dir: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d.%d", outputFile, os.Getpid(), writeAtomicSeq.Add(1))

	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if rd != nil {
		if _, err := io.Copy(f, rd); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("close temp file after copy failure: %s", closeErr.Error())
			}
			removeStaleTemp(tmpFile)
			return fmt.Errorf("copy to file: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		removeStaleTemp(tmpFile)
		return fmt.Errorf("close file: %w", err)
	}

	if err := os.Rename(tmpFile, outputFile); err != nil {
		removeStaleTemp(tmpFile)
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("mkdir output dir: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), writeAtomicSeq.Add(1))

	if err := os.WriteFile(tmpFile, data, mode); err != nil {
		removeStaleTemp(tmpFile)
		return err
	}

	if err := os.Rename(tmpFile, path); err != nil {
		removeStaleTemp(tmpFile)
		return err
	}

	return nil
}

func removeStaleTemp(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("remove stale temp file %s: %s", path, err.Error())
	}
}

func cleanRelativePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("empty path")
	}
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("absolute path is not allowed: %q", path)
	}

	cleaned := filepath.Clean(filepath.FromSlash(path))
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("invalid path: %q", path)
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes cache root: %q", path)
	}

	return filepath.ToSlash(cleaned), nil
}

func (s *Store) commitManifestPath(commit string, buildType string) (string, error) {
	commit = strings.TrimSpace(commit)
	if len(commit) > maxManifestKeyLen {
		return "", fmt.Errorf("commit too long: %d > %d", len(commit), maxManifestKeyLen)
	}
	if !validScopedKeyName.MatchString(commit) {
		return "", fmt.Errorf("invalid commit: %q", commit)
	}

	prefix := commit
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}

	scopeDir, err := manifestScopeDir(buildType)
	if err != nil {
		return "", err
	}

	return filepath.Join(s.dir, "manifests", scopeDir, prefix, commit), nil
}

func (s *Store) changesManifestPath(changesID string, buildType string) (string, error) {
	return s.scopedChangesPath(changesID, buildType, "changes")
}

func (s *Store) uploadSessionPath(uploadID string) (string, error) {
	uploadID = strings.TrimSpace(uploadID)
	if uploadID == "" {
		return "", fmt.Errorf("invalid upload-id: %q", uploadID)
	}
	if len(uploadID) > maxManifestKeyLen {
		return "", fmt.Errorf("upload-id too long: %d > %d", len(uploadID), maxManifestKeyLen)
	}
	if !validScopedKeyName.MatchString(uploadID) {
		return "", fmt.Errorf("invalid upload-id: %q", uploadID)
	}

	prefix := uploadID
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}

	return filepath.Join(s.dir, "uploads", prefix, uploadID), nil
}

func (s *Store) targetManifestPaths(req Request) ([]string, error) {
	if strings.TrimSpace(req.BuildType) != "" &&
		strings.TrimSpace(req.Commit) == "" &&
		strings.TrimSpace(req.ChangesID) == "" {
		scopeDir, err := manifestScopeDir(req.BuildType)
		if err != nil {
			return nil, err
		}

		return listManifestFiles(filepath.Join(s.dir, "manifests", scopeDir))
	}

	targets := make([]string, 0, 2)
	if strings.TrimSpace(req.Commit) != "" {
		manifestPath, err := s.commitManifestPath(req.Commit, req.BuildType)
		if err != nil {
			return nil, err
		}
		targets = append(targets, manifestPath)
	}
	if strings.TrimSpace(req.ChangesID) != "" {
		manifestPath, err := s.changesManifestPath(req.ChangesID, req.BuildType)
		if err != nil {
			return nil, err
		}
		targets = append(targets, manifestPath)
	}

	return targets, nil
}

func (s *Store) scopedChangesPath(changesID string, buildType string, kind string) (string, error) {
	changesID = strings.TrimSpace(changesID)
	if changesID == "" {
		return "", fmt.Errorf("invalid changes-id: %q", changesID)
	}
	if len(changesID) > maxManifestKeyLen {
		return "", fmt.Errorf("changes-id too long: %d > %d", len(changesID), maxManifestKeyLen)
	}

	escaped := url.QueryEscape(changesID)
	prefix := escaped
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}

	scopeDir, err := manifestScopeDir(buildType)
	if err != nil {
		return "", err
	}

	return filepath.Join(s.dir, "manifests", scopeDir, kind, prefix, escaped), nil
}

func manifestScopeDir(buildType string) (string, error) {
	buildType = strings.TrimSpace(buildType)
	if buildType == "" {
		return "default", nil
	}
	if len(buildType) > maxManifestKeyLen {
		return "", fmt.Errorf("build-type too long: %d > %d", len(buildType), maxManifestKeyLen)
	}
	if !validScopedKeyName.MatchString(buildType) {
		return "", fmt.Errorf("invalid build-type: %q", buildType)
	}
	return "buildtype-" + buildType, nil
}

func listManifestFiles(root string) ([]string, error) {
	files := make([]string, 0)

	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, err
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

func readManifestPaths(manifestPath string) ([]string, error) {
	f, err := os.Open(manifestPath) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close manifest %s: %s", manifestPath, closeErr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	res := make([]string, 0)
	seen := make(map[string]struct{})

	for scanner.Scan() {
		relPath, err := cleanRelativePath(scanner.Text())
		if err != nil {
			continue
		}
		if _, ok := seen[relPath]; ok {
			continue
		}
		seen[relPath] = struct{}{}
		res = append(res, relPath)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan manifest %s: %w", manifestPath, err)
	}

	return res, nil
}

func (s *Store) scanManifestRefs(skip map[string]struct{}) (map[string]struct{}, error) {
	manifestRoot := filepath.Join(s.dir, "manifests")
	files, err := listManifestFiles(manifestRoot)
	if err != nil {
		return nil, err
	}

	refs := make(map[string]struct{})
	for _, manifestPath := range files {
		if _, ok := skip[manifestPath]; ok {
			continue
		}
		paths, err := readManifestPaths(manifestPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, relPath := range paths {
			refs[relPath] = struct{}{}
		}
	}

	return refs, nil
}

func (s *Store) writeManifest(manifestPath string, body string) error {
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o750); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}
	if err := os.WriteFile(manifestPath+".tmp", []byte(body), 0o600); err != nil {
		return fmt.Errorf("write manifest: %w", s.handleWriteErr(err))
	}
	if err := os.Rename(manifestPath+".tmp", manifestPath); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

func (s *Store) mergeManifest(manifestPath string, paths []string) error {
	existing, _, err := s.loadManifest(manifestPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	seen := make(map[string]struct{}, len(existing)+len(paths))
	merged := make([]string, 0, len(existing)+len(paths))

	for _, relPath := range append(existing, paths...) {
		relPath = strings.TrimSpace(relPath)
		if relPath == "" {
			continue
		}
		if _, ok := seen[relPath]; ok {
			continue
		}
		seen[relPath] = struct{}{}
		merged = append(merged, relPath)
	}

	body := strings.Join(merged, "\n")
	if body != "" {
		body += "\n"
	}

	return s.writeManifest(manifestPath, body)
}

func (s *Store) indexPath() string {
	return filepath.Join(s.dir, "index.json")
}

func (s *Store) Stats() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]string{
		"hits":                  strconv.FormatInt(s.hits, 10),
		"misses":                strconv.FormatInt(s.misses, 10),
		"puts":                  strconv.FormatInt(s.puts, 10),
		"putsExist":             strconv.FormatInt(s.putsExist, 10),
		"index":                 strconv.Itoa(len(s.index)),
		"diskBytes":             strconv.FormatInt(s.currentDiskBytes, 10),
		"maxDiskBytes":          strconv.FormatInt(s.maxDiskBytes, 10),
		"evictionScheduled":     strconv.FormatBool(s.evictionScheduled),
		"lastEvictionUnixMicro": strconv.FormatInt(s.lastEvictionUnixMicro, 10),
		"errors":                strconv.FormatInt(s.errors, 10),
	}
}

func (s *Store) PrintStats() {
	st := s.Stats()

	var stats strings.Builder
	for _, k := range slices.Sorted(maps.Keys(st)) {
		_, _ = fmt.Fprintf(&stats, "%s: %s ", k, st[k])
	}

	statsText := stats.String()
	if statsText != s.prevStats {
		log.Print(statsText)
		s.prevStats = statsText
	}
}

func WriteJobStartMarker(cacheDir string, startedAt time.Time) error {
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	body := strconv.FormatInt(startedAt.UTC().UnixNano(), 10)
	return os.WriteFile(filepath.Join(cacheDir, jobStartMarkerName), []byte(body), 0o600)
}

func ReadJobStartMarker(cacheDir string) (time.Time, error) {
	data, err := os.ReadFile(filepath.Join(cacheDir, jobStartMarkerName)) //nolint:gosec // path is controlled by caller.
	if err != nil {
		return time.Time{}, err
	}

	nanos, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse job start marker: %w", err)
	}

	return time.Unix(0, nanos).UTC(), nil
}

func CollectFreshFiles(cacheDir string, maxFileSize int64) (Batch, error) {
	restoredPaths, err := ReadRestoredPaths(cacheDir)
	if err != nil && !os.IsNotExist(err) {
		return Batch{}, err
	}

	return CollectFilesToSave(cacheDir, restoredPaths, maxFileSize)
}

func CollectFilesToSave(cacheDir string, restoredPaths map[string]struct{}, maxFileSize int64) (Batch, error) {
	batch := Batch{}

	err := filepath.WalkDir(cacheDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.Name() == jobStartMarkerName || d.Name() == restoredPathsListName {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if maxFileSize > 0 && info.Size() > maxFileSize {
			return nil
		}
		relPath, err := filepath.Rel(cacheDir, path)
		if err != nil {
			return err
		}
		relPath, err = cleanRelativePath(relPath)
		if err != nil {
			return err
		}
		if _, ok := restoredPaths[relPath]; ok {
			return nil
		}

		modTime := info.ModTime().UTC()
		item := FileItem{
			Path:     relPath,
			Size:     info.Size(),
			ModTime:  &modTime,
			Mode:     uint32(info.Mode().Perm()),
			WireSize: info.Size(),
			DiskPath: path,
		}
		batch.Items = append(batch.Items, item)
		return nil
	})
	if err != nil {
		return Batch{}, err
	}

	return batch, nil
}

func RestoreToDir(cacheDir string, item FileItem, body io.Reader) error {
	relPath, err := cleanRelativePath(item.Path)
	if err != nil {
		return err
	}
	target := filepath.Join(cacheDir, filepath.FromSlash(relPath))
	mode := item.FileMode()
	if mode == 0 {
		mode = 0o600
	}
	if err := writeAtomic(target, body, mode); err != nil {
		return err
	}

	return nil
}

func WriteRestoredPaths(cacheDir string, paths []string) error {
	cleaned := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, relPath := range paths {
		relPath, err := cleanRelativePath(relPath)
		if err != nil {
			return err
		}
		if _, ok := seen[relPath]; ok {
			continue
		}
		seen[relPath] = struct{}{}
		cleaned = append(cleaned, relPath)
	}

	body := strings.Join(cleaned, "\n")
	if body != "" {
		body += "\n"
	}

	path := filepath.Join(cacheDir, restoredPathsListName)
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		return fmt.Errorf("write restored paths: %w", err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return fmt.Errorf("rename restored paths: %w", err)
	}

	return nil
}

func ReadRestoredPaths(cacheDir string) (map[string]struct{}, error) {
	path := filepath.Join(cacheDir, restoredPathsListName)
	f, err := os.Open(path) //nolint:gosec // path is controlled by caller.
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close restored paths %s: %s", path, closeErr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	res := map[string]struct{}{}
	for scanner.Scan() {
		relPath := strings.TrimSpace(scanner.Text())
		if relPath == "" {
			continue
		}
		relPath, err = cleanRelativePath(relPath)
		if err != nil {
			continue
		}
		res[relPath] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan restored paths: %w", err)
	}

	return res, nil
}
