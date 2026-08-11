package local

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/recordpool"
)

type Store struct {
	dir            string
	compress       bool
	maxDiskBytes   int64
	maxFileBytes   int64
	manifestMaxAge time.Duration
	evictionDelay  time.Duration

	mu                    sync.Mutex
	index                 map[string]indexEntry
	outputRefs            map[string]int
	outputSizes           map[string]int64
	dirty                 bool
	ready                 bool
	currentDiskBytes      int64
	evictionScheduled     bool
	lastEvictionUnixMicro int64

	// pool packs small records (see recordpool.Pool) instead of one file per output, once a
	// given size has demonstrably earned it. outputLocs is the in-memory index from OutputID to
	// where its (single, deduplicated) copy lives -- needed because storage here is
	// content-addressed by OutputID, not by the ActionID keys in index: many ActionIDs can share
	// one OutputID, and putOne must reuse that one copy's location rather than writing (and
	// potentially pool-allocating) the same content again for every ActionID that references it.
	// Rebuilt from s.index and the pool's own directory at startup (see rebuildStorageState);
	// nothing pool-related is persisted separately.
	pool         *recordpool.Pool
	outputLocs   map[string]recordpool.Loc
	poolDisabled bool

	prevStats     string
	hits          int64
	misses        int64
	puts          int64
	putsExist     int64
	putsCompleted int64
	errors        int64
}

var validScopedKeyName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

const maxManifestKeyLen = 100

// indexEntry is the metadata that Store stores on disk for an ActionID.
type indexEntry struct {
	OutputID        string `json:"o"`
	Size            int64  `json:"n"`
	TimeMicro       int64  `json:"t,omitempty"`
	AccessTimeMicro int64  `json:"a,omitempty"`
	Compressed      int64  `json:"c,omitempty"`
	WireSize        int64  `json:"w,omitempty"`

	// PoolPage == 0 means "plain file at OutputFilename(OutputID)" -- today's behavior,
	// untouched. Redundant across every ActionID entry sharing one OutputID (see outputLocs
	// above), which is harmless: it's derived, never the source of truth for freeing storage.
	PoolPage uint32 `json:"pp,omitempty"`
	PoolSlot uint32 `json:"ps,omitempty"`
}

type actionIndexEntry struct {
	actionID string
	entry    indexEntry
}

type StoreOption func(*Store)

func WithCompression() StoreOption {
	return func(s *Store) {
		s.compress = true
	}
}

// WithoutRecordPool disables record-pool packing for small entries, keeping every entry a plain
// file with a real on-disk path. Required wherever this Store answers the actual GOCACHEPROG
// protocol directly (see Proxy.Get): the go command reads Response.DiskPath itself, in a separate
// process, and there is no fallback if it's empty -- a pool-backed entry only ever offers a
// bodyReader, never a real path. Safe to omit for a Store that only ever streams response bytes
// over HTTP instead (the remote cache server), since that path doesn't need DiskPath at all.
func WithoutRecordPool() StoreOption {
	return func(s *Store) {
		s.poolDisabled = true
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

func WithEvictionDelay(evictionDelay time.Duration) StoreOption {
	return func(s *Store) {
		s.evictionDelay = evictionDelay
	}
}

// WithManifestMaxAge sets how long a manifest file may go unwritten before it's deleted by
// collectStaleManifests. Unlike cache objects, manifests are never touched again once their
// commit/PR/changes stream goes cold, so nothing else ever prunes them.
func WithManifestMaxAge(manifestMaxAge time.Duration) StoreOption {
	return func(s *Store) {
		s.manifestMaxAge = manifestMaxAge
	}
}

func NewStore(dir string, opts ...StoreOption) (*Store, error) {
	dir, err := toAbsPath(dir)
	if err != nil {
		return nil, err
	}

	dc := &Store{
		dir:            dir,
		manifestMaxAge: 5 * 24 * time.Hour,
		evictionDelay:  5 * time.Minute,
		index:          make(map[string]indexEntry),
		outputRefs:     make(map[string]int),
		outputSizes:    make(map[string]int64),
		outputLocs:     make(map[string]recordpool.Loc),
	}
	for _, opt := range opts {
		opt(dc)
	}
	dc.pool = recordpool.New(filepath.Join(dir, "records"))

	indexPath := dc.indexPath()
	d, err := os.ReadFile(indexPath) //nolint:gosec // indexPath is derived from the configured cache dir.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read %s: %w", indexPath, err)
		}
	} else if err := json.Unmarshal(d, &dc.index); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", indexPath, err)
	}
	if dc.maxFileBytes > 0 {
		for actionID, ie := range dc.index {
			if ie.Size > dc.maxFileBytes {
				delete(dc.index, actionID)
				dc.dirty = true
			}
		}
	}

	if err := dc.rebuildStorageState(); err != nil {
		return nil, err
	}
	dc.ready = true

	return dc, nil
}

func toAbsPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty path")
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
	if dc.maxFileBytes > 0 && ie.Size > dc.maxFileBytes {
		atomic.AddInt64(&dc.misses, 1)
		return cache.ResponseItem{ActionID: actionID, Miss: true}
	}

	atomic.AddInt64(&dc.hits, 1)
	ie.AccessTimeMicro = time.Now().UTC().UnixMicro()
	dc.index[actionID] = ie
	dc.dirty = true

	return dc.responseItem(actionID, ie)
}

func (dc *Store) responseItem(actionID string, ie indexEntry) cache.ResponseItem {
	res := cache.ResponseItem{ActionID: actionID}

	res.OutputID = ie.OutputID
	res.Size = ie.Size
	res.IsCompressed = ie.Compressed == 1
	res.WireSize = ie.WireSize

	if ie.PoolPage != 0 {
		res.SetBodyReader(dc.poolBodyReader(ie))
	} else {
		res.DiskPath = dc.outputPathForRead(ie.OutputID)
	}

	t := time.UnixMicro(ie.TimeMicro)
	res.Time = &t

	return res
}

// poolBodyReader returns a body reader for a pool-backed entry, for the two call sites that need
// one (responseItem for Get/Preload, verifyOutput for IntegrityCheck).
func (dc *Store) poolBodyReader(ie indexEntry) func() (io.ReadCloser, error) {
	recordSize := dc.entryStoredSize(ie)
	loc := recordpool.Loc{Page: ie.PoolPage, Slot: ie.PoolSlot}

	return func() (io.ReadCloser, error) {
		buf, err := dc.pool.Read(recordSize, loc)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(buf)), nil
	}
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
	if dc.maxFileBytes > 0 && item.Size > dc.maxFileBytes {
		return nil
	}

	now := time.Now().UTC()

	if item.OutputID == "" {
		atomic.AddInt64(&dc.errors, 1)
		println("empty output id:", fmt.Sprintf("%+v", item))
		return fmt.Errorf("empty output id: %+v", item)
	}

	dc.mu.Lock()
	existingEntry, hadExisting := dc.index[item.ActionID]
	existingOutputLoc := dc.outputLocs[item.OutputID] // zero value if this output isn't pool-backed
	_, outputRefExists := dc.outputRefs[item.OutputID]
	dc.mu.Unlock()

	if hadExisting {
		atomic.AddInt64(&dc.putsExist, 1)
	}

	atomic.AddInt64(&dc.puts, 1)

	ie := indexEntry{
		OutputID:        item.OutputID,
		Size:            item.Size,
		TimeMicro:       now.UnixMicro(),
		AccessTimeMicro: now.UnixMicro(),
	}

	rd, err := bodyReaderForPut(item, dc.compress, &ie)
	if err != nil {
		atomic.AddInt64(&dc.errors, 1)
		println("get reader for put:", err.Error())
		return fmt.Errorf("get reader for put: %w", err)
	}

	newStoredSize := dc.entryStoredSize(ie)

	loc, err := dc.storePutBody(item, rd, newStoredSize, outputRefExists, existingOutputLoc)
	if err != nil {
		atomic.AddInt64(&dc.errors, 1)
		return err
	}
	ie.PoolPage, ie.PoolSlot = loc.Page, loc.Slot

	dc.mu.Lock()
	defer dc.mu.Unlock()

	if hadExisting {
		dc.replaceOutputRefLocked(existingEntry, ie, newStoredSize, loc)
	} else {
		dc.addOutputRefLocked(ie.OutputID, newStoredSize, loc)
	}
	dc.index[item.ActionID] = ie
	dc.dirty = true
	dc.scheduleEvictionLocked()

	atomic.AddInt64(&dc.putsCompleted, 1)

	return nil
}

// bodyReaderForPut picks the right reader for item given whether the store compresses, setting
// ie.Compressed/ie.WireSize when the body ends up compressed on disk.
func bodyReaderForPut(item cache.ResponseItem, compress bool, ie *indexEntry) (io.Reader, error) {
	if item.IsCompressed {
		if !compress {
			return item.UncompressedBodyReader() // decompress a compressed body
		}
		ie.Compressed = 1
		ie.WireSize = item.WireSize
		return item.WireBodyReader() // pass compressed body as is
	}
	if compress && item.Size >= cache.MinCompressionSize {
		return compressNowBodyReader(item, ie) // enable compression if it is not there
	}
	return item.UncompressedBodyReader() // pass uncompressed body as is
}

// compressNowBodyReader compresses item's body immediately, rather than handing back a reader
// that would compress lazily on read, so ie.WireSize reflects the real compressed length by the
// time this returns. item.WireSize can't be used the way the IsCompressed branch above does:
// this item arrived uncompressed, so its WireSize field was never meaningful, and there's no way
// to know the real compressed length without actually compressing first -- pool eligibility and
// disk accounting both need that number to be trustworthy.
func compressNowBodyReader(item cache.ResponseItem, ie *indexEntry) (io.Reader, error) {
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

	return bytes.NewReader(compressed), nil
}

// storePutBody writes rd's bytes to wherever this OutputID belongs: an already-stored location
// reused as is (content-addressed, see the outputRefExists case), a pool slot if eligible and
// promoted, or a plain file otherwise.
func (dc *Store) storePutBody(item cache.ResponseItem, rd io.Reader, newStoredSize int64, outputRefExists bool, existingOutputLoc recordpool.Loc) (recordpool.Loc, error) {
	if outputRefExists {
		// Content-addressed: this OutputID is already stored somewhere, whether by an earlier
		// ActionID sharing it or a previous put of this same ActionID. Reuse that one copy's
		// location instead of writing (and, if pool-backed, allocating) the same bytes again --
		// still drain rd so its underlying body reader/upload machinery finishes cleanly, the
		// same bytes writeAtomic would otherwise have consumed.
		if rd != nil {
			if _, err := io.Copy(io.Discard, rd); err != nil {
				return recordpool.Loc{}, fmt.Errorf("drain body for already-stored output: %w", err)
			}
		}
		return existingOutputLoc, nil
	}

	if !dc.poolDisabled && newStoredSize > 0 && newStoredSize <= dc.pool.MaxRecordBytes() {
		return dc.storePoolEligibleBody(item, rd, newStoredSize)
	}

	if err := writeAtomic(dc.OutputFilename(item.OutputID), rd); err != nil {
		println("atomic write:", err.Error())
		return recordpool.Loc{}, fmt.Errorf("atomic write: %w", err)
	}
	return recordpool.Loc{}, nil
}

// storePoolEligibleBody handles a record small enough to be pool-eligible: reads its body,
// attempts Pool.Put, and falls back to a plain file (from the same already-read bytes) when this
// size hasn't crossed the promotion threshold yet.
func (dc *Store) storePoolEligibleBody(item cache.ResponseItem, rd io.Reader, newStoredSize int64) (recordpool.Loc, error) {
	body, err := io.ReadAll(rd)
	if err != nil {
		return recordpool.Loc{}, fmt.Errorf("read body for put: %w", err)
	}
	if int64(len(body)) != newStoredSize {
		return recordpool.Loc{}, fmt.Errorf("put body length %d does not match expected %d", len(body), newStoredSize)
	}

	loc, ok, poolErr := dc.pool.Put(newStoredSize, body)
	if poolErr != nil {
		return recordpool.Loc{}, fmt.Errorf("write pooled record: %w", poolErr)
	}
	if ok {
		return loc, nil
	}

	if err := writeAtomic(dc.OutputFilename(item.OutputID), bytes.NewReader(body)); err != nil {
		println("atomic write:", err.Error())
		return recordpool.Loc{}, fmt.Errorf("atomic write: %w", err)
	}
	return recordpool.Loc{}, nil
}

// writeAtomicSeq disambiguates concurrent writers' temp file names. Two Put calls for
// different ActionIDs can legitimately share an OutputID (identical build output content)
// and race to write the same outputFile; a shared ".tmp" suffix would let one writer's
// rename consume the other's temp file, failing with ENOENT.
var writeAtomicSeq atomic.Int64

func writeAtomic(outputFile string, rd io.Reader) (err error) {
	if err := os.MkdirAll(filepath.Dir(outputFile), 0o750); err != nil {
		return fmt.Errorf("mkdir output dir: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d.%d", outputFile, os.Getpid(), writeAtomicSeq.Add(1))

	f, err := os.Create(tmpFile) //nolint:gosec // outputFile is derived from the configured cache dir.
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if rd != nil {
		_, err = io.Copy(f, rd)
		if err != nil {
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

func (dc *Store) Close() error {
	poolErr := dc.pool.Close()

	if !dc.ready || !dc.dirty {
		return poolErr
	}

	d, err := json.Marshal(dc.index)
	if err != nil {
		return errors.Join(poolErr, err)
	}

	if err := writeFileAtomic(dc.indexPath(), d, 0o600); err != nil {
		return errors.Join(poolErr, err)
	}

	dc.dirty = false
	return poolErr
}

func (dc *Store) OutputFilename(outputID string) string {
	name := strings.ReplaceAll(outputID, "/", "_")
	prefix := name
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}

	return filepath.Join(dc.dir, "entries", prefix, name)
}

func (dc *Store) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	if req.MaxSize == 0 {
		req.MaxSize = 600_000
	}

	var (
		res             []cache.ResponseItem
		snapshot        []actionIndexEntry
		filterActionIDs map[string]struct{}
		err             error
	)

	if req.Commit != "" || req.ParentCommit != "" || req.ChangesID != "" || req.BaseCommit != "" {
		filterActionIDs, _, err = dc.preloadFilterActionIDs(req)
		if err != nil {
			return err
		}
	}

	dc.mu.Lock()
	snapshot = make([]actionIndexEntry, 0, len(dc.index))
	for k, v := range dc.index {
		snapshot = append(snapshot, actionIndexEntry{actionID: k, entry: v})
	}
	dc.mu.Unlock()

	for _, item := range snapshot {
		k, v := item.actionID, item.entry
		if filterActionIDs != nil {
			if _, ok := filterActionIDs[k]; !ok {
				continue
			}
		}

		wireSize := v.WireSize
		if wireSize == 0 {
			wireSize = v.Size
		}

		if wireSize > req.MaxSize {
			continue
		}

		res = append(res, dc.responseItem(k, v))
	}

	for _, item := range res {
		cb(item)
	}

	return nil
}

func (dc *Store) PreloadSources(req cache.PreloadRequest) ([]string, error) {
	_, sources, err := dc.preloadFilterActionIDs(req)
	return sources, err
}

// IntegrityCheck walks every index entry, verifying that each stored object's actual bytes
// match its recorded size (decompressing entries flagged as compressed), and reports every one
// that doesn't. Verification runs against a point-in-time snapshot of the index, taken under a
// brief lock, so the scan's disk I/O never blocks concurrent Get/Put serving. Unless dryRun is
// set, broken entries are then evicted - but only if their index entry is still exactly what was
// verified, so an entry a concurrent Put legitimately replaced while the scan was running is left
// alone.
func (dc *Store) IntegrityCheck(dryRun bool) cache.IntegrityReport {
	dc.mu.Lock()
	snapshot := make([]actionIndexEntry, 0, len(dc.index))
	for k, v := range dc.index {
		snapshot = append(snapshot, actionIndexEntry{actionID: k, entry: v})
	}
	dc.mu.Unlock()

	// Verify each distinct OutputID once: many ActionIDs can legitimately share one OutputID
	// (identical build output content), and re-reading and decompressing the same file once per
	// referencing ActionID would be wasted, disk-bound work.
	verified := make(map[string]error, len(snapshot))

	report := cache.IntegrityReport{DryRun: dryRun}

	for _, item := range snapshot {
		report.Checked++

		verifyErr, ok := verified[item.entry.OutputID]
		if !ok {
			verifyErr = dc.verifyOutput(item.entry)
			verified[item.entry.OutputID] = verifyErr
		}

		if verifyErr == nil {
			continue
		}

		entry := cache.IntegrityReportEntry{
			ActionID: item.actionID,
			OutputID: item.entry.OutputID,
			Size:     item.entry.Size,
			WireSize: item.entry.WireSize,
			Error:    verifyErr.Error(),
		}

		if !dryRun {
			entry.Removed = dc.removeIfUnchanged(item.actionID, item.entry)
		}

		report.Broken = append(report.Broken, entry)
	}

	return report
}

// verifyOutput reads and fully decompresses (if applicable) the object backing ie, confirming
// its actual byte count matches its recorded Size - the same verification a real Get response
// performs when preparing this entry's body, just without a client to hand it to.
func (dc *Store) verifyOutput(ie indexEntry) error {
	item := cache.ResponseItem{
		OutputID:     ie.OutputID,
		Size:         ie.Size,
		IsCompressed: ie.Compressed == 1,
		WireSize:     ie.WireSize,
	}

	if ie.PoolPage != 0 {
		item.SetBodyReader(dc.poolBodyReader(ie))
	} else {
		item.DiskPath = dc.outputPathForRead(ie.OutputID)
	}

	rd, err := item.UncompressedBodyReader()
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	if rd == nil {
		if ie.Size != 0 {
			return fmt.Errorf("missing body for non-empty entry (size=%d)", ie.Size)
		}

		return nil
	}
	defer func() {
		if closeErr := rd.Close(); closeErr != nil {
			log.Printf("integrity check: close body reader for output %s: %s", ie.OutputID, closeErr.Error())
		}
	}()

	n, err := io.Copy(io.Discard, rd)
	if err != nil {
		return fmt.Errorf("read/decompress: %w", err)
	}

	if n != ie.Size {
		return fmt.Errorf("size mismatch: stored object decompresses to %d bytes, index says %d", n, ie.Size)
	}

	return nil
}

// removeIfUnchanged evicts actionID's index entry only if it's still exactly what integrity
// verification saw. If a concurrent Put already replaced it with fresh content while the scan
// was running, that new entry has nothing to do with the broken one found earlier and must be
// left alone.
func (dc *Store) removeIfUnchanged(actionID string, expected indexEntry) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	current, ok := dc.index[actionID]
	if !ok || current != expected {
		return false
	}

	delete(dc.index, actionID)
	dc.releaseOutputRefLocked(expected.OutputID)
	dc.dirty = true

	log.Printf("integrity check: removed broken cache entry action_id=%s output_id=%s size=%d", actionID, expected.OutputID, expected.Size)

	return true
}

func (dc *Store) HasEntries() bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return len(dc.index) > 0
}

func (dc *Store) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error {
	if commit == "" && changesID == "" {
		return nil
	}

	if commit != "" {
		manifestPath, err := dc.commitManifestPath(commit, buildType)
		if err != nil {
			return err
		}

		if err := dc.mergeManifest(manifestPath, actionIDs); err != nil {
			return err
		}
	}

	//nolint:nestif // commit/changes manifest update rules are clearer as explicit branches.
	if changesID != "" {
		manifestPath, err := dc.changesManifestPath(changesID, buildType)
		if err != nil {
			return err
		}

		if replaceChanges {
			body := strings.Join(actionIDs, "\n")
			if body != "" {
				body += "\n"
			}
			if err := dc.writeManifest(manifestPath, body); err != nil {
				return err
			}
		} else if err := dc.mergeManifest(manifestPath, actionIDs); err != nil {
			return err
		}
	}

	return nil
}

func (dc *Store) preloadFilterActionIDs(req cache.PreloadRequest) (map[string]struct{}, []string, error) {
	if req.Commit == "" && req.ParentCommit == "" && req.ChangesID == "" && req.BaseCommit == "" {
		return nil, []string{"all"}, nil
	}

	res := map[string]struct{}{}
	sources := make([]string, 0, 4)

	for _, candidate := range []struct {
		name string
		load func() ([]string, error)
	}{
		{name: "commit", load: func() ([]string, error) { return dc.loadCommitManifest(req.Commit, req.BuildType) }},
		{name: "parent", load: func() ([]string, error) { return dc.loadCommitManifest(req.ParentCommit, req.BuildType) }},
		{name: "changes", load: func() ([]string, error) { return dc.loadChangesManifest(req.ChangesID, req.BuildType) }},
		{name: "base", load: func() ([]string, error) { return dc.loadCommitManifest(req.BaseCommit, req.BuildType) }},
	} {
		actionIDs, err := candidate.load()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return nil, nil, err
		}

		sources = append(sources, candidate.name)
		for _, actionID := range actionIDs {
			res[actionID] = struct{}{}
		}
	}

	if len(sources) == 0 {
		actionIDs, err := dc.loadNewestManifest(req.BuildType)
		if err != nil {
			return nil, nil, err
		}

		if len(actionIDs) > 0 {
			sources = []string{"newest"}
			for _, actionID := range actionIDs {
				res[actionID] = struct{}{}
			}
		} else {
			sources = []string{"none"}
		}
	}

	return res, sources, nil
}

// loadNewestManifest is the last-resort preload source: when no commit/parent/changes/base
// manifest matched - a long pause with nothing relevant built on master since, or the very
// first build of a new build type - it falls back to whichever manifest for this build type was
// written most recently, from any commit or PR. In practice, cache entries are usually still
// largely relevant across unrelated PRs of the same build type, so this beats every PR starting
// completely cold until master catches up.
func (dc *Store) loadNewestManifest(buildType string) ([]string, error) {
	scopeDir, err := dc.manifestScopeDir(buildType)
	if err != nil {
		return nil, err
	}

	scopeRoot := filepath.Join(dc.dir, "manifests", scopeDir)

	var (
		newestPath string
		newestTime time.Time
	)

	err = filepath.WalkDir(scopeRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}

			return walkErr
		}

		if d.IsDir() || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newestPath = path
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan manifest dir %s: %w", scopeRoot, err)
	}

	if newestPath == "" {
		return nil, nil
	}

	return dc.loadManifest(newestPath, filepath.Base(newestPath))
}

func (dc *Store) loadCommitManifest(commit string, buildType string) ([]string, error) {
	if strings.TrimSpace(commit) == "" {
		return nil, os.ErrNotExist
	}

	manifestPath, err := dc.commitManifestPath(commit, buildType)
	if err != nil {
		return nil, err
	}

	return dc.loadManifest(manifestPath, commit)
}

func (dc *Store) loadChangesManifest(changesID string, buildType string) ([]string, error) {
	if strings.TrimSpace(changesID) == "" {
		return nil, os.ErrNotExist
	}

	manifestPath, err := dc.changesManifestPath(changesID, buildType)
	if err != nil {
		return nil, err
	}

	return dc.loadManifest(manifestPath, changesID)
}

func (dc *Store) loadManifest(manifestPath string, name string) ([]string, error) {
	f, err := os.Open(manifestPath) //nolint:gosec // manifestPath is derived from the configured cache dir.
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close manifest file %s: %s", manifestPath, closeErr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	res := make([]string, 0)
	for scanner.Scan() {
		actionID := strings.TrimSpace(scanner.Text())
		if actionID == "" {
			continue
		}

		res = append(res, actionID)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan manifest %s: %w", name, err)
	}

	return res, nil
}

func (dc *Store) commitManifestPath(commit string, buildType string) (string, error) {
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

	scopeDir, err := dc.manifestScopeDir(buildType)
	if err != nil {
		return "", err
	}

	return filepath.Join(dc.dir, "manifests", scopeDir, prefix, commit), nil
}

func (dc *Store) changesManifestPath(changesID string, buildType string) (string, error) {
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

	scopeDir, err := dc.manifestScopeDir(buildType)
	if err != nil {
		return "", err
	}

	return filepath.Join(dc.dir, "manifests", scopeDir, "changes", prefix, escaped), nil
}

func (dc *Store) manifestScopeDir(buildType string) (string, error) {
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

func (dc *Store) indexPath() string {
	return filepath.Join(dc.dir, "index.json")
}

func (dc *Store) legacyOutputFilename(outputID string) string {
	return filepath.Join(dc.dir, strings.ReplaceAll(outputID, "/", "_"))
}

func (dc *Store) outputPathForRead(outputID string) string {
	path := dc.OutputFilename(outputID)
	if _, err := os.Stat(path); err == nil {
		return path
	}

	legacy := dc.legacyOutputFilename(outputID)
	if _, err := os.Stat(legacy); err == nil {
		return legacy
	}

	return path
}

// rebuildStorageState discovers existing pool pages, then makes one pass over the already-loaded
// index to rebuild outputRefs/outputSizes/outputLocs and the pool's own free lists/promotion
// counters. An entry whose PoolPage/PoolSlot no longer resolves to anything on disk is dropped --
// the same treatment a plain file that's gone missing already gets, just enforced rather than
// silently trusted (see outputFileSize), since there's no size to fall back to for a dangling
// pool location the way there is for a plain file.
func (dc *Store) rebuildStorageState() error {
	if err := dc.pool.DiscoverPages(); err != nil {
		return err
	}

	seen := map[string]struct{}{}
	for actionID, ie := range dc.index {
		size := dc.entryStoredSize(ie)
		loc := recordpool.Loc{Page: ie.PoolPage, Slot: ie.PoolSlot}

		if !dc.pool.Valid(size, loc) {
			delete(dc.index, actionID)
			dc.dirty = true
			continue
		}

		dc.outputRefs[ie.OutputID]++
		if _, ok := seen[ie.OutputID]; ok {
			continue
		}
		seen[ie.OutputID] = struct{}{}

		if loc.IsZero() {
			dc.pool.NoteUnpromoted(size)

			fileSize, err := dc.outputFileSize(ie)
			if err != nil {
				return err
			}
			dc.outputSizes[ie.OutputID] = fileSize
			dc.currentDiskBytes += fileSize
		} else {
			dc.pool.NoteOccupied(size, loc)
			dc.outputLocs[ie.OutputID] = loc
			dc.outputSizes[ie.OutputID] = size
			dc.currentDiskBytes += size
		}
	}

	dc.pool.FinishReconcile()

	return nil
}

func (dc *Store) outputFileSize(ie indexEntry) (int64, error) {
	path := dc.outputPathForRead(ie.OutputID)
	fi, err := os.Stat(path)
	if err == nil {
		return fi.Size(), nil
	}
	if !os.IsNotExist(err) {
		return 0, fmt.Errorf("stat output %s: %w", ie.OutputID, err)
	}

	return dc.entryStoredSize(ie), nil
}

func (dc *Store) entryStoredSize(ie indexEntry) int64 {
	if ie.WireSize > 0 {
		return ie.WireSize
	}
	return ie.Size
}

func (dc *Store) addOutputRefLocked(outputID string, size int64, loc recordpool.Loc) {
	if dc.outputRefs[outputID] == 0 {
		dc.outputSizes[outputID] = size
		dc.currentDiskBytes += size
		if !loc.IsZero() {
			dc.outputLocs[outputID] = loc
		}
	}
	dc.outputRefs[outputID]++
}

func (dc *Store) replaceOutputRefLocked(oldEntry indexEntry, newEntry indexEntry, newStoredSize int64, loc recordpool.Loc) {
	if oldEntry.OutputID == newEntry.OutputID {
		// Content-addressed: an unchanged OutputID means unchanged bytes, so the location (if
		// any) can't have changed either -- only the size bookkeeping might need a touch-up.
		oldSize := dc.outputSizes[oldEntry.OutputID]
		dc.outputSizes[oldEntry.OutputID] = newStoredSize
		dc.currentDiskBytes += newStoredSize - oldSize
		return
	}

	dc.releaseOutputRefLocked(oldEntry.OutputID)
	dc.addOutputRefLocked(newEntry.OutputID, newStoredSize, loc)
}

func (dc *Store) releaseOutputRefLocked(outputID string) {
	refCount := dc.outputRefs[outputID]
	if refCount <= 1 {
		delete(dc.outputRefs, outputID)
		size := dc.outputSizes[outputID]
		dc.currentDiskBytes -= size
		delete(dc.outputSizes, outputID)

		if loc, ok := dc.outputLocs[outputID]; ok {
			dc.pool.Free(size, loc)
			delete(dc.outputLocs, outputID)
			return
		}

		path := dc.outputPathForRead(outputID)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("remove output %s: %v", outputID, err)
		}
		return
	}

	dc.outputRefs[outputID] = refCount - 1
}

func (dc *Store) scheduleEvictionLocked() {
	if dc.evictionScheduled {
		return
	}
	if dc.manifestMaxAge <= 0 && (dc.maxDiskBytes <= 0 || dc.currentDiskBytes <= dc.maxDiskBytes) {
		return
	}

	dc.evictionScheduled = true
	delay := dc.evictionDelay
	go func() {
		time.Sleep(delay)

		dc.mu.Lock()
		dc.evictionScheduled = false
		dc.evictIfNeededLocked()
		dc.mu.Unlock()

		// Pure filesystem work over dir/manifests, unrelated to the in-memory index/
		// currentDiskBytes that evictIfNeededLocked guards, so it runs without dc.mu held.
		dc.collectStaleManifests()
	}()
}

// collectStaleManifests deletes manifest files under dir/manifests whose mtime is older than
// manifestMaxAge. See gocache.Store.collectStaleManifests for why nothing else ever prunes them.
func (dc *Store) collectStaleManifests() {
	if dc.manifestMaxAge <= 0 {
		return
	}

	files, err := listManifestFiles(filepath.Join(dc.dir, "manifests"))
	if err != nil {
		log.Printf("collect stale manifests: list %s: %s", dc.dir, err.Error())
		return
	}

	cutoff := time.Now().UTC().Add(-dc.manifestMaxAge)
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
		log.Printf("collect stale manifests: removed %d manifest(s) older than %s", removed, humanDuration(dc.manifestMaxAge))
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

func listManifestFiles(root string) ([]string, error) {
	var files []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || strings.HasSuffix(d.Name(), ".tmp") {
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

// EvictNow forces an eviction pass immediately, bypassing evictionDelay.
func (dc *Store) EvictNow() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.evictIfNeededLocked()
}

func (dc *Store) evictIfNeededLocked() {
	if dc.maxDiskBytes <= 0 {
		return
	}

	for dc.currentDiskBytes > dc.maxDiskBytes && dc.evictOneLocked() {
	}
}

// evictOneLocked removes the single least-recently-used cache object regardless of maxDiskBytes
// and reports whether one was evicted (false once the index is empty). Shared by the
// budget-based loop above and an external combined-budget enforcer (EvictOne).
func (dc *Store) evictOneLocked() bool {
	var (
		evictActionID string
		evictEntry    indexEntry
		found         bool
	)

	for actionID, ie := range dc.index {
		if !found || lruTimeMicro(ie) < lruTimeMicro(evictEntry) {
			evictActionID = actionID
			evictEntry = ie
			found = true
		}
	}
	if !found {
		return false
	}

	delete(dc.index, evictActionID)
	dc.releaseOutputRefLocked(evictEntry.OutputID)
	dc.lastEvictionUnixMicro = time.Now().UTC().UnixMicro()
	dc.dirty = true
	log.Printf("evicted cache entry action_id=%s output_id=%s current_disk_bytes=%d max_disk_bytes=%d", evictActionID, evictEntry.OutputID, dc.currentDiskBytes, dc.maxDiskBytes)

	return true
}

// DiskBytes reports current tracked on-disk usage, for an external combined-budget enforcer.
func (dc *Store) DiskBytes() int64 {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return dc.currentDiskBytes
}

// EvictOne evicts a single least-recently-used entry regardless of maxDiskBytes, for use by an
// external combined-budget enforcer spanning multiple stores. Reports whether anything was
// evicted.
func (dc *Store) EvictOne() bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return dc.evictOneLocked()
}

func lruTimeMicro(ie indexEntry) int64 {
	if ie.AccessTimeMicro != 0 {
		return ie.AccessTimeMicro
	}
	return ie.TimeMicro
}

func (dc *Store) writeManifest(manifestPath string, body string) error {
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o750); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}

	if err := os.WriteFile(manifestPath+".tmp", []byte(body), 0o600); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	if err := os.Rename(manifestPath+".tmp", manifestPath); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

func (dc *Store) mergeManifest(manifestPath string, actionIDs []string) error {
	existing, err := dc.loadManifest(manifestPath, manifestPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	seen := make(map[string]struct{}, len(existing)+len(actionIDs))
	merged := make([]string, 0, len(existing)+len(actionIDs))

	for _, actionID := range existing {
		actionID = strings.TrimSpace(actionID)
		if actionID == "" {
			continue
		}
		if _, ok := seen[actionID]; ok {
			continue
		}
		seen[actionID] = struct{}{}
		merged = append(merged, actionID)
	}

	for _, actionID := range actionIDs {
		actionID = strings.TrimSpace(actionID)
		if actionID == "" {
			continue
		}
		if _, ok := seen[actionID]; ok {
			continue
		}
		seen[actionID] = struct{}{}
		merged = append(merged, actionID)
	}

	body := strings.Join(merged, "\n")
	if body != "" {
		body += "\n"
	}

	return dc.writeManifest(manifestPath, body)
}

func (dc *Store) Stats() map[string]string {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return map[string]string{
		"hits":                  strconv.FormatInt(dc.hits, 10),
		"misses":                strconv.FormatInt(dc.misses, 10),
		"puts":                  strconv.FormatInt(dc.puts, 10),
		"putsExist":             strconv.FormatInt(dc.putsExist, 10),
		"putsCompleted":         strconv.FormatInt(dc.putsCompleted, 10),
		"index":                 strconv.Itoa(len(dc.index)),
		"diskBytes":             strconv.FormatInt(dc.currentDiskBytes, 10),
		"maxDiskBytes":          strconv.FormatInt(dc.maxDiskBytes, 10),
		"uniqueOutputFiles":     strconv.Itoa(len(dc.outputRefs)),
		"evictionScheduled":     strconv.FormatBool(dc.evictionScheduled),
		"lastEvictionUnixMicro": strconv.FormatInt(dc.lastEvictionUnixMicro, 10),
		"errors":                strconv.FormatInt(dc.errors, 10),
	}
}

func (dc *Store) PrintStats() {
	st := dc.Stats()

	var stats strings.Builder
	for _, k := range slices.Sorted(maps.Keys(st)) {
		v := st[k]

		_, _ = fmt.Fprintf(&stats, "%s: %s ", k, v)
	}

	statsText := stats.String()
	if statsText != dc.prevStats {
		log.Print(statsText)
		dc.prevStats = statsText
	}
}
