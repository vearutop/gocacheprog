// Package recordpool packs many small, same-size records into a handful of shared, fixed-length
// page files instead of one file per record -- a plain file wastes up to one filesystem block
// (typically 4096 bytes) regardless of how small its content is, and that waste dominates real
// disk usage once there are enough of them.
//
// A size only ever gets its own pool once it demonstrably needs one: Put tracks how many
// same-size records are still stored elsewhere (NoteUnpromoted during startup reconciliation,
// or organically as further records of that size arrive) and only creates the size's first page
// once that count crosses the point where a page costs less than the block-rounding waste it
// replaces (see breakeven in pool.go). Below that, callers keep storing the record however they
// already do -- Put returning ok=false is the signal to fall back to that existing path.
//
// Reconciliation is caller-driven and storage-agnostic on purpose: this package knows nothing
// about any particular store's index format. A caller reconciles once at startup by calling
// DiscoverPages, then feeding every surviving index entry through Valid (to detect an entry whose
// location no longer resolves to a real page/slot -- the caller decides what "surviving" and
// "drop this entry" mean for its own index) and NoteOccupied/NoteUnpromoted, then FinishReconcile
// before any Put/Read/Free. Nothing here is persisted separately: free lists and promotion
// counts are rebuilt from whatever the caller's own index says on every reconcile.
package recordpool

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

// Loc identifies a record's slot within its size's pool. The zero value means "not pool-backed" --
// callers use it as the sentinel for "store this however you normally would".
type Loc struct {
	Page uint32
	Slot uint32
}

// IsZero reports whether loc is the zero value, i.e. not pool-backed.
func (loc Loc) IsZero() bool {
	return loc.Page == 0
}

// DefaultMaxRecordBytes bounds which record sizes are ever pool-eligible. Matches the filesystem
// block size this package exists to stop wasting: a file smaller than a block wastes the same
// amount no matter how small it is, so there's nothing to gain pooling anything larger.
const DefaultMaxRecordBytes = 4096

// DefaultPageBytes is the eager, fully-written size of one page file (see createPageLocked).
// Chosen to be sub-second to write in full, so paying it once per page isn't worth optimizing
// away.
const DefaultPageBytes = 8 << 20 // 8MB

// Pool manages every size class under one directory.
type Pool struct {
	dir            string
	maxRecordBytes int64
	pageBytes      int64
	breakeven      int
	writeSeq       uint64

	mu      sync.Mutex
	classes map[int64]*class
	counts  map[int64]int // unpromoted record counts, by size
}

// Option configures a Pool constructed by New.
type Option func(*Pool)

// WithMaxRecordBytes overrides DefaultMaxRecordBytes.
func WithMaxRecordBytes(n int64) Option {
	return func(p *Pool) { p.maxRecordBytes = n }
}

// WithPageBytes overrides DefaultPageBytes.
func WithPageBytes(n int64) Option {
	return func(p *Pool) { p.pageBytes = n }
}

// New returns a Pool rooted at dir (created lazily on first page write). Call DiscoverPages,
// then reconcile the caller's index (Valid/NoteOccupied/NoteUnpromoted), then FinishReconcile,
// before any Put/Read/Free.
func New(dir string, opts ...Option) *Pool {
	p := &Pool{
		dir:            dir,
		maxRecordBytes: DefaultMaxRecordBytes,
		pageBytes:      DefaultPageBytes,
		classes:        make(map[int64]*class),
		counts:         make(map[int64]int),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.breakeven = int(p.pageBytes / 4096)

	return p
}

// MaxRecordBytes reports the configured eligibility ceiling.
func (p *Pool) MaxRecordBytes() int64 {
	return p.maxRecordBytes
}

// Breakeven reports the minimum same-size record count a caller must accumulate elsewhere before
// Put will promote that size into its own class (see package docs).
func (p *Pool) Breakeven() int {
	return p.breakeven
}

// PageFileName returns the on-disk file name for one page of a size class, exported so callers
// (including tests) can locate a specific page without reaching into package internals.
func PageFileName(recordSize int64, page uint32) string {
	return pageFileName(recordSize, page)
}

// class is one exact record size's set of pages. Promotion is one-directional: once created, a
// class is never un-promoted, but it can end up with zero pages (see Free) if every record it
// ever held gets evicted -- at that point it's indistinguishable from never having been promoted,
// and NoteUnpromoted/Put's own counting picks back up if that size becomes common again.
type class struct {
	recordSize int64
	capacity   uint32 // records per page
	pages      map[uint32]*page
	nextPage   uint32
}

// page is one fixed-length, fully-preallocated file holding up to capacity records of
// recordSize bytes each, packed back to back with no header or length prefix -- offset is always
// slot*recordSize.
type page struct {
	file *os.File
	free []uint32 // stack of free slot indices
}

var pageNameRe = regexp.MustCompile(`^records\.(\d+)\.p(\d+)\.bin$`)

func pageFileName(recordSize int64, pageNum uint32) string {
	return fmt.Sprintf("records.%d.p%d.bin", recordSize, pageNum)
}

// DiscoverPages opens every existing records.<size>.p<N>.bin file under dir, grouping them into
// classes. Call once, before any reconcile/Valid/NoteOccupied/NoteUnpromoted/FinishReconcile call.
func (p *Pool) DiscoverPages() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, d := range entries {
		if d.IsDir() {
			continue
		}

		m := pageNameRe.FindStringSubmatch(d.Name())
		if m == nil {
			continue
		}

		recordSize, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || recordSize <= 0 {
			continue
		}
		pageNum64, err := strconv.ParseUint(m[2], 10, 32)
		if err != nil {
			continue
		}
		pageNum := uint32(pageNum64)

		f, err := os.OpenFile(filepath.Join(p.dir, d.Name()), os.O_RDWR, 0o600) //nolint:gosec // path is derived from configured storage dir.
		if err != nil {
			return fmt.Errorf("open pool page %s: %w", d.Name(), err)
		}

		c := p.classes[recordSize]
		if c == nil {
			c = &class{
				recordSize: recordSize,
				capacity:   uint32(p.pageBytes / recordSize),
				pages:      make(map[uint32]*page),
			}
			p.classes[recordSize] = c
		}
		c.pages[pageNum] = &page{file: f}
		if pageNum > c.nextPage {
			c.nextPage = pageNum
		}
	}

	return nil
}

// Valid reports whether loc resolves to a real page and slot for a record of size bytes,
// discovered by DiscoverPages. Callers check this before deciding an index entry survives
// reconciliation -- an entry whose loc doesn't resolve should be dropped from the caller's own
// index, the same way a plain file that's gone missing already would be.
func (p *Pool) Valid(size int64, loc Loc) bool {
	if loc.IsZero() {
		return true // not pool-backed; nothing for this package to validate.
	}

	c := p.classes[size]
	if c == nil {
		return false
	}
	pg := c.pages[loc.Page]

	return pg != nil && loc.Slot < c.capacity
}

// NoteOccupied marks loc as referenced. Call once per surviving pool-backed index entry, after
// DiscoverPages and before FinishReconcile.
func (p *Pool) NoteOccupied(size int64, loc Loc) {
	if loc.IsZero() {
		return
	}
	c := p.classes[size]
	if c == nil {
		return
	}
	pg := c.pages[loc.Page]
	if pg == nil {
		return
	}
	// free is repurposed as a scratch occupied-set until FinishReconcile inverts it; see there.
	pg.free = append(pg.free, loc.Slot)
}

// NoteUnpromoted seeds the promotion counter for a size that currently has no pool class, i.e.
// is still stored some other way. Call once per surviving non-pool-backed index entry of that
// size, deduped however "one record" means for the caller's storage model (e.g. once per unique
// content-addressed blob, not once per reference to it, if references can share a blob).
func (p *Pool) NoteUnpromoted(size int64) {
	if _, promoted := p.classes[size]; promoted {
		return
	}
	p.counts[size]++
}

// FinishReconcile derives each discovered page's free list from what NoteOccupied reported.
// Call once, after DiscoverPages and all Valid/NoteOccupied/NoteUnpromoted calls, before any
// Put/Read/Free.
func (p *Pool) FinishReconcile() {
	for _, c := range p.classes {
		for _, pg := range c.pages {
			occupied := make(map[uint32]struct{}, len(pg.free))
			for _, slot := range pg.free {
				occupied[slot] = struct{}{}
			}

			pg.free = pg.free[:0]
			for slot := uint32(0); slot < c.capacity; slot++ {
				if _, used := occupied[slot]; !used {
					pg.free = append(pg.free, slot)
				}
			}
		}
	}
}

// Put stores body (exactly size bytes) in a fresh slot, promoting size to its own class the
// moment its unpromoted count crosses breakeven (see package docs). ok is false when size isn't
// (yet) promoted, in which case the caller should store body however it did before this package
// existed.
func (p *Pool) Put(size int64, body []byte) (loc Loc, ok bool, err error) {
	if size <= 0 || size > p.maxRecordBytes || int64(len(body)) != size {
		return Loc{}, false, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	c := p.classes[size]
	if c == nil {
		p.counts[size]++
		if p.counts[size] < p.breakeven {
			return Loc{}, false, nil
		}

		c = &class{
			recordSize: size,
			capacity:   uint32(p.pageBytes / size),
			pages:      make(map[uint32]*page),
		}
		p.classes[size] = c
		delete(p.counts, size)
	}

	page, slot, err := p.allocateLocked(c)
	if err != nil {
		return Loc{}, false, err
	}

	pg := c.pages[page]
	if _, err := pg.file.WriteAt(body, int64(slot)*size); err != nil {
		p.freeLocked(c, page, slot)
		return Loc{}, false, err
	}

	return Loc{Page: page, Slot: slot}, true, nil
}

// Read reads a record's bytes back.
func (p *Pool) Read(size int64, loc Loc) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	c := p.classes[size]
	if c == nil {
		return nil, fmt.Errorf("recordpool: unknown class for size %d", size)
	}
	pg := c.pages[loc.Page]
	if pg == nil {
		return nil, fmt.Errorf("recordpool: unknown page %d for size %d", loc.Page, size)
	}

	buf := make([]byte, size)
	if _, err := pg.file.ReadAt(buf, int64(loc.Slot)*size); err != nil {
		return nil, err
	}

	return buf, nil
}

// Free releases a slot back to its page's free list. When that empties the page completely, the
// whole file is deleted rather than left around hollow -- and once a class has no pages left,
// it's indistinguishable from never having been promoted (see class docs).
func (p *Pool) Free(size int64, loc Loc) {
	p.mu.Lock()
	defer p.mu.Unlock()

	c := p.classes[size]
	if c == nil {
		return
	}
	p.freeLocked(c, loc.Page, loc.Slot)
}

// Close closes every open page file handle. Safe to call once, when the caller itself is done.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for _, c := range p.classes {
		for _, pg := range c.pages {
			if err := pg.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// allocateLocked picks the first free slot in the oldest page that has one, so pages stay dense
// as records churn instead of always growing the newest page while earlier ones go sparse. Only
// creates a new page once every existing page for this class is full.
func (p *Pool) allocateLocked(c *class) (pageNum uint32, slot uint32, err error) {
	for pn := uint32(1); pn <= c.nextPage; pn++ {
		pg := c.pages[pn]
		if pg == nil || len(pg.free) == 0 {
			continue
		}

		slot = pg.free[len(pg.free)-1]
		pg.free = pg.free[:len(pg.free)-1]

		return pn, slot, nil
	}

	c.nextPage++

	pg, err := p.createPageLocked(c, c.nextPage)
	if err != nil {
		c.nextPage--
		return 0, 0, err
	}

	c.pages[c.nextPage] = pg
	slot = pg.free[len(pg.free)-1]
	pg.free = pg.free[:len(pg.free)-1]

	return c.nextPage, slot, nil
}

// createPageLocked eagerly writes a full, zero-filled page as tmp-then-rename, so a page file
// only ever exists at its real name once fully sized. That fixed length is what lets every other
// pool operation use pure offset math (slot*recordSize) with no length prefix or free-list
// durability of its own: valid slots are always exactly [0, capacity), known from the page's
// length alone.
func (p *Pool) createPageLocked(c *class, pageNum uint32) (*page, error) {
	if err := os.MkdirAll(p.dir, 0o750); err != nil {
		return nil, fmt.Errorf("create pool dir: %w", err)
	}

	path := filepath.Join(p.dir, pageFileName(c.recordSize, pageNum))
	p.writeSeq++
	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", path, os.Getpid(), p.writeSeq)

	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return nil, fmt.Errorf("create pool page: %w", err)
	}

	if err := writeZeroes(f, int64(c.capacity)*c.recordSize); err != nil {
		_ = f.Close()
		removeStaleTemp(tmpPath)
		return nil, fmt.Errorf("write pool page: %w", err)
	}
	if err := f.Close(); err != nil {
		removeStaleTemp(tmpPath)
		return nil, fmt.Errorf("close pool page: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		removeStaleTemp(tmpPath)
		return nil, fmt.Errorf("rename pool page: %w", err)
	}

	f, err = os.OpenFile(path, os.O_RDWR, 0o600) //nolint:gosec // path is derived from configured storage dir.
	if err != nil {
		return nil, fmt.Errorf("reopen pool page: %w", err)
	}

	free := make([]uint32, c.capacity)
	for i := range free {
		free[i] = uint32(i)
	}

	return &page{file: f, free: free}, nil
}

func writeZeroes(f *os.File, n int64) error {
	buf := make([]byte, min(n, 1<<20))
	for written := int64(0); written < n; {
		chunk := buf
		if remaining := n - written; remaining < int64(len(chunk)) {
			chunk = chunk[:remaining]
		}
		wrote, err := f.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(wrote)
	}
	return nil
}

func removeStaleTemp(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("recordpool: remove stale temp file %s: %s", path, err.Error())
	}
}

func (p *Pool) freeLocked(c *class, pageNum uint32, slot uint32) {
	pg := c.pages[pageNum]
	if pg == nil {
		return
	}

	pg.free = append(pg.free, slot)
	if uint32(len(pg.free)) < c.capacity {
		return
	}

	path := filepath.Join(p.dir, pageFileName(c.recordSize, pageNum))
	if err := pg.file.Close(); err != nil {
		log.Printf("recordpool: close empty page %s: %s", path, err.Error())
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("recordpool: remove empty page %s: %s", path, err.Error())
	}
	delete(c.pages, pageNum)
}
