package recordpool

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestPool(t *testing.T) (*Pool, string) {
	t.Helper()

	dir := t.TempDir()
	p := New(dir, WithPageBytes(64<<10)) // 64KB pages -> breakeven=16, small enough to test fast.
	require.NoError(t, p.DiscoverPages())
	p.FinishReconcile()

	return p, dir
}

func TestPool_StaysUnpromotedBelowBreakeven(t *testing.T) {
	p, dir := newTestPool(t)

	for i := 0; i < p.breakeven-1; i++ {
		body := []byte(fmt.Sprintf("%050d", i))
		loc, ok, err := p.Put(int64(len(body)), body)
		require.NoError(t, err)
		require.False(t, ok, "record %d should still be below breakeven", i)
		require.True(t, loc.IsZero())
	}

	require.NoFileExists(t, filepath.Join(dir, pageFileName(50, 1)))
}

func TestPool_PromotesAtBreakevenAndRoundTrips(t *testing.T) {
	p, dir := newTestPool(t)

	body := []byte(fmt.Sprintf("%050d", 0))
	for i := 0; i < p.breakeven-1; i++ {
		_, ok, err := p.Put(int64(len(body)), body)
		require.NoError(t, err)
		require.False(t, ok)
	}

	promoted := []byte(fmt.Sprintf("%050d", 999))
	loc, ok, err := p.Put(int64(len(promoted)), promoted)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, loc.IsZero())
	require.FileExists(t, filepath.Join(dir, pageFileName(50, 1)))

	got, err := p.Read(int64(len(promoted)), loc)
	require.NoError(t, err)
	require.Equal(t, promoted, got)
}

// crossBreakeven puts breakeven-1 throwaway plain-file records of size (none of them pool-backed,
// per the package's own promotion rule), leaving the pool one put away from creating its first
// page for size.
func crossBreakeven(t *testing.T, p *Pool, size int64, body []byte) {
	t.Helper()

	for i := 0; i < p.breakeven-1; i++ {
		_, ok, err := p.Put(size, body)
		require.NoError(t, err)
		require.False(t, ok, "record %d should still be below breakeven", i)
	}
}

func TestPool_AllocatesFirstFitAcrossPagesBeforeNewOne(t *testing.T) {
	dir := t.TempDir()
	size := int64(50)
	body := make([]byte, size)

	// Tiny page (capacity 4 records) makes filling a page and forcing a second one cheap; the
	// resulting breakeven of 0 means every put of this size is pool-backed from the first call.
	p := New(dir, WithPageBytes(4*size))
	require.NoError(t, p.DiscoverPages())
	p.FinishReconcile()

	locs := make([]Loc, 0, 4)
	for i := 0; i < 4; i++ {
		loc, ok, err := p.Put(size, body)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint32(1), loc.Page, "page 1 has room for all 4")
		locs = append(locs, loc)
	}

	// Page 1 is now full; the next put must create page 2.
	overflow, ok, err := p.Put(size, body)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), overflow.Page)

	// Free two slots in page 1: the next puts must reuse page 1 (first-fit oldest page) rather
	// than page 2, which still has room too.
	p.Free(size, locs[0])
	p.Free(size, locs[1])

	reused1, _, err := p.Put(size, body)
	require.NoError(t, err)
	require.Equal(t, uint32(1), reused1.Page)

	reused2, _, err := p.Put(size, body)
	require.NoError(t, err)
	require.Equal(t, uint32(1), reused2.Page)
}

func TestPool_FreeDeletesPageOnceFullyEmptied(t *testing.T) {
	p, dir := newTestPool(t)

	size := int64(50)
	body := make([]byte, size)
	pagePath := filepath.Join(dir, pageFileName(size, 1))
	crossBreakeven(t, p, size, body)

	const pooled = 5
	locs := make([]Loc, 0, pooled)
	for i := 0; i < pooled; i++ {
		loc, ok, err := p.Put(size, body)
		require.NoError(t, err)
		require.True(t, ok)
		locs = append(locs, loc)
	}
	require.FileExists(t, pagePath)

	for _, loc := range locs {
		p.Free(size, loc)
	}
	require.NoFileExists(t, pagePath)
}

func TestPool_ReconcileSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	size := int64(50)
	body := make([]byte, size)

	p1 := New(dir, WithPageBytes(64<<10))
	require.NoError(t, p1.DiscoverPages())
	p1.FinishReconcile()
	crossBreakeven(t, p1, size, body)

	kept, ok, err := p1.Put(size, body)
	require.NoError(t, err)
	require.True(t, ok)

	freedBeforeRestart, ok, err := p1.Put(size, body)
	require.NoError(t, err)
	require.True(t, ok)

	p1.Free(size, freedBeforeRestart) // simulate an eviction the caller recorded before "restart".
	require.NoError(t, p1.Close())

	// "Restart": a fresh Pool over the same dir, reconciled against only the surviving entry
	// (kept) -- everything else, including the freed slot, must come back as free/available.
	p2 := New(dir, WithPageBytes(64<<10))
	require.NoError(t, p2.DiscoverPages())
	require.True(t, p2.Valid(size, kept))
	p2.NoteOccupied(size, kept)
	p2.FinishReconcile()

	got, err := p2.Read(size, kept)
	require.NoError(t, err)
	require.Equal(t, body, got)

	// The freed-before-restart slot, plus every never-used slot, must be allocatable again.
	class := p2.classes[size]
	require.NotNil(t, class)
	require.Equal(t, class.capacity-1, uint32(len(class.pages[1].free)))
}

func TestPool_ValidRejectsDanglingLocation(t *testing.T) {
	dir := t.TempDir()

	p := New(dir, WithPageBytes(64<<10))
	require.NoError(t, p.DiscoverPages())
	p.FinishReconcile()

	require.True(t, p.Valid(50, Loc{})) // zero Loc is always valid: "not pool-backed".
	require.False(t, p.Valid(50, Loc{Page: 1, Slot: 0}), "no class exists yet for size 50")

	size := int64(50)
	body := make([]byte, size)
	for i := 0; i < p.breakeven; i++ {
		_, _, err := p.Put(size, body)
		require.NoError(t, err)
	}
	require.False(t, p.Valid(size, Loc{Page: 2, Slot: 0}), "page 2 was never created")
	require.False(t, p.Valid(size, Loc{Page: 1, Slot: p.classes[size].capacity}), "slot is out of range")
}

// TestPool_ConcurrentPutNeverDoubleAllocatesASlot guards logic, not just data-race-freedom: Put's
// mutex already rules out unsynchronized memory access, but it can't by itself prove the
// allocator never hands the same slot to two callers. A tiny page (capacity 20) forces many
// concurrent page-creation races within a short run, and a page size this small makes breakeven 0,
// so every one of these puts is pool-backed from the very first call -- no throwaway warm-up puts
// needed to reach that state under concurrency.
func TestPool_ConcurrentPutNeverDoubleAllocatesASlot(t *testing.T) {
	dir := t.TempDir()
	size := int64(50)

	p := New(dir, WithPageBytes(20*size))
	require.NoError(t, p.DiscoverPages())
	p.FinishReconcile()
	require.Equal(t, 0, p.breakeven)

	const n = 300
	body := make([]byte, size)

	type result struct {
		loc Loc
		ok  bool
		err error
	}
	results := make([]result, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			loc, ok, err := p.Put(size, body)
			results[i] = result{loc: loc, ok: ok, err: err}
		}(i)
	}
	wg.Wait()

	seen := make(map[Loc]int)
	pooled := 0
	for _, r := range results {
		require.NoError(t, r.err)
		require.True(t, r.ok, "breakeven is 0, every put should be pool-backed")
		pooled++
		seen[r.loc]++
	}

	for loc, count := range seen {
		require.Equal(t, 1, count, "slot %+v was handed out to more than one caller", loc)
	}
	require.Equal(t, n, pooled)
}

// TestPool_ConcurrentPutAndFreeDoNotCorruptOrDoubleAllocate interleaves Free (releasing slots from
// a pre-filled page) with concurrent Put calls that should be free to reuse them -- the case where
// an allocation race could hand the same just-freed slot to two callers, or a Put's WriteAt could
// land in a slot a concurrent Free is still mutating list bookkeeping for. Each concurrent Put
// writes a uniquely tagged body so a wrong-slot write shows up as corrupted content, not just a
// duplicate Loc.
func TestPool_ConcurrentPutAndFreeDoNotCorruptOrDoubleAllocate(t *testing.T) {
	dir := t.TempDir()
	size := int64(50)

	p := New(dir, WithPageBytes(20*size))
	require.NoError(t, p.DiscoverPages())
	p.FinishReconcile()

	const preFill = 40
	preLocs := make([]Loc, preFill)
	for i := range preFill {
		loc, ok, err := p.Put(size, make([]byte, size))
		require.NoError(t, err)
		require.True(t, ok)
		preLocs[i] = loc
	}

	const concurrency = 40
	newLocs := make([]Loc, concurrency)
	putErrs := make([]error, concurrency)

	var wg sync.WaitGroup
	for _, loc := range preLocs {
		wg.Add(1)
		go func(loc Loc) {
			defer wg.Done()
			p.Free(size, loc)
		}(loc)
	}
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tagged := make([]byte, size)
			binary.BigEndian.PutUint64(tagged, uint64(i))
			loc, ok, err := p.Put(size, tagged)
			if err == nil && !ok {
				err = fmt.Errorf("put %d unexpectedly fell back to non-pooled storage", i)
			}
			newLocs[i], putErrs[i] = loc, err
		}(i)
	}
	wg.Wait()

	seen := make(map[Loc]int)
	for i, err := range putErrs {
		require.NoError(t, err)
		seen[newLocs[i]]++
	}
	for loc, count := range seen {
		require.Equal(t, 1, count, "slot %+v was handed out to more than one caller", loc)
	}

	for i, loc := range newLocs {
		got, err := p.Read(size, loc)
		require.NoError(t, err)

		want := make([]byte, size)
		binary.BigEndian.PutUint64(want, uint64(i))
		require.Equal(t, want, got, "record %d content corrupted by a concurrent allocation", i)
	}
}
