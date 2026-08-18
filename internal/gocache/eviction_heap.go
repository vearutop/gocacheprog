package gocache

import (
	"container/heap"
	"time"
)

// evictionHeap orders genuinely new saves by eviction priority (see moreEvictable), so
// evictOneLocked can find the next entry to evict in O(log n) instead of rescanning the whole
// index. That rescan is what made a single synchronous eviction pass (closing the margin gap
// after a big save-cache burst can mean evicting thousands of entries in a row -- see
// evictionMarginFraction) expensive enough to starve concurrent requests' access to Store.mu, in
// production surfacing as save-cache chunk uploads timing out waiting for response headers.
//
// It's push-only, with no update-in-place and no removal by key -- leaning on two properties of
// how the index actually changes: a path's sort key (size, ModTimeMicro) is set once, when it's
// first saved, and never changes afterward. A redundant re-save of an already-existing path
// (GOCACHE paths are content-addressed, so this is always identical content) leaves its
// ModTimeMicro untouched (see putOne), so the entry already pushed for it is still exactly
// correct -- nothing to update. That leaves exactly one way an entry becomes wrong: something
// (Clear, removeBrokenEntry) deletes the path from the index outside of eviction. Rather than
// also removing it from here -- which would need a second index (path -> heap slot) purely to
// make that removal fast -- evictOneLocked just validates what it pops against the live index
// and discards a stale entry if the path is gone or has since been genuinely recreated with a
// new sort key (a fresh entry for it, pushed separately, is still in here and still correct).
// The cost: a path deleted by Clear/removeBrokenEntry lingers here as a harmless phantom until
// eviction actually runs and pops past it -- but Clear is an occasional admin action, not hot
// traffic, and a stale entry's own (older) sort key means eviction would visit it early anyway.
type evictionHeap struct {
	items  []evictionHeapEntry
	bucket time.Duration
}

type evictionHeapEntry struct {
	path         string
	size         int64
	modTimeMicro int64
}

func newEvictionHeap(bucket time.Duration) *evictionHeap {
	return &evictionHeap{bucket: bucket}
}

func (h *evictionHeap) Len() int { return len(h.items) }

// Less reuses moreEvictable's exact comparison (only ModTimeMicro and size matter to it), so the
// heap's ordering can never drift out of sync with the ordering documented and tested there.
func (h *evictionHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	return moreEvictable(indexEntry{ModTimeMicro: a.modTimeMicro}, a.size, indexEntry{ModTimeMicro: b.modTimeMicro}, b.size, h.bucket)
}

func (h *evictionHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *evictionHeap) Push(x any) {
	e, ok := x.(evictionHeapEntry)
	if !ok {
		panic("evictionHeap.Push: unexpected type, only push (below) ever calls this")
	}
	h.items = append(h.items, e)
}

func (h *evictionHeap) Pop() any {
	n := len(h.items)
	e := h.items[n-1]
	h.items = h.items[:n-1]
	return e
}

// push adds path as a new heap entry. Only ever called for a genuinely new save (see putOne).
func (h *evictionHeap) push(path string, size, modTimeMicro int64) {
	heap.Push(h, evictionHeapEntry{path: path, size: size, modTimeMicro: modTimeMicro})
}

// pop removes and returns the single most evictable entry (see moreEvictable), or ok = false if
// the heap is empty. It may be stale -- see evictionHeap's own doc comment -- validating that is
// evictOneLocked's job, not this type's, since it requires looking the path up in Store.index.
func (h *evictionHeap) pop() (entry evictionHeapEntry, ok bool) {
	if h.Len() == 0 {
		return evictionHeapEntry{}, false
	}
	e, ok := heap.Pop(h).(evictionHeapEntry)
	if !ok {
		panic("evictionHeap.pop: unexpected type, only Push (above) ever adds entries")
	}
	return e, true
}
