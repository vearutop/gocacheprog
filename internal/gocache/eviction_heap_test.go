package gocache

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEvictionHeap_OrdersLikeMoreEvictable covers the actual point of introducing the heap: it
// must pop entries in exactly the order moreEvictable would pick via a full scan -- older
// recency bucket first, then larger size within the same bucket.
func TestEvictionHeap_OrdersLikeMoreEvictable(t *testing.T) {
	bucket := time.Hour
	h := newEvictionHeap(bucket)

	bucketMicro := bucket.Microseconds()
	bucketStart := (time.Now().UnixMicro() / bucketMicro) * bucketMicro

	// "older" is a whole bucket earlier than everything else, so it must come out first
	// regardless of size. "small"/"big" share a bucket with each other but not with "older", so
	// among them the larger one must come out first.
	h.push("older", 1, bucketStart-bucketMicro)
	h.push("small", 100, bucketStart+int64(10*time.Minute/time.Microsecond))
	h.push("big", 1_000_000, bucketStart+int64(50*time.Minute/time.Microsecond))

	var order []string
	for {
		e, ok := h.pop()
		if !ok {
			break
		}
		order = append(order, e.path)
	}

	require.Equal(t, []string{"older", "big", "small"}, order)
}

// TestEvictionHeap_PushNeverDeduplicates covers the flip side of being push-only: two pushes for
// the same path (the "stale entry left behind by Clear, later genuinely recreated" case
// evictOneLocked has to handle) are two distinct heap entries, not merged or overwritten -- the
// heap itself has no notion of "this path already exists", by design (see its doc comment).
func TestEvictionHeap_PushNeverDeduplicates(t *testing.T) {
	h := newEvictionHeap(0) // strict LRU, no bucketing -- simplest to reason about here

	h.push("a", 10, 100)
	h.push("a", 10, 200)
	require.Equal(t, 2, h.Len())

	e, ok := h.pop()
	require.True(t, ok)
	require.Equal(t, int64(100), e.modTimeMicro, "the older push must still come out first")

	e, ok = h.pop()
	require.True(t, ok)
	require.Equal(t, int64(200), e.modTimeMicro)
}

func TestEvictionHeap_PopOnEmptyHeap(t *testing.T) {
	h := newEvictionHeap(time.Hour)

	_, ok := h.pop()
	require.False(t, ok)
}

// BenchmarkEvictionHeap_Pop quantifies the actual point of this data structure: at production
// scale (a real server's index was observed at ~200,000 entries), evicting many entries in one
// synchronous call used to mean repeating a full O(n) index scan per entry -- slow enough, at
// that scale, to starve other requests' access to Store.mu for many seconds. Run with
// `go test -bench EvictionHeap_Pop -benchtime 1x ./internal/gocache` to see the per-eviction cost
// stay flat regardless of heap size.
func BenchmarkEvictionHeap_Pop(b *testing.B) {
	const size = 200_000

	h := newEvictionHeap(time.Hour)
	for i := 0; i < size; i++ {
		h.push(strconv.Itoa(i), int64(i%4096+1), int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N && h.Len() > 0; i++ {
		h.pop()
	}
}
