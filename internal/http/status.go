package http

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"runtime"
	"strconv"
)

type statsProvider interface {
	Stats() map[string]string
}

func (h *Handler) Status(rw http.ResponseWriter, _ *http.Request) {
	resp := map[string]any{}

	if s, ok := h.store.(statsProvider); ok {
		stats := s.Stats()
		augmentStatusStats(stats)
		resp["store"] = stats
	}

	if s, ok := any(h.gocacheStore).(statsProvider); ok && h.gocacheStore != nil {
		stats := s.Stats()
		augmentStatusStats(stats)
		resp["gocache"] = stats
	}

	httpStats := h.Stats()
	if len(httpStats) > 0 {
		resp["http"] = httpStats
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	resp["runtime"] = map[string]any{
		"heapInuseBytes": ms.HeapInuse,
		"heapInuse":      byteSize(uint64ToInt64(ms.HeapInuse)),
	}

	rw.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(rw)
	enc.SetIndent("", "  ")
	if err := enc.Encode(resp); err != nil {
		log.Printf("encode status response: %s", err.Error())
	}
}

// augmentStatusStats reshapes a store's raw Stats() map for display (the "/" status page and the
// "/status" JSON endpoint both funnel through this): "diskBytes" becomes a human-readable
// "storage" figure, and "maxDiskBytes" is dropped entirely -- individual per-store budgets are
// rarely set in server mode (see the combined-budget line instead), so showing a usually-zero
// number was just noise.
func augmentStatusStats(stats map[string]string) {
	if stats == nil {
		return
	}

	if v, ok := stats["diskBytes"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			stats["storage"] = byteSize(n)
		}
		delete(stats, "diskBytes")
	}

	delete(stats, "maxDiskBytes")
}

func uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(v)
}
