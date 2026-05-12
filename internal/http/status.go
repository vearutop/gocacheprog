package http

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"time"
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

func augmentStatusStats(stats map[string]string) {
	if stats == nil {
		return
	}

	addHumanByteStat(stats, "diskBytes")
	addHumanByteStat(stats, "maxDiskBytes")

	if v := stats["lastEvictionUnixMicro"]; v != "" && v != "0" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			stats["lastEviction"] = time.UnixMicro(n).UTC().Format(time.RFC3339)
		}
	} else {
		stats["lastEviction"] = ""
	}
}

func addHumanByteStat(stats map[string]string, key string) {
	v := stats[key]
	if v == "" {
		return
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return
	}
	stats[key+"Human"] = byteSize(n)
}

func uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(v)
}
