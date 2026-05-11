package http

import (
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"time"
)

type statsProvider interface {
	Stats() map[string]string
}

func (h *Handler) Status(rw http.ResponseWriter, r *http.Request) {
	resp := map[string]any{}

	if s, ok := h.store.(statsProvider); ok {
		stats := s.Stats()
		resp["store"] = stats
		augmentStatusStats(stats)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	resp["runtime"] = map[string]any{
		"heapInuseBytes": ms.HeapInuse,
		"heapInuse":      byteSize(int64(ms.HeapInuse)),
	}

	rw.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(rw)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
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
