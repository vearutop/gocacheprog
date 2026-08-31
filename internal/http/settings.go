package http

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
)

// serverSettings is the on-disk shape of h.settingsPath (see WithSettingsPath) -- dynamically
// changeable over HTTP, persisted so a restart doesn't silently reset them back to defaults.
// Deliberately a single open struct even though it holds one field today: the file (and this
// type) is meant to grow other server-side settings the same way, not be re-designed for each one.
type serverSettings struct {
	// MaxPreloadTotalBytesByBuildType caps the total wire bytes a single preload/restore-cache
	// response for a build type may return, applied only when the request itself didn't already
	// specify a limit (see maxPreloadTotalBytesFor's callers in restore_cache.go/preload.go) --
	// a request-supplied limit always wins over this server-side default.
	MaxPreloadTotalBytesByBuildType map[string]int64 `json:"max_preload_bytes_by_build_type,omitempty"`

	// MaxFileBytesByBuildType caps the size of any single object a build type will
	// restore/preload, applied only when the request itself didn't already specify one (see
	// maxFileBytesFor's callers) -- a request-supplied value always wins over this server-side
	// default. This is also the value -github-actions-init falls back to querying (via
	// max_file_bytes) when its own DSN doesn't set one, so an operator can retune the
	// default for a build type without touching every workflow file that uses it.
	MaxFileBytesByBuildType map[string]int64 `json:"max_file_bytes_by_build_type,omitempty"`
}

// loadSettings reads h.settingsPath once at startup (see WithSettingsPath); a missing file is
// not an error (first run, or persistence not enabled), just leaves settings at their zero value.
func (h *Handler) loadSettings() {
	if h.settingsPath == "" {
		return
	}

	data, err := os.ReadFile(h.settingsPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("load settings.json: %s", err.Error())
		}
		return
	}

	var s serverSettings
	if err := json.Unmarshal(data, &s); err != nil {
		log.Printf("parse settings.json: %s", err.Error())
		return
	}

	h.settingsMu.Lock()
	h.settings = s
	h.settingsMu.Unlock()
}

// copyInt64Map returns a defensive copy of m, or nil if empty. Every reader of
// serverSettings' maps (the JSON GET handlers, the status page) must go through a snapshot
// method built on this rather than keep the live map past settingsMu.Unlock(): setMaxPreloadTotalBytes
// /setMaxFileBytes mutate that same map in place under their own lock, so holding an unlocked
// reference to it while e.g. json.Encode iterates it is a real data race, not just a staleness
// risk.
func copyInt64Map(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]int64, len(m))
	maps.Copy(out, m)

	return out
}

// maxPreloadTotalBytesSnapshot returns a defensive copy of MaxPreloadTotalBytesByBuildType (see
// copyInt64Map).
func (h *Handler) maxPreloadTotalBytesSnapshot() map[string]int64 {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return copyInt64Map(h.settings.MaxPreloadTotalBytesByBuildType)
}

// maxFileBytesSnapshot returns a defensive copy of MaxFileBytesByBuildType (see copyInt64Map).
func (h *Handler) maxFileBytesSnapshot() map[string]int64 {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return copyInt64Map(h.settings.MaxFileBytesByBuildType)
}

// maxPreloadTotalBytesFor returns the server-configured preload/restore byte budget for buildType,
// or 0 (disabled) if none is set.
func (h *Handler) maxPreloadTotalBytesFor(buildType string) int64 {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return h.settings.MaxPreloadTotalBytesByBuildType[buildType]
}

// setBuildTypeInt64 sets buildType's value in *m to bytes, or clears it entirely when bytes <= 0
// -- matching the 0-means-disabled convention already used throughout this codebase (MaxFileBytes,
// MaxPreloadTotalBytes, max_cache_bytes). Persists h.settings as a whole to h.settingsPath if
// configured; with no path configured, the change still takes effect for this process, it just
// won't survive a restart. Shared by setMaxPreloadTotalBytes/setMaxFileBytes so their persistence
// logic can't drift apart -- must be called with h.settingsMu held.
func (h *Handler) setBuildTypeInt64(m *map[string]int64, buildType string, bytes int64) error {
	if bytes <= 0 {
		delete(*m, buildType)
	} else {
		if *m == nil {
			*m = make(map[string]int64)
		}

		(*m)[buildType] = bytes
	}

	if h.settingsPath == "" {
		return nil
	}

	data, err := json.MarshalIndent(h.settings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}

	return writeFileAtomic(h.settingsPath, data, 0o600)
}

// setMaxPreloadTotalBytes sets buildType's preload/restore byte budget (see setBuildTypeInt64).
func (h *Handler) setMaxPreloadTotalBytes(buildType string, bytes int64) error {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return h.setBuildTypeInt64(&h.settings.MaxPreloadTotalBytesByBuildType, buildType, bytes)
}

// maxFileBytesFor returns the server-configured max-file-bytes default for buildType, or 0
// (disabled) if none is set.
func (h *Handler) maxFileBytesFor(buildType string) int64 {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return h.settings.MaxFileBytesByBuildType[buildType]
}

// setMaxFileBytes sets buildType's max-file-bytes default (see setBuildTypeInt64).
func (h *Handler) setMaxFileBytes(buildType string, bytes int64) error {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return h.setBuildTypeInt64(&h.settings.MaxFileBytesByBuildType, buildType, bytes)
}

// writeFileAtomic writes data to path via a temp-file-then-rename, so a reader (or a crash
// mid-write) never sees a partially-written settings.json.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("mkdir settings dir: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmpFile, data, mode); err != nil {
		if rmErr := os.Remove(tmpFile); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Printf("remove stale temp file %s: %s", tmpFile, rmErr.Error())
		}
		return fmt.Errorf("write temp settings file: %w", err)
	}

	if err := os.Rename(tmpFile, path); err != nil {
		return fmt.Errorf("rename temp settings file: %w", err)
	}

	return nil
}

// buildTypeInt64Settings implements the shared GET/POST shape for a per-build-type int64
// setting: GET returns the full current map as JSON; POST requires a build-type query param and
// sets that build type's value to the bytes query param, or clears it (bytes=0 or omitted).
// Shared by MaxPreloadTotalBytesSettings/MaxFileBytesSettings so the two endpoints' request handling
// can't drift apart -- logName only affects the encode-failure log line.
func (h *Handler) buildTypeInt64Settings(rw http.ResponseWriter, r *http.Request, logName string, snapshot func() map[string]int64, set func(buildType string, bytes int64) error) {
	switch r.Method {
	case http.MethodGet:
		rw.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(rw).Encode(snapshot()); err != nil {
			log.Printf("encode %s settings: %s", logName, err.Error())
		}
	case http.MethodPost:
		buildType := strings.TrimSpace(r.URL.Query().Get("build-type"))
		if buildType == "" {
			http.Error(rw, "build-type is required", http.StatusBadRequest)
			return
		}

		var bytes int64
		if raw := strings.TrimSpace(r.URL.Query().Get("bytes")); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(rw, fmt.Sprintf("invalid bytes %q: %s", raw, err.Error()), http.StatusBadRequest)
				return
			}
			bytes = n
		}

		if err := set(buildType, bytes); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.WriteHeader(http.StatusNoContent)
	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// MaxPreloadTotalBytesSettings views (GET) or changes (POST) the server-side per-build-type preload
// budget (see serverSettings.MaxPreloadTotalBytesByBuildType). Bearer-gated like the other admin
// endpoints (/clear, /inspect), not Basic-Auth-gated like the status page.
func (h *Handler) MaxPreloadTotalBytesSettings(rw http.ResponseWriter, r *http.Request) {
	h.buildTypeInt64Settings(rw, r, "max-preload-total-bytes", h.maxPreloadTotalBytesSnapshot, h.setMaxPreloadTotalBytes)
}

// MaxFileBytesSettings views (GET) or changes (POST) the server-side per-build-type
// max-file-bytes default (see serverSettings.MaxFileBytesByBuildType). Same shape as
// MaxPreloadTotalBytesSettings.
func (h *Handler) MaxFileBytesSettings(rw http.ResponseWriter, r *http.Request) {
	h.buildTypeInt64Settings(rw, r, "max-file-bytes", h.maxFileBytesSnapshot, h.setMaxFileBytes)
}

// preloadTrimBucket buckets items by save time before ranking them for trimming (see
// trimToPreloadBudget), matching the granularity gocache.Store's own eviction heap defaults to
// (see WithEvictionBucket) -- kept as a separate constant rather than sharing that one directly
// since this is a different store's budget, not a reason to couple the two.
const preloadTrimBucket = time.Hour

// trimToPreloadBudget drops items from a preload response until the remaining total fits within
// limitBytes, preserving the original relative order of whatever survives. limitBytes <= 0
// disables trimming (returns items unchanged). Ranking mirrors gocache.Store's own eviction
// ordering (see moreEvictable): items are bucketed by save time (preloadTrimBucket-wide), the
// oldest bucket is dropped from first, and within a bucket the largest item goes first -- so one
// large-but-not-meaningfully-newer item can't crowd out many smaller items from about the same
// time window the way a pure "biggest first" or pure "oldest first" rule each would.
func trimToPreloadBudget(items []cache.ResponseItem, limitBytes int64) []cache.ResponseItem {
	if limitBytes <= 0 || len(items) == 0 {
		return items
	}

	sizeOf := func(item cache.ResponseItem) int64 {
		if item.WireSize > 0 {
			return item.WireSize
		}
		return item.Size
	}

	total := int64(0)
	for _, item := range items {
		total += sizeOf(item)
	}
	if total <= limitBytes {
		return items
	}

	bucketOf := func(item cache.ResponseItem) int64 {
		if item.Time == nil {
			return 0
		}
		return item.Time.UnixMicro() / preloadTrimBucket.Microseconds()
	}

	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		ia, ib := items[order[a]], items[order[b]]
		if ba, bb := bucketOf(ia), bucketOf(ib); ba != bb {
			return ba < bb
		}
		return sizeOf(ia) > sizeOf(ib)
	})

	drop := make(map[int]struct{}, len(items))
	for _, idx := range order {
		if total <= limitBytes {
			break
		}
		drop[idx] = struct{}{}
		total -= sizeOf(items[idx])
	}

	survivors := make([]cache.ResponseItem, 0, len(items)-len(drop))
	for i, item := range items {
		if _, dropped := drop[i]; dropped {
			continue
		}
		survivors = append(survivors, item)
	}

	return survivors
}
