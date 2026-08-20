package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/vearutop/gocacheprog/internal/cache"
)

// serverSettings is the on-disk shape of h.settingsPath (see WithSettingsPath) -- dynamically
// changeable over HTTP, persisted so a restart doesn't silently reset them back to defaults.
// Deliberately a single open struct even though it holds one field today: the file (and this
// type) is meant to grow other server-side settings the same way, not be re-designed for each one.
type serverSettings struct {
	// PreloadLimitBytesByBuildType caps the total wire bytes a single preload/restore-cache
	// response for a build type may return, applied only when the request itself didn't already
	// specify a limit (see preloadLimitBytesFor's callers in restore_cache.go/preload.go) --
	// a request-supplied limit always wins over this server-side default.
	PreloadLimitBytesByBuildType map[string]int64 `json:"preload_limit_bytes_by_build_type,omitempty"`
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

// preloadLimitBytesFor returns the server-configured preload/restore byte budget for buildType,
// or 0 (disabled) if none is set.
func (h *Handler) preloadLimitBytesFor(buildType string) int64 {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	return h.settings.PreloadLimitBytesByBuildType[buildType]
}

// setPreloadLimitBytes sets buildType's preload/restore byte budget, or clears it entirely when
// bytes <= 0 -- matching the 0-means-disabled convention already used throughout this codebase
// (MaxFileBytes, RestoreLimitBytes, max_cache_bytes). Persists to h.settingsPath if configured;
// with no path configured, the change still takes effect for this process, it just won't survive
// a restart.
func (h *Handler) setPreloadLimitBytes(buildType string, bytes int64) error {
	h.settingsMu.Lock()
	defer h.settingsMu.Unlock()

	if bytes <= 0 {
		delete(h.settings.PreloadLimitBytesByBuildType, buildType)
	} else {
		if h.settings.PreloadLimitBytesByBuildType == nil {
			h.settings.PreloadLimitBytesByBuildType = make(map[string]int64)
		}
		h.settings.PreloadLimitBytesByBuildType[buildType] = bytes
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

// PreloadLimitBytesSettings views (GET) or changes (POST) the server-side per-build-type preload
// budget (see serverSettings.PreloadLimitBytesByBuildType). Bearer-gated like the other admin
// endpoints (/clear, /inspect), not Basic-Auth-gated like the status page.
//
// GET returns the full current map as JSON.
//
// POST requires a build-type query param and sets that build type's budget to the bytes query
// param's value; bytes=0 (or omitted) clears the override for that build type instead.
func (h *Handler) PreloadLimitBytesSettings(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.settingsMu.Lock()
		limits := h.settings.PreloadLimitBytesByBuildType
		h.settingsMu.Unlock()

		rw.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(rw).Encode(limits); err != nil {
			log.Printf("encode preload-limit-bytes settings: %s", err.Error())
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

		if err := h.setPreloadLimitBytes(buildType, bytes); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.WriteHeader(http.StatusNoContent)
	default:
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// trimToPreloadBudget drops items from a preload response's most expensive entries first --
// largest wire size first -- until the remaining total fits within limitBytes, preserving the
// original relative order of whatever survives. limitBytes <= 0 disables trimming (returns items
// unchanged). Unlike gocache.Store's own RestoreLimitBytes selection (which prioritizes recency
// over size), this is the GOCACHEPROG /preload path's only total-size control today, with no
// existing behavior to stay compatible with -- so it implements the simpler, literal "biggest
// goes first" policy instead.
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

	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return sizeOf(items[order[a]]) > sizeOf(items[order[b]])
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
