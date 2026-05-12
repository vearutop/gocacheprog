package http

import (
	"bufio"
	"io"
	"net/http"
	"strings"

	"github.com/vearutop/gocacheprog/internal/cache"
)

func (h *Handler) CacheUsed(rw http.ResponseWriter, r *http.Request) {
	commit := strings.TrimSpace(r.URL.Query().Get("commit"))
	changesID := strings.TrimSpace(r.URL.Query().Get("changes-id"))
	buildType := strings.TrimSpace(r.URL.Query().Get("build-type"))
	replaceChanges := r.URL.Query().Get("replace-changes") == "1"
	if commit == "" && changesID == "" {
		http.Error(rw, "missing commit or changes-id", http.StatusBadRequest)
		return
	}

	recorder, ok := h.store.(cache.UsageRecorder)
	if !ok {
		http.Error(rw, "cache-used is not supported", http.StatusNotImplemented)
		return
	}

	actionIDs, err := readActionIDs(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	if err := recorder.PostCacheUsed(commit, changesID, buildType, actionIDs, replaceChanges); err != nil {
		if isManifestValidationError(err) {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
}

func readActionIDs(body io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	seen := map[string]struct{}{}
	res := make([]string, 0)

	for scanner.Scan() {
		actionID := strings.TrimSpace(scanner.Text())
		if actionID == "" {
			continue
		}

		if _, ok := seen[actionID]; ok {
			continue
		}

		seen[actionID] = struct{}{}
		res = append(res, actionID)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return res, nil
}
