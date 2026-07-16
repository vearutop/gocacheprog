package http

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/vearutop/gocacheprog/internal/cache"
)

// IntegrityCheck walks every stored cache entry, verifying its actual bytes against its declared
// size, and reports (JSON) every one that doesn't match. Pass ?dry_run=1 to only report without
// removing anything; without it, broken entries are evicted so they stop being served as hits.
func (h *Handler) IntegrityCheck(rw http.ResponseWriter, r *http.Request) {
	checker, ok := h.store.(cache.IntegrityChecker)
	if !ok {
		http.Error(rw, "integrity check is not supported", http.StatusNotImplemented)
		return
	}

	dryRun := r.URL.Query().Get("dry_run") == "1"

	report := checker.IntegrityCheck(dryRun)

	log.Printf("integrity check: checked=%d broken=%d dry_run=%t", report.Checked, len(report.Broken), report.DryRun)

	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(report); err != nil {
		log.Printf("encode integrity check report: %s", err.Error())
	}
}
