package http

import (
	"encoding/json"
	"log"
	"net/http"
)

// InspectKeys checks a batch of GOCACHE object keys (this store's own on-disk relative object
// path convention, e.g. "xx/hash-a") against the resolved manifest(s)/store for the request's
// scope (build-type/commit/changes-id/base-commit query params, same as every other gocache
// route -- see parseGOCACHERequest) -- see gocache.Store.InspectKeys for what each result field
// means. Built for post-mortem cache-miss forensics: handed a specific miss's object keys (e.g.
// from teststat's -testcache-keys), this answers "was it ever seen before, and if so where did
// it go" instead of a caller having to guess.
func (h *Handler) InspectKeys(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "inspect-keys is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	var keys []string
	if err := json.NewDecoder(r.Body).Decode(&keys); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	req := parseGOCACHERequest(r)

	result, err := h.gocacheStore.InspectKeys(req, keys)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(result); err != nil {
		log.Printf("encode inspect-keys response: %s", err.Error())
	}
}
