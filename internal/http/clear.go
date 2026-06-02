package http

import (
	"encoding/json"
	"net/http"

	"github.com/vearutop/gocacheprog/internal/gocache"
)

func (h *Handler) ClearCache(rw http.ResponseWriter, r *http.Request) {
	h.handleNativeAdminJSON(rw, r, "clear", func(req gocache.Request) (any, error) {
		return h.gocacheStore.Clear(req)
	})
}

func (h *Handler) InspectCache(rw http.ResponseWriter, r *http.Request) {
	h.handleNativeAdminJSON(rw, r, "inspect", func(req gocache.Request) (any, error) {
		return h.gocacheStore.Inspect(req)
	})
}

func (h *Handler) handleNativeAdminJSON(rw http.ResponseWriter, r *http.Request, op string, run func(req gocache.Request) (any, error)) {
	if h.gocacheStore == nil {
		http.Error(rw, op+" is not supported", http.StatusNotImplemented)
		return
	}

	rawReq := parseGOCACHERequest(r)
	resp, err := run(gocache.Request{
		Commit:    rawReq.Commit,
		ChangesID: rawReq.ChangesID,
		BuildType: rawReq.BuildType,
	})
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(resp); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
	}
}
