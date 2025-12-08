package http

import (
	"encoding/json"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"io"
	"net/http"
)

func (h *Handler) Head(rw http.ResponseWriter, r *http.Request) {
	bb, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	if err := r.Body.Close(); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	var req cache.Request
	if err := json.Unmarshal(bb, &req); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var resp cache.Response

	err = h.store.Get(req, func(item cache.ResponseItem) {
		if item.WireSize == 0 {
			item.WireSize = item.Size
		}
		if item.DiskPath != "" {
			item.DiskPath = ""
		}

		resp.Items = append(resp.Items, item)
	})
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	j, err := json.Marshal(resp)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)

	_, _ = rw.Write(j)
}
