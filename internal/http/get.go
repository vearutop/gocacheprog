package http

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/vearutop/gocacheprog/internal/cache"
)

func (h *Handler) Get(rw http.ResponseWriter, r *http.Request) {
	bb, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

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
			diskPath := item.DiskPath
			item.SetBodyReader(func() (io.ReadCloser, error) {
				f, err := os.Open(diskPath) //nolint:gosec // diskPath comes from the local cache index.
				if err != nil {
					return nil, err
				}
				return f, nil
			})

			item.DiskPath = ""
		}

		resp.Items = append(resp.Items, item)
	})
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	cl, err := resp.ContentLength()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.Itoa(int(cl)))
	rw.WriteHeader(http.StatusOK)

	n, err := resp.WriteTo(rw)
	if err != nil {
		log.Println("get error:", err.Error(), "; bytes written:", n, "; content length:", cl)
	}
}
