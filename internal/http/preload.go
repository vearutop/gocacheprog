package http

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/vearutop/gocacheprogd/internal/cache"
)

func (h *Handler) Preload(rw http.ResponseWriter, r *http.Request) {
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

	var req cache.PreloadRequest
	if err := json.Unmarshal(bb, &req); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var resp cache.Response

	p, ok := h.store.(cache.Preloader)
	if !ok {
		http.Error(rw, "preload is not supported", http.StatusNotImplemented)
		return
	}

	err = p.Preload(req, func(item cache.ResponseItem) {
		if item.WireSize == 0 {
			item.WireSize = item.Size
		}
		if item.DiskPath != "" {
			diskPath := item.DiskPath
			item.SetBodyReader(func() (io.ReadCloser, error) {
				f, err := os.Open(diskPath)
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

	println("preload content length:", cl, "items:", len(resp.Items))

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.Itoa(int(cl)))
	rw.WriteHeader(http.StatusOK)

	n, err := resp.WriteTo(rw)
	if err != nil {
		log.Println("get error:", err.Error(), "; bytes written:", n, "; content length:", cl)
	}
}
