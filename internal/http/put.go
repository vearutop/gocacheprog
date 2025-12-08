package http

import (
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"io"
	"net/http"
)

func (h *Handler) Put(rw http.ResponseWriter, r *http.Request) {
	var resp cache.Response

	_, err := resp.ReaderFrom(r.Body, func(item cache.ResponseItem, body io.Reader) error {
		if item.Size != 0 {
			if body == nil {
				return fmt.Errorf("empty body, item: %v", item)
			}

			item.DiskPath = ""
			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(body), nil
			})
		}

		return h.store.Put(cache.Response{Items: []cache.ResponseItem{item}})
	})
	defer r.Body.Close()

	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
}
