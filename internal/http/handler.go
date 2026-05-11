package http

import (
	"net/http"

	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprogd/internal/cache"
)

type Handler struct {
	store cache.Store
}

func NewHandler(store cache.Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/version" {
		println("version")
		_, _ = rw.Write([]byte("gocacheprogd " + version.Module("github.com/vearutop/gocacheprogd").Version))
		return
	}

	if r.URL.Path == "/preload" {
		println("preload")
		h.Preload(rw, r)
		return
	}

	if r.URL.Path == "/cache-used" {
		h.CacheUsed(rw, r)
		return
	}

	if r.URL.Path == "/put" {
		h.Put(rw, r)
		return
	}

	if r.URL.Path == "/get" {
		h.Get(rw, r)
		return
	}

	if r.URL.Path == "/head" {
		h.Head(rw, r)
		return
	}

	http.NotFound(rw, r)
}
