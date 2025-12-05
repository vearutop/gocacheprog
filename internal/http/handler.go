package http

import (
	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"net/http"
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

	if r.URL.Path == "/put" {
		//println("put")
		h.Put(rw, r)
		return
	}

	if r.URL.Path == "/get" {
		//println("get")
		h.Get(rw, r)
		return
	}

	http.NotFound(rw, r)
}
