package http

import (
	"net/http"
	"strings"

	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprogd/internal/cache"
)

type Handler struct {
	store     cache.Store
	authToken string
}

func NewHandler(store cache.Store, authToken string) *Handler {
	return &Handler{store: store, authToken: authToken}
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		rw.Header().Set("WWW-Authenticate", `Bearer realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

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

func (h *Handler) authorized(r *http.Request) bool {
	if h.authToken == "" {
		return true
	}

	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return false
	}

	return strings.TrimSpace(strings.TrimPrefix(auth, prefix)) == h.authToken
}
