package http

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprog/internal/cache"
)

type Handler struct {
	store            cache.Store
	authToken        string
	preloadSem       chan struct{}
	preloadInFlight  int64
	preloadStarted   int64
	preloadCompleted int64
}

func NewHandler(store cache.Store, authToken string) *Handler {
	return NewHandlerWithPreloadLimit(store, authToken, 2)
}

func NewHandlerWithPreloadLimit(store cache.Store, authToken string, preloadLimit int) *Handler {
	if preloadLimit < 1 {
		preloadLimit = 1
	}

	return &Handler{
		store:      store,
		authToken:  authToken,
		preloadSem: make(chan struct{}, preloadLimit),
	}
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		rw.Header().Set("WWW-Authenticate", `Bearer realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

	if r.URL.Path == "/version" {
		logVersionProbe(r)
		_, _ = rw.Write([]byte("gocacheprogd " + version.Module("github.com/vearutop/gocacheprog").Version))
		return
	}

	if r.URL.Path == "/status" {
		h.Status(rw, r)
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

func logVersionProbe(r *http.Request) {
	log.Printf(
		"version; remote=%s; session_id=%q; started_at=%q; pid=%q; cache_dir=%q; commit=%q; parent=%q; changes=%q; build_type=%q; base=%q",
		r.RemoteAddr,
		r.Header.Get("X-GoCacheProg-Session-Id"),
		r.Header.Get("X-GoCacheProg-Started-At"),
		r.Header.Get("X-GoCacheProg-Pid"),
		r.Header.Get("X-GoCacheProg-Cache-Dir"),
		r.Header.Get("X-GoCacheProg-Commit"),
		r.Header.Get("X-GoCacheProg-Parent"),
		r.Header.Get("X-GoCacheProg-Changes"),
		r.Header.Get("X-GoCacheProg-Build-Type"),
		r.Header.Get("X-GoCacheProg-Base"),
	)
}

func (h *Handler) Stats() map[string]string {
	return map[string]string{
		"preloadInFlight":  strconv.FormatInt(atomic.LoadInt64(&h.preloadInFlight), 10),
		"preloadStarted":   strconv.FormatInt(atomic.LoadInt64(&h.preloadStarted), 10),
		"preloadCompleted": strconv.FormatInt(atomic.LoadInt64(&h.preloadCompleted), 10),
		"preloadLimit":     strconv.Itoa(cap(h.preloadSem)),
	}
}
