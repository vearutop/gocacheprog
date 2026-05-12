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
		if _, err := rw.Write([]byte("gocacheprog " + version.Module("github.com/vearutop/gocacheprog").Version)); err != nil {
			log.Printf("write version response: %s", err.Error())
		}
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
	//nolint:gosec // request metadata is intentionally logged for debugging shim/daemon session fan-out.
	log.Printf(
		"version; remote=%s; session_id=%q; started_at=%q; pid=%q; cache_dir=%q; commit=%q; parent=%q; changes=%q; build_type=%q; base=%q",
		r.RemoteAddr,
		r.Header.Get(headerSessionID),
		r.Header.Get(headerStartedAt),
		r.Header.Get(headerPID),
		r.Header.Get(headerCacheDir),
		r.Header.Get(headerCommit),
		r.Header.Get(headerParent),
		r.Header.Get(headerChanges),
		r.Header.Get(headerBuildType),
		r.Header.Get(headerBase),
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
