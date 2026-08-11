package http

import (
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/gocache"
)

type Handler struct {
	store                cache.Store
	gocacheStore         *gocache.Store
	authToken            string
	fallbackAuthToken    string
	preloadSem           chan struct{}
	saveSessionsMu       sync.Mutex
	saveSessions         map[string]*saveCacheSession
	preloadInFlight      int64
	preloadStarted       int64
	preloadCompleted     int64
	clientSessionsMu     sync.Mutex
	clientSessions       map[string]*clientSession
	combinedMaxDiskBytes int64
}

// HandlerOption configures optional Handler behavior not covered by NewHandlerWithPreloadLimit's
// required parameters.
type HandlerOption func(*Handler)

// WithMaxDiskBytes sets a combined on-disk budget shared across store and gocacheStore: when
// their combined usage exceeds n, requests trigger eviction from whichever store currently holds
// the most bytes until back under budget. n <= 0 disables combined enforcement (the default).
func WithMaxDiskBytes(n int64) HandlerOption {
	return func(h *Handler) { h.combinedMaxDiskBytes = n }
}

// clientSession tracks the most recent request seen from one client process (identified by its
// session ID), so the status page can show which sessions are still active and on what version.
type clientSession struct {
	Version   string
	PID       string
	CacheDir  string
	Commit    string
	BuildType string
	FirstSeen time.Time
	LastSeen  time.Time
}

// sessionIdleTimeout is how long a session is still shown as "in progress" after its last
// request; a client resends its session ID on every call, so a real build never goes this long
// between requests.
const sessionIdleTimeout = 5 * time.Minute

// sessionRetention bounds how long a finished session lingers on the status page before being
// pruned. ponytail: prune is a linear scan of the whole map on every request; fine at CI-fleet
// scale, revisit with a time-ordered index if session churn ever makes this measurable.
const sessionRetention = 24 * time.Hour

func NewHandler(store cache.Store, authToken string) *Handler {
	return NewHandlerWithPreloadLimit(store, nil, authToken, "", 2)
}

func NewHandlerWithPreloadLimit(store cache.Store, gocacheStore *gocache.Store, authToken, fallbackAuthToken string, preloadLimit int, opts ...HandlerOption) *Handler {
	if preloadLimit < 1 {
		preloadLimit = 1
	}

	h := &Handler{
		store:             store,
		gocacheStore:      gocacheStore,
		authToken:         authToken,
		fallbackAuthToken: fallbackAuthToken,
		preloadSem:        make(chan struct{}, preloadLimit),
		saveSessions:      make(map[string]*saveCacheSession),
		clientSessions:    make(map[string]*clientSession),
	}
	for _, opt := range opts {
		opt(h)
	}

	return h
}

// diskBudgetStore is implemented by both store and gocacheStore; used to enforce a combined
// budget across whichever of them are configured.
type diskBudgetStore interface {
	DiskBytes() int64
	EvictOne() bool
}

func (h *Handler) diskBudgetStores() []diskBudgetStore {
	var stores []diskBudgetStore
	if s, ok := h.store.(diskBudgetStore); ok {
		stores = append(stores, s)
	}
	if h.gocacheStore != nil {
		stores = append(stores, h.gocacheStore)
	}

	return stores
}

// enforceCombinedBudget evicts from whichever store currently holds the most bytes until combined
// usage across store and gocacheStore is back under combinedMaxDiskBytes, or nothing more can be
// evicted anywhere. Called after every request so a burst of writes can't outrun it the way a
// timer-based sweep could.
func (h *Handler) enforceCombinedBudget() {
	if h.combinedMaxDiskBytes <= 0 {
		return
	}

	stores := h.diskBudgetStores()
	if len(stores) == 0 {
		return
	}

	for {
		var total int64
		for _, s := range stores {
			total += s.DiskBytes()
		}
		if total <= h.combinedMaxDiskBytes {
			return
		}

		sort.Slice(stores, func(i, j int) bool { return stores[i].DiskBytes() > stores[j].DiskBytes() })

		evicted := false
		for _, s := range stores {
			if s.EvictOne() {
				evicted = true
				break
			}
		}
		if !evicted {
			return
		}
	}
}

// touchSession records activity from the request's session ID header, if any. Called for every
// authorized request so the status page reflects sessions that are genuinely still running.
func (h *Handler) touchSession(r *http.Request) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	now := time.Now()

	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	for id, cs := range h.clientSessions {
		if now.Sub(cs.LastSeen) > sessionRetention {
			delete(h.clientSessions, id)
		}
	}

	cs := h.clientSessions[sid]
	if cs == nil {
		cs = &clientSession{FirstSeen: now}
		h.clientSessions[sid] = cs
	}
	cs.LastSeen = now
	if v := r.Header.Get(headerClientVersion); v != "" {
		cs.Version = v
	}
	if v := r.Header.Get(headerPID); v != "" {
		cs.PID = v
	}
	if v := r.Header.Get(headerCacheDir); v != "" {
		cs.CacheDir = v
	}
	if v := r.Header.Get(headerCommit); v != "" {
		cs.Commit = v
	}
	if v := r.Header.Get(headerBuildType); v != "" {
		cs.BuildType = v
	}
}

// clientSessionsSnapshot returns a stable-ordered copy for rendering, most recently active first.
func (h *Handler) clientSessionsSnapshot() []clientSessionView {
	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	views := make([]clientSessionView, 0, len(h.clientSessions))
	for id, cs := range h.clientSessions {
		views = append(views, clientSessionView{
			SessionID:  id,
			Version:    cs.Version,
			PID:        cs.PID,
			CacheDir:   cs.CacheDir,
			Commit:     cs.Commit,
			BuildType:  cs.BuildType,
			FirstSeen:  cs.FirstSeen,
			LastSeen:   cs.LastSeen,
			InProgress: time.Since(cs.LastSeen) <= sessionIdleTimeout,
		})
	}

	sort.Slice(views, func(i, j int) bool { return views[i].LastSeen.After(views[j].LastSeen) })

	return views
}

type clientSessionView struct {
	SessionID  string
	Version    string
	PID        string
	CacheDir   string
	Commit     string
	BuildType  string
	FirstSeen  time.Time
	LastSeen   time.Time
	InProgress bool
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		h.Index(rw, r)
		return
	}

	if !h.authorized(r) {
		rw.Header().Set("WWW-Authenticate", `Bearer realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

	h.touchSession(r)
	defer h.enforceCombinedBudget()

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

	if r.URL.Path == "/restore-cache" {
		h.RestoreCache(rw, r)
		return
	}

	if r.URL.Path == "/clear" {
		h.ClearCache(rw, r)
		return
	}

	if r.URL.Path == "/inspect" {
		h.InspectCache(rw, r)
		return
	}

	if r.URL.Path == "/integrity-check" {
		h.IntegrityCheck(rw, r)
		return
	}

	if r.URL.Path == "/save-cache" {
		h.SaveCache(rw, r)
		return
	}

	if r.URL.Path == "/save-cache-chunk" {
		h.SaveCacheChunk(rw, r)
		return
	}

	if r.URL.Path == "/save-cache-start" {
		h.StartSaveCache(rw, r)
		return
	}

	if r.URL.Path == "/save-cache-finalize" {
		h.FinalizeSaveCache(rw, r)
		return
	}

	if r.URL.Path == "/save-cache-abort" {
		h.AbortSaveCache(rw, r)
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

type saveCacheSession struct {
	writer          io.WriteCloser
	done            chan error
	startedAt       time.Time
	chunks          int64
	bytes           int64
	lastLogUnixNano int64
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

	token := strings.TrimSpace(strings.TrimPrefix(auth, prefix))
	if token == h.authToken {
		return true
	}

	if h.fallbackAuthToken != "" && token == h.fallbackAuthToken {
		log.Printf("fallback auth used; remote=%s; path=%s; session_id=%q", r.RemoteAddr, r.URL.Path, r.Header.Get(headerSessionID))
		return true
	}

	return false
}

func logVersionProbe(r *http.Request) {
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
