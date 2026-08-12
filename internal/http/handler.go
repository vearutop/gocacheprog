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
	Version       string
	ChangesID     string
	Commit        string
	BuildType     string
	PreloadBytes  int64
	PreloadTime   time.Duration
	FinalizeBytes int64
	FinalizeTime  time.Duration
	Done          bool
	FirstSeen     time.Time
	LastSeen      time.Time
	DoneAt        time.Time
}

// sessionIdleTimeout is how long a session is still shown as "in progress" after its last
// request; a client resends its session ID on every call, so a real build never goes this long
// between requests.
const sessionIdleTimeout = 5 * time.Minute

// doneSessionRetention bounds how long a session that explicitly called -github-actions-done
// stays on the status page afterward, before being dropped from the list entirely.
const doneSessionRetention = 5 * time.Minute

// sessionRetention bounds how long a session with no done signal (idle status, or a mode that
// never marks done) lingers on the status page before being pruned: sessionIdleTimeout to go
// idle, plus doneSessionRetention more to actually see it, then gone.
const sessionRetention = sessionIdleTimeout + doneSessionRetention

// sessionExpired reports whether cs should be dropped from the status page entirely.
func sessionExpired(cs *clientSession, now time.Time) bool {
	if cs.Done {
		return now.Sub(cs.DoneAt) > doneSessionRetention
	}

	return now.Sub(cs.LastSeen) > sessionRetention
}

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
		if sessionExpired(cs, now) {
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
	if v := r.Header.Get(headerCommit); v != "" {
		cs.Commit = v
	}
	if v := r.Header.Get(headerChanges); v != "" {
		cs.ChangesID = v
	}
	if v := r.Header.Get(headerBuildType); v != "" {
		cs.BuildType = v
	}
}

// recordSessionPreload attributes a completed preload/restore-cache transfer (whichever the
// session's mode actually uses to pull the cache down at job start) to its session, if any.
func (h *Handler) recordSessionPreload(r *http.Request, wireBytes int64, dur time.Duration) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	if cs := h.clientSessions[sid]; cs != nil {
		cs.PreloadBytes += wireBytes
		cs.PreloadTime += dur
	}
}

// recordSessionFinalize attributes a completed save-cache upload to its session, if any.
func (h *Handler) recordSessionFinalize(r *http.Request, wireBytes int64, dur time.Duration) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	if cs := h.clientSessions[sid]; cs != nil {
		cs.FinalizeBytes += wireBytes
		cs.FinalizeTime += dur
	}
}

// markSessionDone flags the request's session as finished, e.g. once -github-actions-done
// completes. Done sessions are dropped from the status page after doneSessionRetention.
func (h *Handler) markSessionDone(r *http.Request) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	if cs := h.clientSessions[sid]; cs != nil {
		cs.Done = true
		cs.DoneAt = time.Now()
	}
}

// clientSessionsSnapshot returns a stable-ordered copy for rendering, most recently active first.
// Expired sessions (see sessionExpired) are dropped from the underlying map here, so the page
// never has to be visited by another session's activity to clean itself up.
func (h *Handler) clientSessionsSnapshot() []clientSessionView {
	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	now := time.Now()
	views := make([]clientSessionView, 0, len(h.clientSessions))
	for id, cs := range h.clientSessions {
		if sessionExpired(cs, now) {
			delete(h.clientSessions, id)
			continue
		}

		ref := cs.ChangesID
		if ref == "" {
			ref = cs.Commit
		}

		status := "idle"
		if cs.Done {
			status = "done"
		} else if now.Sub(cs.LastSeen) <= sessionIdleTimeout {
			status = "in progress"
		}

		end := now
		if cs.Done {
			end = cs.DoneAt
		}

		views = append(views, clientSessionView{
			Status:        status,
			Version:       cs.Version,
			Ref:           ref,
			BuildType:     cs.BuildType,
			PreloadBytes:  cs.PreloadBytes,
			PreloadTime:   cs.PreloadTime,
			FinalizeBytes: cs.FinalizeBytes,
			FinalizeTime:  cs.FinalizeTime,
			SessionTime:   end.Sub(cs.FirstSeen),
			lastSeen:      cs.LastSeen,
		})
	}

	sort.Slice(views, func(i, j int) bool { return views[i].lastSeen.After(views[j].lastSeen) })

	return views
}

type clientSessionView struct {
	Status        string
	Version       string
	Ref           string
	BuildType     string
	PreloadBytes  int64
	PreloadTime   time.Duration
	FinalizeBytes int64
	FinalizeTime  time.Duration
	SessionTime   time.Duration
	lastSeen      time.Time
}

// routes maps each authenticated endpoint to its handler method, so ServeHTTP is a single lookup
// instead of a long if-chain. "/" and "/session-done"/"/version" (both trivial, listed here for
// the same one-lookup dispatch) are the only paths with logic that doesn't fit a bare method
// value; see serveVersion and serveSessionDone.
var routes = map[string]func(*Handler, http.ResponseWriter, *http.Request){
	"/version":             (*Handler).serveVersion,
	"/status":              (*Handler).Status,
	"/session-done":        (*Handler).serveSessionDone,
	"/preload":             (*Handler).Preload,
	"/cache-used":          (*Handler).CacheUsed,
	"/restore-cache":       (*Handler).RestoreCache,
	"/clear":               (*Handler).ClearCache,
	"/inspect":             (*Handler).InspectCache,
	"/integrity-check":     (*Handler).IntegrityCheck,
	"/save-cache":          (*Handler).SaveCache,
	"/save-cache-chunk":    (*Handler).SaveCacheChunk,
	"/save-cache-start":    (*Handler).StartSaveCache,
	"/save-cache-finalize": (*Handler).FinalizeSaveCache,
	"/save-cache-abort":    (*Handler).AbortSaveCache,
	"/put":                 (*Handler).Put,
	"/get":                 (*Handler).Get,
	"/head":                (*Handler).Head,
}

func (h *Handler) serveVersion(rw http.ResponseWriter, r *http.Request) {
	logVersionProbe(r)
	if _, err := rw.Write([]byte("gocacheprog " + version.Module("github.com/vearutop/gocacheprog").Version)); err != nil {
		log.Printf("write version response: %s", err.Error())
	}
}

func (h *Handler) serveSessionDone(rw http.ResponseWriter, r *http.Request) {
	h.markSessionDone(r)
	rw.WriteHeader(http.StatusNoContent)
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

	route, ok := routes[r.URL.Path]
	if !ok {
		http.NotFound(rw, r)
		return
	}

	route(h, rw, r)
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
