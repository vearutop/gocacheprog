package http

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"
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
	panicCount           int64
	panicMu              sync.Mutex
	lastPanicMessage     string
	lastPanicStack       string
	lastPanicAt          time.Time
	sessionsJSONLPath    string
	settingsPath         string
	settingsMu           sync.Mutex
	settings             serverSettings
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

// WithSessionsJSONL appends a line to path every time a session starts and every time it's
// marked done (see appendSessionsJSONL), for offline analysis of cache performance over time.
// Empty (the default) disables it.
func WithSessionsJSONL(path string) HandlerOption {
	return func(h *Handler) { h.sessionsJSONLPath = path }
}

// WithSettingsPath enables persisted, dynamically-updatable server settings (see serverSettings)
// backed by path -- read once at startup, written on every change, surviving restarts. Empty
// (the default) disables persistence only: settings can still be changed at runtime, they just
// reset to defaults on the next restart since there's nowhere to write them.
func WithSettingsPath(path string) HandlerOption {
	return func(h *Handler) { h.settingsPath = path }
}

// clientSession tracks the most recent request seen from one client process (identified by its
// session ID), so the status page can show which sessions are still active and on what version.
type clientSession struct {
	Version       string
	ChangesID     string
	Commit        string
	BuildType     string
	JobURL        string
	PreloadBytes  int64
	PreloadTime   time.Duration
	PreloadSource string
	FinalizeBytes int64
	FinalizeTime  time.Duration
	Done          bool
	FirstSeen     time.Time
	LastSeen      time.Time
	DoneAt        time.Time
	// Extra holds whatever markSessionDone's caller reported alongside going done (see
	// MarkSessionDone), merged as additional top-level fields into this session's "done" line in
	// sessions.jsonl -- e.g. -github-actions-done's save-cache skip counts and report_<name>
	// file contents. Never shown on the status page, only in sessions.jsonl.
	Extra map[string]any
}

// sessionIdleTimeout is how long a session is still shown as "in progress" after its last
// request; a client resends its session ID on every call, so a real build never goes this long
// between requests.
const sessionIdleTimeout = 10 * time.Minute

// doneSessionRetention bounds how long a session that explicitly called -github-actions-done
// stays on the status page afterward, before being dropped from the list entirely.
const doneSessionRetention = 24 * time.Hour

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
	h.loadSettings()

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

// evictionMarginFraction is the fraction of combinedMaxDiskBytes eviction clears below the
// limit, so the combined total has room to grow before the next write needs evicting again --
// this runs after every request, so settling exactly at the limit would mean re-evicting on
// almost every subsequent write once the stores fill up. Same reasoning as
// gocache/local.Store's own evictionMarginFraction and evictOldestUntilFits's client-side trim.
const evictionMarginFraction = 10

// enforceCombinedBudget evicts from whichever store currently holds the most bytes, but only
// once combined usage across store and gocacheStore actually exceeds combinedMaxDiskBytes --
// growth up to that real limit is otherwise left alone. Once triggered, it evicts down to a
// margin below the limit rather than stopping the instant it's back under, and keeps going until
// that margin is reached or nothing more can be evicted anywhere. Called after every request so
// a burst of writes can't outrun it the way a timer-based sweep could.
func (h *Handler) enforceCombinedBudget() {
	if h.combinedMaxDiskBytes <= 0 {
		return
	}

	stores := h.diskBudgetStores()
	if len(stores) == 0 {
		return
	}

	total := func() int64 {
		var t int64
		for _, s := range stores {
			t += s.DiskBytes()
		}
		return t
	}

	if total() <= h.combinedMaxDiskBytes {
		return
	}

	target := h.combinedMaxDiskBytes - h.combinedMaxDiskBytes/evictionMarginFraction

	for total() > target {
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

	for id, cs := range h.clientSessions {
		if sessionExpired(cs, now) {
			delete(h.clientSessions, id)
		}
	}

	cs, isNew := h.clientSessions[sid], false
	if cs == nil {
		cs = &clientSession{FirstSeen: now}
		h.clientSessions[sid] = cs
		isNew = true
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
	if v := r.Header.Get(headerJobURL); v != "" {
		cs.JobURL = v
	}

	// Snapshotted (a plain struct copy) while still under the lock, then appended to
	// sessions.jsonl after releasing it -- file I/O has no business blocking every other
	// session's bookkeeping.
	snapshot := *cs
	h.clientSessionsMu.Unlock()

	if isNew {
		h.appendSessionsJSONL("started", sid, snapshot)
	}
}

// recordSessionPreload attributes a completed preload/restore-cache transfer (whichever the
// session's mode actually uses to pull the cache down at job start) to its session, if any.
// sources records which manifest(s) resolved this pull (e.g. "changes,default"), so a small or
// unhealthy-looking preload can be diagnosed from the status page without needing server logs.
func (h *Handler) recordSessionPreload(r *http.Request, wireBytes int64, dur time.Duration, sources string) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	h.clientSessionsMu.Lock()
	defer h.clientSessionsMu.Unlock()

	if cs := h.clientSessions[sid]; cs != nil {
		cs.PreloadBytes += wireBytes
		cs.PreloadTime += dur
		if sources != "" {
			cs.PreloadSource = sources
		}
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
// completes. Done sessions are dropped from the status page after doneSessionRetention. extra
// (see MarkSessionDone) is attached to the session before it's appended to sessions.jsonl.
func (h *Handler) markSessionDone(r *http.Request, extra map[string]any) {
	sid := r.Header.Get(headerSessionID)
	if sid == "" {
		return
	}

	h.clientSessionsMu.Lock()
	cs := h.clientSessions[sid]
	var snapshot clientSession
	found := cs != nil
	if found {
		cs.Done = true
		cs.DoneAt = time.Now()
		cs.Extra = extra
		snapshot = *cs
	}
	h.clientSessionsMu.Unlock()

	if found {
		h.appendSessionsJSONL("done", sid, snapshot)
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
			Ref:           sessionRef(*cs),
			JobURL:        cs.JobURL,
			BuildType:     cs.BuildType,
			StartedAt:     cs.FirstSeen,
			PreloadBytes:  cs.PreloadBytes,
			PreloadTime:   cs.PreloadTime,
			PreloadSource: cs.PreloadSource,
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
	JobURL        string
	BuildType     string
	StartedAt     time.Time
	PreloadBytes  int64
	PreloadTime   time.Duration
	PreloadSource string
	FinalizeBytes int64
	FinalizeTime  time.Duration
	SessionTime   time.Duration
	lastSeen      time.Time
}

// sessionRef is the identifying label shown for a session: its changes-id (PR/branch) if set,
// else the raw commit.
func sessionRef(cs clientSession) string {
	if cs.ChangesID != "" {
		return cs.ChangesID
	}
	return cs.Commit
}

// routes maps each authenticated endpoint to its handler method, so ServeHTTP is a single lookup
// instead of a long if-chain. "/" and "/session-done"/"/version" (both trivial, listed here for
// the same one-lookup dispatch) are the only paths with logic that doesn't fit a bare method
// value; see serveVersion and serveSessionDone.
var routes = map[string]func(*Handler, http.ResponseWriter, *http.Request){
	"/version":                      (*Handler).serveVersion,
	"/status":                       (*Handler).Status,
	"/session-done":                 (*Handler).serveSessionDone,
	"/preload":                      (*Handler).Preload,
	"/cache-used":                   (*Handler).CacheUsed,
	"/restore-cache":                (*Handler).RestoreCache,
	"/clear":                        (*Handler).ClearCache,
	"/inspect":                      (*Handler).InspectCache,
	"/integrity-check":              (*Handler).IntegrityCheck,
	"/save-cache-has":               (*Handler).SaveCacheHas,
	"/save-cache":                   (*Handler).SaveCache,
	"/save-cache-chunk":             (*Handler).SaveCacheChunk,
	"/save-cache-start":             (*Handler).StartSaveCache,
	"/save-cache-finalize":          (*Handler).FinalizeSaveCache,
	"/save-cache-abort":             (*Handler).AbortSaveCache,
	"/put":                          (*Handler).Put,
	"/get":                          (*Handler).Get,
	"/head":                         (*Handler).Head,
	"/settings/preload-limit-bytes": (*Handler).PreloadLimitBytesSettings,
}

func (h *Handler) serveVersion(rw http.ResponseWriter, r *http.Request) {
	logVersionProbe(r)
	if _, err := rw.Write([]byte("gocacheprog " + version.Module("github.com/vearutop/gocacheprog").Version)); err != nil {
		log.Printf("write version response: %s", err.Error())
	}
}

// serveSessionDone accepts an optional JSON object body (see Client.MarkSessionDone) with extra
// fields to attach to this session's sessions.jsonl "done" line. A missing/empty body is the
// common case (most callers have nothing extra to report) and isn't an error.
func (h *Handler) serveSessionDone(rw http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r)

	var extra map[string]any
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&extra); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
	}

	h.markSessionDone(r, extra)
	rw.WriteHeader(http.StatusNoContent)
}

// recordPanic tracks a recovered panic so the status page can surface it, instead of it only
// ever showing up as a one-line log entry easy to miss.
func (h *Handler) recordPanic(rec any) {
	atomic.AddInt64(&h.panicCount, 1)
	stack := string(debug.Stack())

	h.panicMu.Lock()
	h.lastPanicMessage = fmt.Sprintf("%v", rec)
	h.lastPanicStack = stack
	h.lastPanicAt = time.Now()
	h.panicMu.Unlock()

	log.Printf("panic recovered: %v\n%s", rec, stack)
}

func (h *Handler) panicSnapshot() (count int64, message, stack string, at time.Time) {
	h.panicMu.Lock()
	defer h.panicMu.Unlock()

	return atomic.LoadInt64(&h.panicCount), h.lastPanicMessage, h.lastPanicStack, h.lastPanicAt
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// A panic anywhere below would otherwise crash the whole process (a Go panic in any
	// goroutine, unrecovered, takes down every in-flight request across every session, not just
	// this one) -- recover it here so one bad request fails on its own instead.
	defer func() {
		if rec := recover(); rec != nil {
			h.recordPanic(rec)
			http.Error(rw, "internal server error", http.StatusInternalServerError)
		}
	}()

	if r.URL.Path == "/" {
		h.Index(rw, r)
		return
	}

	// Basic-Auth-gated like "/", not Bearer-gated like the routes below: both are meant for a
	// human hitting the URL directly (browser or curl -u), not the cache protocol client.
	if r.URL.Path == "/sessions.jsonl" {
		h.SessionsJSONL(rw, r)
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
	writer          *io.PipeWriter
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
		"panics":           strconv.FormatInt(atomic.LoadInt64(&h.panicCount), 10),
	}
}
