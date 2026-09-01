package http

import (
	"html/template"
	"log"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bool64/dev/version"
)

var indexTemplate = template.Must(template.New("index").Parse(`<!doctype html>
<html>
<head><title>gocacheprogd status</title>
<style>
body{font-family:monospace;margin:2rem;}
table{border-collapse:collapse;margin-bottom:2rem;}
td,th{border:1px solid #ccc;padding:.25rem .75rem;text-align:left;}
h2{margin-top:0;}
.warn{color:#b00;font-weight:bold;}
.active{color:#080;font-weight:bold;}
.done{color:#357;font-weight:bold;}
.idle{color:#888;}
pre{white-space:pre-wrap;word-break:break-all;background:#f6f6f6;padding:.5rem;border:1px solid #ccc;overflow-x:auto;}
</style>
</head>
<body>
<h1>gocacheprogd status</h1>
<p>server version: <a href="{{.ServerVersionURL}}" target="_blank" rel="noopener noreferrer" style="vertical-align:middle"><svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" style="vertical-align:-3px;margin-right:.25em"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path></svg>{{.ServerVersion}}</a></p>
<p>heap in use: {{.HeapInUse}}</p>
{{if .CombinedBudget}}<p>combined disk budget: {{.CombinedBudget}}</p>{{else}}<p class="warn">no combined disk budget configured — eviction disabled</p>{{end}}
{{range .Sections}}
<h2>{{.Title}}</h2>
<table>
{{range .Rows}}<tr><th>{{.Key}}</th><td>{{.Value}}</td></tr>
{{end}}
</table>
{{end}}
<h2>Build-type overrides</h2>
<table>
<tr><th>build type</th><th>max preload bytes</th><th>max file bytes</th></tr>
{{range .Overrides}}<tr><td>{{.BuildType}}</td><td>{{.MaxPreloadTotalBytes}}</td><td>{{.MaxFileBytes}}</td></tr>
{{end}}
</table>
<form method="post">
<input type="hidden" name="action" value="set-max-preload-total-bytes">
<input name="build-type" placeholder="build type" required>
<input name="bytes" placeholder="bytes (blank clears)">
<button type="submit">Set max preload bytes</button>
</form>
<form method="post">
<input type="hidden" name="action" value="set-max-file-bytes">
<input name="build-type" placeholder="build type" required>
<input name="bytes" placeholder="bytes (blank clears)">
<button type="submit">Set max file bytes</button>
</form>
<h2>Client sessions{{if .SessionsJSONL}} <a href="/sessions.jsonl">Archive</a>{{end}}</h2>
<table>
<tr><th>status</th><th>version</th><th>ref</th><th>build type</th><th>started at</th><th>preload size</th><th>preload source</th><th>preload time</th><th>finalize size</th><th>finalize time</th><th>session time</th></tr>
{{range .Sessions}}<tr>
<td>{{if eq .Status "done"}}<span class="done">done</span>{{else if eq .Status "in progress"}}<span class="active">in progress</span>{{else}}<span class="idle">idle</span>{{end}}</td>
<td>{{.Version}}</td>
<td>{{if .JobURL}}<a href="{{.JobURL}}" target="_blank" rel="noopener noreferrer">{{.Ref}}</a>{{else}}{{.Ref}}{{end}}</td>
<td>{{.BuildType}}</td>
<td>{{.StartedAt}}</td>
<td>{{.PreloadSize}}</td>
<td>{{.PreloadSource}}</td>
<td>{{.PreloadTime}}</td>
<td>{{.FinalizeSize}}</td>
<td>{{.FinalizeTime}}</td>
<td>{{.SessionTime}}</td>
</tr>
{{end}}
</table>
<form method="post">
<input type="hidden" name="action" value="cleanup">
<button type="submit">Run cleanup now</button>
</form>
{{if .Panics.Count}}<h2>Panics</h2>
<p class="warn">count: {{.Panics.Count}} | last: {{.Panics.At}} — {{.Panics.Message}}</p>
<pre>{{.Panics.Stack}}</pre>{{end}}
</body>
</html>
`))

type statRow struct {
	Key   string
	Value string
}

type statSection struct {
	Title string
	Rows  []statRow
}

type panicInfo struct {
	Count   int64
	Message string
	Stack   string
	At      string
}

type overrideRow struct {
	BuildType            string
	MaxPreloadTotalBytes string
	MaxFileBytes         string
}

// buildOverrideRows merges preload's and maxFile's per-build-type overrides into one sorted,
// display-ready list -- a build type configured in only one of the two still gets a row, with
// "-" for whichever setting it doesn't have.
func buildOverrideRows(preload, maxFile map[string]int64) []overrideRow {
	buildTypes := make(map[string]struct{}, len(preload)+len(maxFile))
	for bt := range preload {
		buildTypes[bt] = struct{}{}
	}

	for bt := range maxFile {
		buildTypes[bt] = struct{}{}
	}

	names := make([]string, 0, len(buildTypes))
	for bt := range buildTypes {
		names = append(names, bt)
	}

	sort.Strings(names)

	rows := make([]overrideRow, 0, len(names))

	for _, bt := range names {
		row := overrideRow{BuildType: bt, MaxPreloadTotalBytes: "-", MaxFileBytes: "-"}
		if v, ok := preload[bt]; ok {
			row.MaxPreloadTotalBytes = byteSize(v)
		}

		if v, ok := maxFile[bt]; ok {
			row.MaxFileBytes = byteSize(v)
		}

		rows = append(rows, row)
	}

	return rows
}

// applyOverrideForm handles a "set-max-preload-total-bytes"/"set-max-file-bytes" status-page form
// submission: build-type is required, bytes is parsed if present (blank clears the override, same
// convention as the JSON settings endpoints). Writes an error response and returns false on any
// problem, so the caller knows not to redirect afterward.
func (h *Handler) applyOverrideForm(rw http.ResponseWriter, r *http.Request, set func(buildType string, bytes int64) error) bool {
	// Already wrapped by Index before dispatching here; repeated so this function is safe to
	// call on its own too, and so gosec's per-function G120 check doesn't need to trust that.
	r.Body = http.MaxBytesReader(rw, r.Body, 4096)

	buildType := strings.TrimSpace(r.FormValue("build-type"))
	if buildType == "" {
		http.Error(rw, "build type is required", http.StatusBadRequest)
		return false
	}

	var bytes int64

	if raw := strings.TrimSpace(r.FormValue("bytes")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(rw, "invalid bytes: "+err.Error(), http.StatusBadRequest)
			return false
		}

		bytes = n
	}

	if err := set(buildType, bytes); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return false
	}

	return true
}

type sessionRow struct {
	Status        string
	Version       string
	Ref           string
	JobURL        string
	BuildType     string
	StartedAt     string
	PreloadSize   string
	PreloadSource string
	PreloadTime   string
	FinalizeSize  string
	FinalizeTime  string
	SessionTime   string
}

// prNumberFromRef extracts a PR number from ref if it looks like a changes-id in this codebase's
// "owner/repo#123" convention (see -changes-id). Returns "" if ref doesn't match -- e.g. it's a
// raw commit hash, which never contains "#".
func prNumberFromRef(ref string) string {
	repo, num, ok := strings.Cut(ref, "#")
	if !ok || !strings.Contains(repo, "/") || num == "" {
		return ""
	}
	if _, err := strconv.Atoi(num); err != nil {
		return ""
	}

	return num
}

// jobURLWithPR appends ?pr=<number> to jobURL when ref carries a PR number, the same query
// param GitHub's own UI adds when you navigate to a run from a PR's checks tab -- it makes the
// run page show a "part of #<number>" link back to the PR.
func jobURLWithPR(jobURL, ref string) string {
	if jobURL == "" {
		return ""
	}

	num := prNumberFromRef(ref)
	if num == "" {
		return jobURL
	}

	sep := "?"
	if strings.Contains(jobURL, "?") {
		sep = "&"
	}

	return jobURL + sep + "pr=" + num
}

// Index serves a Basic-Auth-gated HTML status page at "/" with storage stats and a manual
// cleanup trigger, for operators without easy access to the Bearer-token JSON /status endpoint.
func (h *Handler) Index(rw http.ResponseWriter, r *http.Request) {
	if !h.basicAuthorized(r) {
		rw.Header().Set("WWW-Authenticate", `Basic realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method == http.MethodPost {
		// This page's own forms are a handful of short fields -- cap well above anything they'd
		// ever send, just so a POST here can't be used to exhaust memory via an oversized body.
		r.Body = http.MaxBytesReader(rw, r.Body, 4096)

		switch r.FormValue("action") {
		case "set-max-preload-total-bytes":
			if !h.applyOverrideForm(rw, r, h.setMaxPreloadTotalBytes) {
				return
			}
		case "set-max-file-bytes":
			if !h.applyOverrideForm(rw, r, h.setMaxFileBytes) {
				return
			}
		default:
			if e, ok := h.store.(interface{ EvictNow() }); ok {
				e.EvictNow()
			}
			if h.gocacheStore != nil {
				h.gocacheStore.EvictNow()
			}
			h.enforceCombinedBudget()
		}

		http.Redirect(rw, r, "/", http.StatusSeeOther)
		return
	}

	var sections []statSection
	if s, ok := h.store.(statsProvider); ok {
		if stats := s.Stats(); stats["index"] != "0" {
			sections = append(sections, toSection("Objects store", stats))
		}
	}
	if h.gocacheStore != nil {
		if stats := h.gocacheStore.Stats(); stats["index"] != "0" {
			sections = append(sections, toSection("Native GOCACHE store", stats))
		}
	}

	var combinedBudget string
	if h.combinedMaxDiskBytes > 0 {
		var total int64
		for _, s := range h.diskBudgetStores() {
			total += s.DiskBytes()
		}
		combinedBudget = byteSize(total) + " / " + byteSize(h.combinedMaxDiskBytes)
	}

	var sessions []sessionRow
	for _, cs := range h.clientSessionsSnapshot() {
		sessions = append(sessions, sessionRow{
			Status:        cs.Status,
			Version:       cs.Version,
			Ref:           cs.Ref,
			JobURL:        jobURLWithPR(cs.JobURL, cs.Ref),
			BuildType:     cs.BuildType,
			StartedAt:     cs.StartedAt.Format(time.RFC3339),
			PreloadSize:   byteSize(cs.PreloadBytes),
			PreloadSource: cs.PreloadSource,
			PreloadTime:   cs.PreloadTime.Round(time.Millisecond).String(),
			FinalizeSize:  byteSize(cs.FinalizeBytes),
			FinalizeTime:  cs.FinalizeTime.Round(time.Millisecond).String(),
			SessionTime:   cs.SessionTime.Round(time.Second).String(),
		})
	}

	panicCount, panicMessage, panicStack, panicAt := h.panicSnapshot()
	panics := panicInfo{Count: panicCount, Message: panicMessage, Stack: panicStack}
	if panicCount > 0 {
		panics.At = panicAt.Format(time.RFC3339)
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	serverVersion := version.Module("github.com/vearutop/gocacheprog").Version
	serverVersionURL := "https://github.com/vearutop/gocacheprog"
	if strings.HasPrefix(serverVersion, "v") && strings.Contains(serverVersion, ".") {
		serverVersionURL += "/releases/tag/" + serverVersion
	}

	data := struct {
		ServerVersion    string
		ServerVersionURL string
		HeapInUse        string
		CombinedBudget   string
		Panics           panicInfo
		Sections         []statSection
		Overrides        []overrideRow
		Sessions         []sessionRow
		SessionsJSONL    bool
	}{
		ServerVersion:    serverVersion,
		ServerVersionURL: serverVersionURL,
		HeapInUse:        byteSize(uint64ToInt64(ms.HeapInuse)),
		CombinedBudget:   combinedBudget,
		Panics:           panics,
		Sections:         sections,
		Overrides:        buildOverrideRows(h.maxPreloadTotalBytesSnapshot(), h.maxFileBytesSnapshot()),
		Sessions:         sessions,
		SessionsJSONL:    h.sessionsJSONLPath != "",
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := indexTemplate.Execute(rw, data); err != nil {
		log.Printf("render index page: %s", err.Error())
	}
}

func (h *Handler) basicAuthorized(r *http.Request) bool {
	if h.authToken == "" {
		return true
	}

	_, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	if pass == h.authToken {
		return true
	}

	return h.fallbackAuthToken != "" && pass == h.fallbackAuthToken
}

func toSection(title string, stats map[string]string) statSection {
	augmentStatusStats(stats)

	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([]statRow, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, statRow{Key: k, Value: stats[k]})
	}

	return statSection{Title: title, Rows: rows}
}
