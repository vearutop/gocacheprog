package http

import (
	"html/template"
	"log"
	"net/http"
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
<p>server version: {{.ServerVersion}}</p>
{{if .CombinedBudget}}<p>combined disk budget: {{.CombinedBudget}}</p>{{else}}<p class="warn">no combined disk budget configured — eviction disabled</p>{{end}}
<h2>Panics</h2>
{{if .Panics.Count}}<p class="warn">count: {{.Panics.Count}} | last: {{.Panics.At}} — {{.Panics.Message}}</p>
<pre>{{.Panics.Stack}}</pre>{{else}}<p>none recovered</p>{{end}}
{{range .Sections}}
<h2>{{.Title}}</h2>
<table>
{{range .Rows}}<tr><th>{{.Key}}</th><td>{{.Value}}</td></tr>
{{end}}
</table>
{{end}}
<h2>Client sessions</h2>
<table>
<tr><th>status</th><th>version</th><th>ref</th><th>build type</th><th>preload size</th><th>preload source</th><th>preload time</th><th>finalize size</th><th>finalize time</th><th>session time</th></tr>
{{range .Sessions}}<tr>
<td>{{if eq .Status "done"}}<span class="done">done</span>{{else if eq .Status "in progress"}}<span class="active">in progress</span>{{else}}<span class="idle">idle</span>{{end}}</td>
<td>{{.Version}}</td>
<td>{{if .JobURL}}<a href="{{.JobURL}}" target="_blank" rel="noopener noreferrer">{{.Ref}}</a>{{else}}{{.Ref}}{{end}}</td>
<td>{{.BuildType}}</td>
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
<button type="submit">Run cleanup now</button>
</form>
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

type sessionRow struct {
	Status        string
	Version       string
	Ref           string
	JobURL        string
	BuildType     string
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
		if e, ok := h.store.(interface{ EvictNow() }); ok {
			e.EvictNow()
		}
		if h.gocacheStore != nil {
			h.gocacheStore.EvictNow()
		}
		h.enforceCombinedBudget()
		http.Redirect(rw, r, "/", http.StatusSeeOther)
		return
	}

	var sections []statSection
	if s, ok := h.store.(statsProvider); ok {
		sections = append(sections, toSection("Objects store", s.Stats()))
	}
	if h.gocacheStore != nil {
		sections = append(sections, toSection("Native GOCACHE store", h.gocacheStore.Stats()))
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

	data := struct {
		ServerVersion  string
		CombinedBudget string
		Panics         panicInfo
		Sections       []statSection
		Sessions       []sessionRow
	}{
		ServerVersion:  version.Module("github.com/vearutop/gocacheprog").Version,
		CombinedBudget: combinedBudget,
		Panics:         panics,
		Sections:       sections,
		Sessions:       sessions,
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
