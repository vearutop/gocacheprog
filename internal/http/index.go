package http

import (
	"html/template"
	"log"
	"net/http"
	"sort"
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
.idle{color:#888;}
</style>
</head>
<body>
<h1>gocacheprogd status</h1>
<p>server version: {{.ServerVersion}}</p>
{{if .CombinedBudget}}<p>combined disk budget: {{.CombinedBudget}}</p>{{else}}<p class="warn">no combined disk budget configured — eviction disabled</p>{{end}}
{{range .Sections}}
<h2>{{.Title}}</h2>
<table>
{{range .Rows}}<tr><th>{{.Key}}</th><td>{{.Value}}</td></tr>
{{end}}
</table>
{{end}}
<h2>Client sessions</h2>
<table>
<tr><th>session</th><th>status</th><th>version</th><th>pid</th><th>cache dir</th><th>commit</th><th>build type</th><th>first seen</th><th>last seen</th></tr>
{{range .Sessions}}<tr>
<td>{{.SessionID}}</td>
<td>{{if .InProgress}}<span class="active">in progress</span>{{else}}<span class="idle">idle</span>{{end}}</td>
<td>{{.Version}}</td>
<td>{{.PID}}</td>
<td>{{.CacheDir}}</td>
<td>{{.Commit}}</td>
<td>{{.BuildType}}</td>
<td>{{.FirstSeen}}</td>
<td>{{.LastSeen}}</td>
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

type sessionRow struct {
	SessionID  string
	InProgress bool
	Version    string
	PID        string
	CacheDir   string
	Commit     string
	BuildType  string
	FirstSeen  string
	LastSeen   string
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
			SessionID:  cs.SessionID,
			InProgress: cs.InProgress,
			Version:    cs.Version,
			PID:        cs.PID,
			CacheDir:   cs.CacheDir,
			Commit:     cs.Commit,
			BuildType:  cs.BuildType,
			FirstSeen:  cs.FirstSeen.Format(time.RFC3339),
			LastSeen:   cs.LastSeen.Format(time.RFC3339),
		})
	}

	data := struct {
		ServerVersion  string
		CombinedBudget string
		Sections       []statSection
		Sessions       []sessionRow
	}{
		ServerVersion:  version.Module("github.com/vearutop/gocacheprog").Version,
		CombinedBudget: combinedBudget,
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
