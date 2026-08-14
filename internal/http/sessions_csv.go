package http

import (
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// sessionsCSVHeader is sessions.csv's fixed column order (see appendSessionsCSV). Keep changes
// additive-only -- append new columns at the end -- since the file is meant to be read back and
// analyzed across the lifetime of many server versions; reordering would silently break an
// existing analysis script that indexes columns by position.
var sessionsCSVHeader = []string{
	"event", "timestamp", "session_id", "started_at",
	"status", "version", "ref", "build_type", "job_url",
	"preload_bytes", "preload_time_s", "preload_source",
	"finalize_bytes", "finalize_time_s",
	"session_time_s",
}

// appendSessionsCSV appends one row to h.sessionsCSVPath for a session lifecycle event
// ("started" or "done"). The file is opened and closed for this write alone, never held open
// across calls, so nothing else reading or backing it up is blocked by a long-lived handle; it's
// also why this survives restarts and can grow indefinitely without the process caring. A no-op
// if no path was configured (see WithSessionsCSV). Best-effort: a write failure is logged, not
// propagated -- this is an analytics side channel, not part of the cache's own correctness.
func (h *Handler) appendSessionsCSV(event, sid string, cs clientSession) {
	if h.sessionsCSVPath == "" {
		return
	}

	status := "in progress"
	if cs.Done {
		status = "done"
	}

	var sessionTime time.Duration
	if cs.Done {
		sessionTime = cs.DoneAt.Sub(cs.FirstSeen)
	}

	row := []string{
		event,
		strconv.FormatInt(time.Now().Unix(), 10),
		sid,
		strconv.FormatInt(cs.FirstSeen.Unix(), 10),
		status,
		cs.Version,
		sessionRef(cs),
		cs.BuildType,
		cs.JobURL,
		strconv.FormatInt(cs.PreloadBytes, 10),
		formatSeconds(cs.PreloadTime),
		cs.PreloadSource,
		strconv.FormatInt(cs.FinalizeBytes, 10),
		formatSeconds(cs.FinalizeTime),
		formatSeconds(sessionTime),
	}

	if err := appendCSVRow(h.sessionsCSVPath, row); err != nil {
		log.Printf("append sessions.csv: %s", err.Error())
	}
}

// formatSeconds renders d in plain fractional seconds (millisecond precision), not a
// unit-suffixed Duration string, so a spreadsheet or analysis script can treat the column as a
// plain number.
func formatSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 3, 64)
}

func appendCSVRow(path string, row []string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // path is operator-configured, not request-derived.
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close sessions.csv: %s", closeErr.Error())
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	if info.Size() == 0 {
		if err := w.Write(sessionsCSVHeader); err != nil {
			return err
		}
	}
	if err := w.Write(row); err != nil {
		return err
	}
	w.Flush()

	return w.Error()
}

// SessionsCSV serves the raw sessions.csv file for download, Basic-Auth-gated the same way as
// the "/" status page.
func (h *Handler) SessionsCSV(rw http.ResponseWriter, r *http.Request) {
	if !h.basicAuthorized(r) {
		rw.Header().Set("WWW-Authenticate", `Basic realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

	if h.sessionsCSVPath == "" {
		http.Error(rw, "sessions.csv is not enabled", http.StatusNotFound)
		return
	}

	rw.Header().Set("Content-Type", "text/csv")
	http.ServeFile(rw, r, h.sessionsCSVPath)
}
