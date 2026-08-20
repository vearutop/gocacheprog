package http

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"time"
)

// sessionsJSONLRecord is one line of sessions.jsonl for a session lifecycle event ("started" or
// "done"). Unlike the CSV format this replaced, adding a new field later needs no coordination
// with old rows or existing readers -- each line is self-describing, so a reader that doesn't
// know about a new key simply ignores it, and an old line simply lacks it.
type sessionsJSONLRecord struct {
	Event         string  `json:"event"`
	Timestamp     int64   `json:"timestamp"`
	SessionID     string  `json:"session_id"`
	StartedAt     int64   `json:"started_at"`
	Status        string  `json:"status"`
	Version       string  `json:"version,omitempty"`
	Ref           string  `json:"ref,omitempty"`
	BuildType     string  `json:"build_type,omitempty"`
	JobURL        string  `json:"job_url,omitempty"`
	PreloadBytes  int64   `json:"preload_bytes"`
	PreloadTimeS  float64 `json:"preload_time_s"`
	PreloadSource string  `json:"preload_source,omitempty"`
	FinalizeBytes int64   `json:"finalize_bytes"`
	FinalizeTimeS float64 `json:"finalize_time_s"`
	SessionTimeS  float64 `json:"session_time_s"`
}

// appendSessionsJSONL appends one line to h.sessionsJSONLPath for a session lifecycle event
// ("started" or "done"). The file is opened and closed for this write alone, never held open
// across calls, so nothing else reading or backing it up is blocked by a long-lived handle; it's
// also why this survives restarts and can grow indefinitely without the process caring. A no-op
// if no path was configured (see WithSessionsJSONL). Best-effort: a write failure is logged, not
// propagated -- this is an analytics side channel, not part of the cache's own correctness.
func (h *Handler) appendSessionsJSONL(event, sid string, cs clientSession) {
	if h.sessionsJSONLPath == "" {
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

	record := sessionsJSONLRecord{
		Event:         event,
		Timestamp:     time.Now().Unix(),
		SessionID:     sid,
		StartedAt:     cs.FirstSeen.Unix(),
		Status:        status,
		Version:       cs.Version,
		Ref:           sessionRef(cs),
		BuildType:     cs.BuildType,
		JobURL:        cs.JobURL,
		PreloadBytes:  cs.PreloadBytes,
		PreloadTimeS:  roundSeconds(cs.PreloadTime),
		PreloadSource: cs.PreloadSource,
		FinalizeBytes: cs.FinalizeBytes,
		FinalizeTimeS: roundSeconds(cs.FinalizeTime),
		SessionTimeS:  roundSeconds(sessionTime),
	}

	if err := appendJSONLLine(h.sessionsJSONLPath, record, cs.Extra); err != nil {
		log.Printf("append sessions.jsonl: %s", err.Error())
	}
}

// roundSeconds renders d in plain fractional seconds, rounded to millisecond precision, matching
// the resolution sessions.jsonl (and its CSV predecessor) has always reported at.
func roundSeconds(d time.Duration) float64 {
	return math.Round(d.Seconds()*1000) / 1000
}

// appendJSONLLine appends v (marshaled to a JSON object) to path, merged with extra's keys as
// additional top-level fields -- a key in extra that collides with one of v's own fields is
// dropped rather than overwriting it, so a caller's report_<name> can never clobber a fixed
// field like "event" or "session_id" out from under it.
func appendJSONLLine(path string, v any, extra map[string]any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	if len(extra) > 0 {
		var merged map[string]any
		if err := json.Unmarshal(data, &merged); err != nil {
			return err
		}
		for k, val := range extra {
			if _, reserved := merged[k]; reserved {
				continue
			}
			merged[k] = val
		}
		data, err = json.Marshal(merged)
		if err != nil {
			return err
		}
	}

	data = append(data, '\n')

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // path is operator-configured, not request-derived.
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close sessions.jsonl: %s", closeErr.Error())
		}
	}()

	_, err = f.Write(data)
	return err
}

// SessionsJSONL serves the raw sessions.jsonl file for download, Basic-Auth-gated the same way as
// the "/" status page.
func (h *Handler) SessionsJSONL(rw http.ResponseWriter, r *http.Request) {
	if !h.basicAuthorized(r) {
		rw.Header().Set("WWW-Authenticate", `Basic realm="gocacheprogd"`)
		http.Error(rw, "unauthorized", http.StatusUnauthorized)
		return
	}

	if h.sessionsJSONLPath == "" {
		http.Error(rw, "sessions.jsonl is not enabled", http.StatusNotFound)
		return
	}

	rw.Header().Set("Content-Type", "application/x-ndjson")
	http.ServeFile(rw, r, h.sessionsJSONLPath)
}
