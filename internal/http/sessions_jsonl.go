package http

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"maps"
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

// sessionsJSONLTailBytes bounds how much of sessions.jsonl loadRecentSessions reads on startup --
// the file can grow indefinitely, so it seeks this far back from the end instead of reading it
// whole. Comfortably covers sessionRetention worth of history for any realistic session rate.
const sessionsJSONLTailBytes = 500_000

// loadRecentSessions best-effort repopulates h.clientSessions from the tail of sessionsJSONLPath
// on startup, so the status page still shows recently active sessions across a restart instead of
// going blank. Reads only the last sessionsJSONLTailBytes of the file (it can grow indefinitely)
// starting at the next line boundary after the seek point, so a line split by the seek is skipped
// rather than misparsed. Any error -- missing file, seek/read failure, a malformed line -- is
// logged (or silently skipped, for a single bad line) and otherwise ignored: this is a
// best-effort convenience, never a reason to fail startup.
func (h *Handler) loadRecentSessions() {
	if h.sessionsJSONLPath == "" {
		return
	}

	f, err := os.Open(h.sessionsJSONLPath) //nolint:gosec // path is operator-configured, not request-derived.
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("load sessions.jsonl: %s", err.Error())
		}
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close sessions.jsonl: %s", closeErr.Error())
		}
	}()

	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		log.Printf("load sessions.jsonl: %s", err.Error())
		return
	}

	offset := int64(0)
	if size > sessionsJSONLTailBytes {
		offset = size - sessionsJSONLTailBytes
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		log.Printf("load sessions.jsonl: %s", err.Error())
		return
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	if offset > 0 {
		// The seek almost certainly landed mid-line; that first (partial) line is unparseable and
		// must be discarded, not attributed to the wrong session.
		scanner.Scan()
	}

	now := time.Now()
	sessions := make(map[string]*clientSession)

	for scanner.Scan() {
		var rec sessionsJSONLRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil || rec.SessionID == "" {
			continue
		}

		cs := &clientSession{
			Version:       rec.Version,
			ChangesID:     rec.Ref,
			BuildType:     rec.BuildType,
			JobURL:        rec.JobURL,
			PreloadBytes:  rec.PreloadBytes,
			PreloadTime:   secondsToDuration(rec.PreloadTimeS),
			PreloadSource: rec.PreloadSource,
			FinalizeBytes: rec.FinalizeBytes,
			FinalizeTime:  secondsToDuration(rec.FinalizeTimeS),
			FirstSeen:     time.Unix(rec.StartedAt, 0),
			LastSeen:      time.Unix(rec.Timestamp, 0),
		}
		if rec.Status == "done" {
			cs.Done = true
			cs.DoneAt = time.Unix(rec.Timestamp, 0)
		}

		sessions[rec.SessionID] = cs
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		log.Printf("load sessions.jsonl: %s", err.Error())
	}

	for id, cs := range sessions {
		if sessionExpired(cs, now) {
			delete(sessions, id)
		}
	}

	h.clientSessionsMu.Lock()
	maps.Copy(h.clientSessions, sessions)
	h.clientSessionsMu.Unlock()
}

// secondsToDuration is the inverse of roundSeconds, for reconstructing a clientSession's
// durations from a parsed sessions.jsonl record.
func secondsToDuration(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
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
