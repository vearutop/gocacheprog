package http_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

// readJSONLRecords parses path as newline-delimited JSON objects, one map per line.
func readJSONLRecords(t *testing.T, path string) []map[string]any {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	var records []map[string]any
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())

	return records
}

// TestSessionsJSONL_RecordsStartedAndDoneEvents covers the actual point of sessions.jsonl: a line
// appended on session start and another on session done, surviving as a flat file an operator
// can pull down and analyze later -- not just the in-memory status-page view, which resets on
// restart.
func TestSessionsJSONL_RecordsStartedAndDoneEvents(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "sessions.jsonl")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{
		SessionID: "session-jsonl-1",
		Params:    local.ProxyParams{ChangesID: "acme/widgets#1", BuildType: "unit"},
	})
	require.NoError(t, err)

	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))
	require.NoError(t, client.MarkSessionDone(nil))

	records := readJSONLRecords(t, jsonlPath)
	require.Len(t, records, 2, "started + done, got: %v", records)

	started, done := records[0], records[1]
	require.Equal(t, "started", started["event"])
	require.Equal(t, "session-jsonl-1", started["session_id"])
	require.Equal(t, "acme/widgets#1", started["ref"])
	require.Equal(t, "unit", started["build_type"])

	require.Equal(t, "done", done["event"])
	require.Equal(t, "session-jsonl-1", done["session_id"])
	require.Equal(t, "done", done["status"])

	// Plain JSON numbers, not human-formatted strings: unix timestamps and fractional seconds,
	// so an analysis script can treat every numeric field as a plain number without parsing.
	_, ok := started["timestamp"].(float64)
	require.True(t, ok, "timestamp should be a plain unix timestamp, got: %v", started["timestamp"])
	_, ok = started["started_at"].(float64)
	require.True(t, ok, "started_at should be a plain unix timestamp, got: %v", started["started_at"])
	_, ok = done["session_time_s"].(float64)
	require.True(t, ok, "session_time_s should be a plain number of seconds, got: %v", done["session_time_s"])
	_, ok = done["preload_time_s"].(float64)
	require.True(t, ok, "preload_time_s should be a plain number of seconds, got: %v", done["preload_time_s"])
	_, ok = done["finalize_time_s"].(float64)
	require.True(t, ok, "finalize_time_s should be a plain number of seconds, got: %v", done["finalize_time_s"])
	_, ok = done["preload_bytes"].(float64)
	require.True(t, ok, "preload_bytes should be a plain byte count, got: %v", done["preload_bytes"])
}

// TestSessionsJSONL_MarkSessionDoneExtraFieldsAreMerged covers passing extra data through
// MarkSessionDone (e.g. -github-actions-done's save-cache skip counts and report_<name> file
// contents): it must land as additional top-level fields on the "done" line, both a nested JSON
// value and a plain string.
func TestSessionsJSONL_MarkSessionDoneExtraFieldsAreMerged(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "sessions.jsonl")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{SessionID: "session-extra-1"})
	require.NoError(t, err)

	require.NoError(t, client.MarkSessionDone(map[string]any{
		"unit_total":  map[string]any{"foo": "bar"},
		"other_stats": "abcde",
	}))

	records := readJSONLRecords(t, jsonlPath)
	require.Len(t, records, 2, "started + done, got: %v", records)

	done := records[1]
	require.Equal(t, "done", done["event"])
	require.Equal(t, map[string]any{"foo": "bar"}, done["unit_total"])
	require.Equal(t, "abcde", done["other_stats"])
}

// TestSessionsJSONL_MarkSessionDoneExtraCannotOverrideFixedFields covers the safety rail: an
// extra field colliding with one of sessions.jsonl's own fixed fields (like "event") must be
// dropped, not silently corrupt the line.
func TestSessionsJSONL_MarkSessionDoneExtraCannotOverrideFixedFields(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "sessions.jsonl")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{SessionID: "session-extra-2"})
	require.NoError(t, err)

	require.NoError(t, client.MarkSessionDone(map[string]any{"event": "hijacked", "session_id": "hijacked"}))

	records := readJSONLRecords(t, jsonlPath)
	require.Len(t, records, 2, "started + done, got: %v", records)

	done := records[1]
	require.Equal(t, "done", done["event"], "a reserved field name in extra must not override the real value")
	require.Equal(t, "session-extra-2", done["session_id"])
}

// TestSessionsJSONL_SurvivesAcrossHandlerRestarts covers the "survives restarts" requirement: a
// second Handler pointed at the same path must append after what an earlier one already wrote,
// never truncating it.
func TestSessionsJSONL_SurvivesAcrossHandlerRestarts(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "sessions.jsonl")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	firstStoreDir := t.TempDir()
	firstStore, err := local.NewStore(firstStoreDir)
	require.NoError(t, err)
	h1 := http.NewHandlerWithPreloadLimit(firstStore, nil, "", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv1 := httptest.NewServer(h1)
	client1, err := http.NewClientWithSession(srv1.URL, "", &http.SessionInfo{SessionID: "session-a"})
	require.NoError(t, err)
	require.NoError(t, client1.MarkSessionDone(nil))
	srv1.Close()

	// A brand new Handler/server, same jsonlPath -- simulating a process restart.
	h2 := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv2 := httptest.NewServer(h2)
	t.Cleanup(srv2.Close)
	client2, err := http.NewClientWithSession(srv2.URL, "", &http.SessionInfo{SessionID: "session-b"})
	require.NoError(t, err)
	require.NoError(t, client2.MarkSessionDone(nil))

	records := readJSONLRecords(t, jsonlPath)

	var sessionIDs []string
	for _, record := range records {
		sid, ok := record["session_id"].(string)
		require.True(t, ok)
		sessionIDs = append(sessionIDs, sid)
	}
	require.Contains(t, sessionIDs, "session-a", "rows from before the restart must survive")
	require.Contains(t, sessionIDs, "session-b")
}

func TestSessionsJSONL_DisabledByDefault(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := nethttp.Get(srv.URL + "/sessions.jsonl")
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, nethttp.StatusNotFound, res.StatusCode)
}

func TestSessionsJSONL_DownloadRequiresBasicAuth(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "sessions.jsonl")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "secret-token", "", 2, http.WithSessionsJSONL(jsonlPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "secret-token", &http.SessionInfo{SessionID: "session-x"})
	require.NoError(t, err)
	require.NoError(t, client.MarkSessionDone(nil))

	req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/sessions.jsonl", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	require.Equal(t, nethttp.StatusUnauthorized, res.StatusCode)

	req.SetBasicAuth("anyone", "secret-token")
	res, err = nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, nethttp.StatusOK, res.StatusCode)

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(b), "session-x")
}

func TestIndex_ShowsStartedAt(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{SessionID: "session-started-at"})
	require.NoError(t, err)

	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))

	req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/", nil)
	require.NoError(t, err)
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	body := string(b)

	require.Contains(t, body, "<th>started at</th>")
	require.True(t, strings.Contains(body, "T"), "expected an RFC3339-ish timestamp in the started at column, got body: %s", body)
}
