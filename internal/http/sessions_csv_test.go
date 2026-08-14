package http_test

import (
	"bytes"
	"encoding/csv"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

// TestSessionsCSV_RecordsStartedAndDoneEvents covers the actual point of sessions.csv: a row
// appended on session start and another on session done, surviving as a flat file an operator
// can pull down and analyze later -- not just the in-memory status-page view, which resets on
// restart.
func TestSessionsCSV_RecordsStartedAndDoneEvents(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "sessions.csv")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsCSV(csvPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{
		SessionID: "session-csv-1",
		Params:    local.ProxyParams{ChangesID: "acme/widgets#1", BuildType: "unit"},
	})
	require.NoError(t, err)

	item := cache.ResponseItem{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("hello")), nil
	})
	require.NoError(t, client.Put(cache.Response{Items: []cache.ResponseItem{item}}))
	require.NoError(t, client.MarkSessionDone())

	f, err := os.Open(csvPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3, "header + started + done, got: %v", rows)

	header := rows[0]
	col := func(row []string, name string) string {
		for i, h := range header {
			if h == name {
				return row[i]
			}
		}
		t.Fatalf("column %q not found in header %v", name, header)
		return ""
	}

	started, done := rows[1], rows[2]
	require.Equal(t, "started", col(started, "event"))
	require.Equal(t, "session-csv-1", col(started, "session_id"))
	require.Equal(t, "acme/widgets#1", col(started, "ref"))
	require.Equal(t, "unit", col(started, "build_type"))

	require.Equal(t, "done", col(done, "event"))
	require.Equal(t, "session-csv-1", col(done, "session_id"))
	require.Equal(t, "done", col(done, "status"))

	// Plain units, not human-formatted strings: int unix timestamps and fractional seconds, so
	// a spreadsheet or analysis script can treat every numeric column as a plain number.
	_, err = strconv.ParseInt(col(started, "timestamp"), 10, 64)
	require.NoError(t, err, "timestamp should be a plain unix timestamp")
	_, err = strconv.ParseInt(col(started, "started_at"), 10, 64)
	require.NoError(t, err, "started_at should be a plain unix timestamp")
	_, err = strconv.ParseFloat(col(done, "session_time_s"), 64)
	require.NoError(t, err, "session_time_s should be a plain number of seconds")
	_, err = strconv.ParseFloat(col(done, "preload_time_s"), 64)
	require.NoError(t, err, "preload_time_s should be a plain number of seconds")
	_, err = strconv.ParseFloat(col(done, "finalize_time_s"), 64)
	require.NoError(t, err, "finalize_time_s should be a plain number of seconds")
	_, err = strconv.ParseInt(col(done, "preload_bytes"), 10, 64)
	require.NoError(t, err, "preload_bytes should be a plain byte count")
}

// TestSessionsCSV_SurvivesAcrossHandlerRestarts covers the "survives restarts" requirement: a
// second Handler pointed at the same path must append after what an earlier one already wrote,
// never truncating it.
func TestSessionsCSV_SurvivesAcrossHandlerRestarts(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "sessions.csv")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	firstStoreDir := t.TempDir()
	firstStore, err := local.NewStore(firstStoreDir)
	require.NoError(t, err)
	h1 := http.NewHandlerWithPreloadLimit(firstStore, nil, "", "", 2, http.WithSessionsCSV(csvPath))
	srv1 := httptest.NewServer(h1)
	client1, err := http.NewClientWithSession(srv1.URL, "", &http.SessionInfo{SessionID: "session-a"})
	require.NoError(t, err)
	require.NoError(t, client1.MarkSessionDone())
	srv1.Close()

	// A brand new Handler/server, same csvPath -- simulating a process restart.
	h2 := http.NewHandlerWithPreloadLimit(localStore, nil, "", "", 2, http.WithSessionsCSV(csvPath))
	srv2 := httptest.NewServer(h2)
	t.Cleanup(srv2.Close)
	client2, err := http.NewClientWithSession(srv2.URL, "", &http.SessionInfo{SessionID: "session-b"})
	require.NoError(t, err)
	require.NoError(t, client2.MarkSessionDone())

	f, err := os.Open(csvPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)

	var sessionIDs []string
	for _, row := range rows[1:] {
		sessionIDs = append(sessionIDs, row[2])
	}
	require.Contains(t, sessionIDs, "session-a", "rows from before the restart must survive")
	require.Contains(t, sessionIDs, "session-b")
}

func TestSessionsCSV_DisabledByDefault(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := nethttp.Get(srv.URL + "/sessions.csv")
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, nethttp.StatusNotFound, res.StatusCode)
}

func TestSessionsCSV_DownloadRequiresBasicAuth(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "sessions.csv")

	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, nil, "secret-token", "", 2, http.WithSessionsCSV(csvPath))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClientWithSession(srv.URL, "secret-token", &http.SessionInfo{SessionID: "session-x"})
	require.NoError(t, err)
	require.NoError(t, client.MarkSessionDone())

	req, err := nethttp.NewRequest(nethttp.MethodGet, srv.URL+"/sessions.csv", nil)
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
