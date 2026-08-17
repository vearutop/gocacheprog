package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServeHTTP_RecoversFromPanic is a regression test for the actual production incident: an
// unrecovered panic in any request goroutine crashes the whole process, taking down every
// in-flight request across every session, not just the one that triggered it. ServeHTTP must
// recover it, fail only that one request, and make it visible on the status page instead of
// only ever showing up as a crash log an operator might miss.
func TestServeHTTP_RecoversFromPanic(t *testing.T) {
	const testPath = "/__test_panic__"
	routes[testPath] = func(h *Handler, rw http.ResponseWriter, r *http.Request) {
		panic("boom")
	}
	t.Cleanup(func() { delete(routes, testPath) })

	h := &Handler{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + testPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	require.Equal(t, http.StatusInternalServerError, res.StatusCode)

	require.Equal(t, "1", h.Stats()["panics"])

	count, message, stack, at := h.panicSnapshot()
	require.EqualValues(t, 1, count)
	require.Equal(t, "boom", message)
	require.Contains(t, stack, "goroutine")
	require.False(t, at.IsZero())

	// The server itself must still be alive and answering other requests.
	res2, err := http.Get(srv.URL + "/version")
	require.NoError(t, err)
	require.NoError(t, res2.Body.Close())
	require.Equal(t, http.StatusOK, res2.StatusCode)
}

// TestIndex_HidesPanicsSectionWhenEmpty covers the actual ask: a server that's never recovered a
// panic shouldn't show a "Panics" heading and a reassuring "none recovered" line taking up space
// on every single status-page load -- it should just not be there.
func TestIndex_HidesPanicsSectionWhenEmpty(t *testing.T) {
	h := &Handler{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	require.NotContains(t, string(b), "Panics")
}

// TestIndex_ShowsPanicsSectionAtBottomWhenPresent covers the other half: once something has
// actually panicked, the section should appear -- and at the bottom of the page, after the
// client sessions table, not competing with it for the operator's attention at the top.
func TestIndex_ShowsPanicsSectionAtBottomWhenPresent(t *testing.T) {
	const testPath = "/__test_panic_for_index__"
	routes[testPath] = func(h *Handler, rw http.ResponseWriter, r *http.Request) {
		panic("boom")
	}
	t.Cleanup(func() { delete(routes, testPath) })

	h := &Handler{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + testPath)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	res, err = http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	body := string(b)

	require.Contains(t, body, "Panics")
	require.Contains(t, body, "boom")

	sessionsIdx := strings.Index(body, "Client sessions")
	panicsIdx := strings.Index(body, "<h2>Panics</h2>")
	require.NotEqual(t, -1, sessionsIdx)
	require.NotEqual(t, -1, panicsIdx)
	require.Greater(t, panicsIdx, sessionsIdx, "Panics section should render after Client sessions, at the bottom of the page")
}
