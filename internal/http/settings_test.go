package http

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/gocache"
)

func responseItemForTest(actionID, body string) cache.ResponseItem {
	item := cache.ResponseItem{
		ActionID: actionID,
		OutputID: actionID + "-output",
		Size:     int64(len(body)),
		WireSize: int64(len(body)),
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString(body)), nil
	})

	return item
}

func TestTrimToPreloadBudget_DisabledReturnsUnchanged(t *testing.T) {
	items := []cache.ResponseItem{responseItemForTest("a", "12345"), responseItemForTest("b", "1234567890")}
	require.Equal(t, items, trimToPreloadBudget(items, 0))
	require.Equal(t, items, trimToPreloadBudget(items, -1))
}

func TestTrimToPreloadBudget_AlreadyUnderBudgetReturnsUnchanged(t *testing.T) {
	items := []cache.ResponseItem{responseItemForTest("a", "12345"), responseItemForTest("b", "1234567890")}
	require.Equal(t, items, trimToPreloadBudget(items, 1000))
}

// TestTrimToPreloadBudget_DropsLargestFirstPreservingOrder covers the actual point of this
// function: when the total exceeds budget, the largest items are dropped first, and whatever
// survives keeps its original relative order (not sorted-by-size order).
func TestTrimToPreloadBudget_DropsLargestFirstPreservingOrder(t *testing.T) {
	items := []cache.ResponseItem{
		responseItemForTest("small-1", "12"),      // 2 bytes
		responseItemForTest("huge", "1234567890"), // 10 bytes
		responseItemForTest("small-2", "34"),      // 2 bytes
		responseItemForTest("medium", "12345"),    // 5 bytes
	}

	// Budget fits everything except "huge": 2+2+5 = 9 <= 10, but adding huge's 10 pushes it to
	// 19 > 10.
	got := trimToPreloadBudget(items, 10)

	var gotIDs []string
	for _, item := range got {
		gotIDs = append(gotIDs, item.ActionID)
	}
	require.Equal(t, []string{"small-1", "small-2", "medium"}, gotIDs, "huge must be dropped, survivors keep original order")
}

func TestTrimToPreloadBudget_DropsMultipleLargestUntilItFits(t *testing.T) {
	items := []cache.ResponseItem{
		responseItemForTest("keep", "12"),           // 2 bytes
		responseItemForTest("drop-1", "1234567890"), // 10 bytes
		responseItemForTest("drop-2", "123456789"),  // 9 bytes
	}

	got := trimToPreloadBudget(items, 5)

	require.Len(t, got, 1)
	require.Equal(t, "keep", got[0].ActionID)
}

// TestTrimToPreloadBudget_PrefersRecentBucketOverSizeWithinOlderBucket covers the point of the
// bucketing added on top of the plain "biggest first" rule: an old-but-small item is dropped
// before a same-bucket comparison ever reaches a new-but-huge item, because the old item's whole
// (older) time bucket is ranked for removal ahead of the current bucket entirely.
func TestTrimToPreloadBudget_PrefersRecentBucketOverSizeWithinOlderBucket(t *testing.T) {
	now := time.Now()
	old := now.Add(-2 * time.Hour)

	oldSmall := responseItemForTest("old-small", "12")       // 2 bytes, old bucket
	newHuge := responseItemForTest("new-huge", "1234567890") // 10 bytes, current bucket
	newSmall := responseItemForTest("new-small", "34")       // 2 bytes, current bucket
	oldSmall.Time = &old
	newHuge.Time = &now
	newSmall.Time = &now

	// Total 14 > budget 10: dropping just "new-huge" (the plain-size-desc pick) would already
	// fit (14-10=4<=10) and keep "old-small". Bucketing should drop "old-small" first instead
	// (older bucket ranked ahead of the current one), then still need to drop "new-huge" too
	// (14-2-10=2<=10) since removing the 2-byte old item alone isn't enough.
	got := trimToPreloadBudget([]cache.ResponseItem{oldSmall, newHuge, newSmall}, 10)

	require.Len(t, got, 1)
	require.Equal(t, "new-small", got[0].ActionID)
}

func TestTrimToPreloadBudget_FallsBackToSizeWhenWireSizeUnset(t *testing.T) {
	big := cache.ResponseItem{ActionID: "big", Size: 100}
	small := cache.ResponseItem{ActionID: "small", Size: 5}

	got := trimToPreloadBudget([]cache.ResponseItem{big, small}, 10)
	require.Len(t, got, 1)
	require.Equal(t, "small", got[0].ActionID)
}

func TestPreloadLimitBytesFor_DefaultsToZero(t *testing.T) {
	h := NewHandler(nil, "")
	require.Equal(t, int64(0), h.preloadLimitBytesFor("unit"))
}

func TestSetPreloadLimitBytes_RoundTripsInMemory(t *testing.T) {
	h := NewHandler(nil, "")

	require.NoError(t, h.setPreloadLimitBytes("unit", 500))
	require.Equal(t, int64(500), h.preloadLimitBytesFor("unit"))
	require.Equal(t, int64(0), h.preloadLimitBytesFor("other"), "unrelated build type must be unaffected")

	require.NoError(t, h.setPreloadLimitBytes("unit", 0))
	require.Equal(t, int64(0), h.preloadLimitBytesFor("unit"), "0 must clear the override")
}

// TestSettings_SurviveRestart is a regression test for the actual point of settings.json: a
// second Handler pointed at the same settingsPath must pick up whatever an earlier one wrote.
func TestSettings_SurviveRestart(t *testing.T) {
	dir := t.TempDir()
	settingsPath := filepath.Join(dir, "settings.json")

	h1 := NewHandlerWithPreloadLimit(nil, nil, "", "", 2, WithSettingsPath(settingsPath))
	require.NoError(t, h1.setPreloadLimitBytes("unit", 500_000_000))

	h2 := NewHandlerWithPreloadLimit(nil, nil, "", "", 2, WithSettingsPath(settingsPath))
	require.Equal(t, int64(500_000_000), h2.preloadLimitBytesFor("unit"), "a fresh Handler must load the previous one's persisted settings")
}

func TestSettings_NoPathConfiguredStillWorksInMemory(t *testing.T) {
	h := NewHandler(nil, "")
	require.NoError(t, h.setPreloadLimitBytes("unit", 42))
	require.Equal(t, int64(42), h.preloadLimitBytesFor("unit"))
}

func TestSettings_MissingFileIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	h := NewHandlerWithPreloadLimit(nil, nil, "", "", 2, WithSettingsPath(filepath.Join(dir, "does-not-exist.json")))
	require.Equal(t, int64(0), h.preloadLimitBytesFor("unit"))
}

func TestPreloadLimitBytesSettings_GetAndPost(t *testing.T) {
	h := NewHandler(nil, "")

	get := func() map[string]int64 {
		rw := httptest.NewRecorder()
		h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodGet, "/settings/preload-limit-bytes", nil))
		require.Equal(t, http.StatusOK, rw.Code)

		var got map[string]int64
		require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &got))
		return got
	}

	require.Empty(t, get())

	rw := httptest.NewRecorder()
	h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodPost, "/settings/preload-limit-bytes?build-type=unit&bytes=1000", nil))
	require.Equal(t, http.StatusNoContent, rw.Code)

	require.Equal(t, map[string]int64{"unit": 1000}, get())

	rw = httptest.NewRecorder()
	h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodPost, "/settings/preload-limit-bytes?build-type=unit&bytes=0", nil))
	require.Equal(t, http.StatusNoContent, rw.Code)

	require.Empty(t, get(), "bytes=0 must clear the override")
}

func TestPreloadLimitBytesSettings_MissingBuildTypeIsBadRequest(t *testing.T) {
	h := NewHandler(nil, "")

	rw := httptest.NewRecorder()
	h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodPost, "/settings/preload-limit-bytes?bytes=1000", nil))
	require.Equal(t, http.StatusBadRequest, rw.Code)
}

func TestPreloadLimitBytesSettings_InvalidBytesIsBadRequest(t *testing.T) {
	h := NewHandler(nil, "")

	rw := httptest.NewRecorder()
	h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodPost, "/settings/preload-limit-bytes?build-type=unit&bytes=not-a-number", nil))
	require.Equal(t, http.StatusBadRequest, rw.Code)
}

func TestPreloadLimitBytesSettings_MethodNotAllowed(t *testing.T) {
	h := NewHandler(nil, "")

	rw := httptest.NewRecorder()
	h.PreloadLimitBytesSettings(rw, httptest.NewRequest(http.MethodDelete, "/settings/preload-limit-bytes", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rw.Code)
}

// TestRestoreCache_ServerDefaultAppliesWhenClientOmitsLimit and
// TestRestoreCache_ClientLimitTakesPrecedenceOverServerDefault cover the actual precedence rule:
// a client-supplied -restore-limit-bytes always wins over the server-side per-build-type default,
// which only kicks in when the client didn't send one at all.
func TestRestoreCache_ServerDefaultAppliesWhenClientOmitsLimit(t *testing.T) {
	dir := t.TempDir()
	store, err := gocache.NewStore(dir)
	require.NoError(t, err)

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}

	// Saved in this order so "small" ends up the more recently uploaded of the two -- under
	// selectRestoreEntries' recency-first, smaller-tiebreak ordering, that's what must survive a
	// tight budget that can't fit both.
	saveRestoreItemForTest(t, store, req, "large", "1234567890") // 10 bytes
	time.Sleep(2 * time.Millisecond)
	saveRestoreItemForTest(t, store, req, "small", "12") // 2 bytes

	h := NewHandlerWithPreloadLimit(nil, store, "", "", 2)
	require.NoError(t, h.setPreloadLimitBytes("unit", 5))

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := NewClient(srv.URL, "")
	require.NoError(t, err)

	var restored []string
	stats, err := client.RestoreCache(req, func(item gocache.FileItem, body io.Reader) error {
		restored = append(restored, item.Path)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"small"}, restored, "server default (5 bytes) must trim to only the smaller, more recent item")
	require.EqualValues(t, len(restored), stats.Files)
}

func TestRestoreCache_ClientLimitTakesPrecedenceOverServerDefault(t *testing.T) {
	dir := t.TempDir()
	store, err := gocache.NewStore(dir)
	require.NoError(t, err)

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}

	saveRestoreItemForTest(t, store, req, "large", "1234567890") // 10 bytes
	time.Sleep(2 * time.Millisecond)
	saveRestoreItemForTest(t, store, req, "small", "12") // 2 bytes

	h := NewHandlerWithPreloadLimit(nil, store, "", "", 2)
	require.NoError(t, h.setPreloadLimitBytes("unit", 5)) // would trim to 1 item if it applied

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := NewClient(srv.URL, "")
	require.NoError(t, err)

	reqWithLimit := req
	reqWithLimit.RestoreLimitBytes = 100 // client's own, generous budget

	var restored []string
	stats, err := client.RestoreCache(reqWithLimit, func(item gocache.FileItem, body io.Reader) error {
		restored = append(restored, item.Path)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"large", "small"}, restored, "client's own limit must win over the smaller server default")
	require.EqualValues(t, len(restored), stats.Files)
}

func saveRestoreItemForTest(t *testing.T, store *gocache.Store, req gocache.Request, path, body string) {
	t.Helper()

	item := gocache.FileItem{Path: path, Size: int64(len(body)), WireSize: int64(len(body))}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte(body))), nil
	})
	require.NoError(t, store.Save(req, gocache.Batch{Items: []gocache.FileItem{item}}))
}
