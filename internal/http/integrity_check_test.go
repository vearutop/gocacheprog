package http_test

import (
	"bytes"
	"encoding/json"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func TestIntegrityCheck_DetectsAndRemovesBrokenEntryByDefault(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{
		integrityTestItem("actionId1", "outputId1", "hello-world"),
	}}))
	require.NoError(t, os.WriteFile(localStore.OutputFilename("outputId1"), []byte("short"), 0o600))

	srv := httptest.NewServer(http.NewHandler(localStore, ""))
	t.Cleanup(srv.Close)

	resp, err := nethttp.Get(srv.URL + "/integrity-check")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, nethttp.StatusOK, resp.StatusCode)

	var report cache.IntegrityReport
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&report))
	require.EqualValues(t, 1, report.Checked)
	require.False(t, report.DryRun)
	require.Len(t, report.Broken, 1)
	require.Equal(t, "actionId1", report.Broken[0].ActionID)
	require.True(t, report.Broken[0].Removed)
}

func TestIntegrityCheck_DryRunReportsWithoutRemoving(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{
		integrityTestItem("actionId1", "outputId1", "hello-world"),
	}}))
	require.NoError(t, os.WriteFile(localStore.OutputFilename("outputId1"), []byte("short"), 0o600))

	srv := httptest.NewServer(http.NewHandler(localStore, ""))
	t.Cleanup(srv.Close)

	resp, err := nethttp.Get(srv.URL + "/integrity-check?dry_run=1")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, nethttp.StatusOK, resp.StatusCode)

	var report cache.IntegrityReport
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&report))
	require.True(t, report.DryRun)
	require.Len(t, report.Broken, 1)
	require.False(t, report.Broken[0].Removed)
}

func TestIntegrityCheck_RequiresAuthWhenConfigured(t *testing.T) {
	localStore, err := local.NewStore(t.TempDir())
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandler(localStore, "secret-token"))
	t.Cleanup(srv.Close)

	resp, err := nethttp.Get(srv.URL + "/integrity-check")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, nethttp.StatusUnauthorized, resp.StatusCode)
}

func integrityTestItem(actionID, outputID, body string) cache.ResponseItem {
	now := time.Now()
	item := cache.ResponseItem{
		ActionID: actionID,
		OutputID: outputID,
		Size:     int64(len(body)),
		Time:     &now,
		WireSize: int64(len(body)),
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString(body)), nil
	})

	return item
}
