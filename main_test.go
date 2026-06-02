package main

import (
	"encoding/binary"
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/local"
)

func TestParseProxyParams_FlagParseUpdatesReturnedStruct(t *testing.T) {
	oldCommandLine := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
	})

	params := parseProxyParams()

	err := flag.CommandLine.Parse([]string{
		"-preload",
		"-skip-preload",
		"-preload-size", "3000000",
		"-commit", "commit123",
		"-changes-id", "repo/pr-123",
		"-build-type", "lint",
		"-base-commit", "base123",
		"-parent-commit", "parent123",
	})
	require.NoError(t, err)

	require.True(t, params.Preload)
	require.True(t, params.SkipPreload)
	require.Equal(t, int64(3000000), params.PreloadSize)
	require.Equal(t, "commit123", params.Commit)
	require.Equal(t, "repo/pr-123", params.ChangesID)
	require.Equal(t, "lint", params.BuildType)
	require.Equal(t, "base123", params.BaseCommit)
	require.Equal(t, "parent123", params.ParentCommit)
}

func TestRunNativeGOCACHEMode_SendsSessionHeadersOnVersionProbe(t *testing.T) {
	var gotHeaders map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			gotHeaders = map[string]string{
				"session_id": r.Header.Get("X-Gocacheprog-Session-Id"),
				"started_at": r.Header.Get("X-Gocacheprog-Started-At"),
				"pid":        r.Header.Get("X-Gocacheprog-Pid"),
				"cache_dir":  r.Header.Get("X-Gocacheprog-Cache-Dir"),
				"commit":     r.Header.Get("X-Gocacheprog-Commit"),
				"parent":     r.Header.Get("X-Gocacheprog-Parent"),
				"changes":    r.Header.Get("X-Gocacheprog-Changes"),
				"build_type": r.Header.Get("X-Gocacheprog-Build-Type"),
				"base":       r.Header.Get("X-Gocacheprog-Base"),
			}
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/restore-cache":
			rw.WriteHeader(http.StatusOK)
			require.NoError(t, binary.Write(rw, binary.BigEndian, int32(0)))
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	cacheDir := t.TempDir()
	startedAt := time.Date(2026, time.June, 2, 9, 12, 26, 0, time.UTC)
	params := &local.ProxyParams{
		Commit:       "commit123",
		ChangesID:    "repo/pr-123",
		BuildType:    "unit",
		BaseCommit:   "base123",
		ParentCommit: "parent123",
	}

	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", true, false, 0, 1024, startedAt, params)
	require.NoError(t, err)
	require.NotEmpty(t, gotHeaders["session_id"])
	require.Equal(t, startedAt.Format(time.RFC3339Nano), gotHeaders["started_at"])
	require.NotEmpty(t, gotHeaders["pid"])
	require.Equal(t, cacheDir, gotHeaders["cache_dir"])
	require.Equal(t, "commit123", gotHeaders["commit"])
	require.Equal(t, "parent123", gotHeaders["parent"])
	require.Equal(t, "repo/pr-123", gotHeaders["changes"])
	require.Equal(t, "unit", gotHeaders["build_type"])
	require.Equal(t, "base123", gotHeaders["base"])
}

func TestHumanBytesPerSecond(t *testing.T) {
	require.Equal(t, "0 B/s", humanBytesPerSecond(0, time.Second))
	require.Equal(t, "0 B/s", humanBytesPerSecond(1024, 0))
	require.Equal(t, "1.0 KiB/s", humanBytesPerSecond(2048, 2*time.Second))
	require.Equal(t, "1.5 KiB/s", humanBytesPerSecond(1536, time.Second))
}
