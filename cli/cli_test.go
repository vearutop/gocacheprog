package cli

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/gocache"
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
		"-commit", "commit123",
		"-changes-id", "repo/pr-123",
		"-build-type", "lint",
		"-base-commit", "base123",
		"-parent-commit", "parent123",
		"-remote-batch-concurrency", "4",
	})
	require.NoError(t, err)

	require.True(t, params.Preload)
	require.True(t, params.SkipPreload)
	require.Equal(t, "commit123", params.Commit)
	require.Equal(t, "repo/pr-123", params.ChangesID)
	require.Equal(t, "lint", params.BuildType)
	require.Equal(t, "base123", params.BaseCommit)
	require.Equal(t, "parent123", params.ParentCommit)
	require.Equal(t, 4, params.RemoteBatchConcurrency)
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

	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", true, false, 0, 0, 1024, 0, startedAt, params)
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

func TestRunNativeGOCACHEMode_RestoreCachePassesMaxFileBytes(t *testing.T) {
	var gotReq struct {
		maxFileBytes      string
		restoreLimitBytes string
	}

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/restore-cache":
			gotReq.maxFileBytes = r.URL.Query().Get("max-file-bytes")
			gotReq.restoreLimitBytes = r.URL.Query().Get("restore-limit-bytes")
			rw.WriteHeader(http.StatusOK)
			require.NoError(t, binary.Write(rw, binary.BigEndian, int32(0)))
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	cacheDir := t.TempDir()
	startedAt := time.Date(2026, time.June, 2, 9, 12, 26, 0, time.UTC)

	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", true, false, 1234, 4321, 1024, 0, startedAt, &local.ProxyParams{})
	require.NoError(t, err)
	require.Equal(t, "1234", gotReq.maxFileBytes)
	require.Equal(t, "4321", gotReq.restoreLimitBytes)
}

func TestRunNativeGOCACHEMode_SaveCacheSkipsOversizedFilesBeforeUpload(t *testing.T) {
	var uploadedPaths []string

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/save-cache-start":
			require.Equal(t, "4", r.URL.Query().Get("max-file-bytes"))
			rw.WriteHeader(http.StatusNoContent)
		case "/save-cache-chunk":
			require.Equal(t, "4", r.URL.Query().Get("max-file-bytes"))
			_, err := gocache.ReadStream(r.Body, func(item gocache.FileItem, body io.Reader) error {
				uploadedPaths = append(uploadedPaths, item.Path)
				if body != nil {
					_, err := io.Copy(io.Discard, body)
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)
			rw.WriteHeader(http.StatusNoContent)
		case "/save-cache-finalize":
			require.Equal(t, "4", r.URL.Query().Get("max-file-bytes"))
			rw.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	cacheDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "ab"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "small"), []byte("1234"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "large"), []byte("123456"), 0o600))

	startedAt := time.Date(2026, time.June, 2, 9, 12, 26, 0, time.UTC)
	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", false, true, 4, 0, 1<<20, 0, startedAt, &local.ProxyParams{})
	require.NoError(t, err)
	require.Equal(t, []string{"ab/small"}, uploadedPaths)
}

func newSaveCacheUploadPathsServer(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()

	var uploadedPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/save-cache-start":
			rw.WriteHeader(http.StatusNoContent)
		case "/save-cache-chunk":
			_, err := gocache.ReadStream(r.Body, func(item gocache.FileItem, body io.Reader) error {
				uploadedPaths = append(uploadedPaths, item.Path)
				if body != nil {
					_, err := io.Copy(io.Discard, body)
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)
			rw.WriteHeader(http.StatusNoContent)
		case "/save-cache-finalize":
			rw.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	return srv, &uploadedPaths
}

func TestRunNativeGOCACHEMode_SaveCacheUsesJobStartUnixToExcludeOlderFiles(t *testing.T) {
	srv, uploadedPaths := newSaveCacheUploadPathsServer(t)

	cacheDir := t.TempDir()
	older := time.Date(2026, time.June, 2, 9, 0, 0, 0, time.UTC)
	jobStart := time.Date(2026, time.June, 2, 9, 30, 0, 0, time.UTC)
	newer := time.Date(2026, time.June, 2, 10, 0, 0, 0, time.UTC)

	oldPath := filepath.Join(cacheDir, "ab", "old")
	require.NoError(t, os.MkdirAll(filepath.Dir(oldPath), 0o750))
	require.NoError(t, os.WriteFile(oldPath, []byte("stale"), 0o600))
	require.NoError(t, os.Chtimes(oldPath, older, older))

	newPath := filepath.Join(cacheDir, "ab", "new")
	require.NoError(t, os.WriteFile(newPath, []byte("fresh"), 0o600))
	require.NoError(t, os.Chtimes(newPath, newer, newer))

	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", false, true, 0, 0, 1<<20, jobStart.UnixNano(), time.Now(), &local.ProxyParams{})
	require.NoError(t, err)
	require.Equal(t, []string{"ab/new"}, *uploadedPaths)
}

func TestRunNativeGOCACHEMode_SaveCacheUsesRestoreMarkerWhenJobStartUnixNotGiven(t *testing.T) {
	restoreSrv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/restore-cache":
			rw.WriteHeader(http.StatusOK)
			require.NoError(t, binary.Write(rw, binary.BigEndian, int32(0)))
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(restoreSrv.Close)

	cacheDir := t.TempDir()
	jobStart := time.Date(2026, time.June, 2, 9, 30, 0, 0, time.UTC)

	// An empty -restore-cache still writes the job-start marker save-cache reads back.
	err := runNativeGOCACHEMode(cacheDir, "", restoreSrv.URL, "", true, false, 0, 0, 1024, 0, jobStart, &local.ProxyParams{})
	require.NoError(t, err)

	older := jobStart.Add(-time.Hour)
	newer := jobStart.Add(time.Hour)

	oldPath := filepath.Join(cacheDir, "ab", "old")
	require.NoError(t, os.MkdirAll(filepath.Dir(oldPath), 0o750))
	require.NoError(t, os.WriteFile(oldPath, []byte("stale"), 0o600))
	require.NoError(t, os.Chtimes(oldPath, older, older))

	newPath := filepath.Join(cacheDir, "ab", "new")
	require.NoError(t, os.WriteFile(newPath, []byte("fresh"), 0o600))
	require.NoError(t, os.Chtimes(newPath, newer, newer))

	saveSrv, uploadedPaths := newSaveCacheUploadPathsServer(t)

	err = runNativeGOCACHEMode(cacheDir, "", saveSrv.URL, "", false, true, 0, 0, 1<<20, 0, time.Now(), &local.ProxyParams{})
	require.NoError(t, err)
	require.Equal(t, []string{"ab/new"}, *uploadedPaths)
}

func TestRunNativeGOCACHEMode_RestoreCacheSkipsOversizedFilesBeforeDownload(t *testing.T) {
	cacheDir := t.TempDir()

	smallBody := []byte("1234")
	largeBody := []byte("123456")
	now := time.Date(2026, time.June, 2, 9, 12, 26, 0, time.UTC)

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/version":
			_, err := rw.Write([]byte("gocacheprog test"))
			require.NoError(t, err)
		case "/restore-cache":
			require.Equal(t, "4", r.URL.Query().Get("max-file-bytes"))
			rw.WriteHeader(http.StatusOK)
			sw := gocache.NewStreamWriter(rw)

			small := gocache.FileItem{
				Path:     "ab/small",
				Size:     int64(len(smallBody)),
				WireSize: int64(len(smallBody)),
				ModTime:  &now,
			}
			small.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(smallBody)), nil
			})
			require.NoError(t, sw.WriteItem(small))

			if r.URL.Query().Get("max-file-bytes") == "" {
				large := gocache.FileItem{
					Path:     "ab/large",
					Size:     int64(len(largeBody)),
					WireSize: int64(len(largeBody)),
					ModTime:  &now,
				}
				large.SetBodyReader(func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(largeBody)), nil
				})
				require.NoError(t, sw.WriteItem(large))
			}

			require.NoError(t, sw.Close())
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)

	startedAt := time.Date(2026, time.June, 2, 9, 12, 26, 0, time.UTC)
	err := runNativeGOCACHEMode(cacheDir, "", srv.URL, "", true, false, 4, 0, 1024, 0, startedAt, &local.ProxyParams{})
	require.NoError(t, err)

	body, err := os.ReadFile(filepath.Join(cacheDir, "ab", "small"))
	require.NoError(t, err)
	require.Equal(t, smallBody, body)

	_, err = os.Stat(filepath.Join(cacheDir, "ab", "large"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNormalizeServerFlags(t *testing.T) {
	for _, tc := range []struct {
		name        string
		httpListen  string
		httpsListen string
		httpsHost   string
		wantHTTP    string
		wantHTTPS   string
		wantErr     string
	}{
		{
			name:      "https host implies defaults",
			httpsHost: "example.com",
			wantHTTP:  ":80",
			wantHTTPS: ":443",
		},
		{
			name:        "https host with explicit https keeps http default",
			httpsHost:   "example.com",
			httpsListen: ":445",
			wantHTTP:    ":80",
			wantHTTPS:   ":445",
		},
		{
			name:       "http only stays http",
			httpListen: "192.168.1.23:1234",
			wantHTTP:   "192.168.1.23:1234",
		},
		{
			name:       "unix http allowed without https",
			httpListen: "unix:///tmp/gocacheprog.sock",
			wantHTTP:   "unix:///tmp/gocacheprog.sock",
		},
		{
			name:        "https requires host",
			httpsListen: ":443",
			wantErr:     "-https requires -https-host",
		},
		{
			name:       "https host rejects custom http port",
			httpListen: ":9882",
			httpsHost:  "example.com",
			wantErr:    "-https-host requires -http on port 80",
		},
		{
			name:       "https host rejects unix",
			httpListen: "unix:///tmp/gocacheprog.sock",
			httpsHost:  "example.com",
			wantErr:    "-https-host cannot be combined with unix -http",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpListen := tc.httpListen
			httpsListen := tc.httpsListen
			httpsHost := tc.httpsHost

			err := normalizeServerFlags(&httpListen, &httpsListen, &httpsHost)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantHTTP, httpListen)
			require.Equal(t, tc.wantHTTPS, httpsListen)
		})
	}
}
