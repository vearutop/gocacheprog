package local

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/gocache"
	cachehttp "github.com/vearutop/gocacheprog/internal/http"
)

// newFallbackTestServer stands up a real gocache-backed HTTP server (the same stack the
// production remote runs), suitable for exercising SaveFreshNativeCache/initLocalGocacheMode's
// fallback_remote restore/upload against a genuine round trip rather than hand-rolled endpoints.
func newFallbackTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	serverDir := t.TempDir()
	localStore, err := NewStore(serverDir, WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(cachehttp.NewHandlerWithPreloadLimit(localStore, nativeStore, "", "", 2))
	t.Cleanup(srv.Close)

	return srv
}

func writeCacheFile(t *testing.T, cacheDir, relPath, content string, modTime time.Time) {
	t.Helper()

	path := filepath.Join(cacheDir, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	require.NoError(t, os.Chtimes(path, modTime, modTime))
}

func restoredPathsFrom(t *testing.T, client *cachehttp.Client, req gocache.Request) []string {
	t.Helper()

	restoreDir := t.TempDir()
	var paths []string
	_, err := client.RestoreCache(req, func(item gocache.FileItem, body io.Reader) error {
		paths = append(paths, item.Path)
		return gocache.RestoreToDir(restoreDir, item, body)
	})
	require.NoError(t, err)

	return paths
}

func TestSaveFreshNativeCache_UploadsOnlyFreshNonExcludedFiles(t *testing.T) {
	srv := newFallbackTestServer(t)
	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	cacheDir := t.TempDir()
	since := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)

	writeCacheFile(t, cacheDir, "ab/old", "stale entry from another build type", since.Add(-time.Hour))
	writeCacheFile(t, cacheDir, "ab/fresh", "genuinely new this job", since.Add(time.Minute))
	writeCacheFile(t, cacheDir, localGocacheStatsFilename, `{"build_types":{}}`, since.Add(time.Minute))
	writeCacheFile(t, cacheDir, "cd/restored", "just pulled from remote", since.Add(time.Minute))
	require.NoError(t, gocache.WriteRestoredPaths(cacheDir, []string{"cd/restored"}))

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}
	stats, err := SaveFreshNativeCache(cacheDir, client, req, 0, since, isLocalGocacheProtectedFile)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files)

	require.Equal(t, []string{"ab/fresh"}, restoredPathsFrom(t, client, req))
}

func TestSaveFreshNativeCache_NoFreshFilesIsNoop(t *testing.T) {
	srv := newFallbackTestServer(t)
	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	cacheDir := t.TempDir()
	since := time.Now().UTC()
	writeCacheFile(t, cacheDir, "ab/old", "stale", since.Add(-time.Hour))

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}
	stats, err := SaveFreshNativeCache(cacheDir, client, req, 0, since, isLocalGocacheProtectedFile)
	require.NoError(t, err)
	require.Equal(t, 0, stats.Files)
}

func TestInitLocalGocacheMode_FallbackRemoteRestoresWhenCold(t *testing.T) {
	srv := newFallbackTestServer(t)
	seedClient, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	seedDir := t.TempDir()
	writeCacheFile(t, seedDir, "ab/seed", "preexisting remote content", time.Now())
	seedBatch, err := gocache.CollectFreshFiles(seedDir, 0)
	require.NoError(t, err)

	req := gocache.Request{Commit: "commit123", BuildType: "owner-repo-unit"}
	_, err = seedClient.SaveCache(req, seedBatch)
	require.NoError(t, err)

	cacheDir := filepath.Join(t.TempDir(), "cache")
	githubEnv := filepath.Join(t.TempDir(), "github_env")
	t.Setenv("GITHUB_ENV", githubEnv)

	cfg := githubActionsConfig{
		cacheDir:       cacheDir,
		buildType:      "owner-repo-unit",
		remoteURL:      srv.URL,
		fallbackRemote: true,
	}
	require.NoError(t, initLocalGocacheMode(cfg, "commit123", "", "", time.Now()))

	body, err := os.ReadFile(filepath.Join(cacheDir, "ab", "seed"))
	require.NoError(t, err)
	require.Equal(t, "preexisting remote content", string(body))

	env, err := os.ReadFile(githubEnv)
	require.NoError(t, err)
	require.Contains(t, string(env), envGHALocalFallback+"=1")
	require.Contains(t, string(env), envGHARemoteURL+"="+srv.URL)
	require.Contains(t, string(env), envGHACommit+"=commit123")
}

func TestInitLocalGocacheMode_FallbackRemoteSkipsRestoreWhenWarm(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.MkdirAll(cacheDir, 0o750))
	_, err := recordLocalGocacheUsage(cacheDir, "owner-repo-unit", time.Now().UTC())
	require.NoError(t, err)

	githubEnv := filepath.Join(t.TempDir(), "github_env")
	t.Setenv("GITHUB_ENV", githubEnv)

	cfg := githubActionsConfig{
		cacheDir:       cacheDir,
		buildType:      "owner-repo-unit",
		remoteURL:      "http://127.0.0.1:1", // unreachable; a restore attempt would fail the call
		fallbackRemote: true,
	}
	require.NoError(t, initLocalGocacheMode(cfg, "commit123", "", "", time.Now()))

	env, err := os.ReadFile(githubEnv)
	require.NoError(t, err)
	require.NotContains(t, string(env), envGHALocalFallback)
}

func TestInitLocalGocacheMode_FallbackRemoteColdWithoutRemoteURLErrors(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	githubEnv := filepath.Join(t.TempDir(), "github_env")
	t.Setenv("GITHUB_ENV", githubEnv)

	cfg := githubActionsConfig{
		cacheDir:       cacheDir,
		buildType:      "owner-repo-unit",
		fallbackRemote: true,
	}
	err := initLocalGocacheMode(cfg, "commit123", "", "", time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "fallback_remote requires a remote URL")
}

func TestDoneLocalGocacheFallbackUpload_UploadsOnlyFilesSinceInit(t *testing.T) {
	srv := newFallbackTestServer(t)

	cacheDir := t.TempDir()
	since := time.Now().UTC()

	writeCacheFile(t, cacheDir, "ab/preexisting", "from another build type", since.Add(-time.Hour))
	writeCacheFile(t, cacheDir, "ab/produced", "built this job", since.Add(time.Minute))

	t.Setenv(envGHARemoteURL, srv.URL)
	t.Setenv(envGHAAuth, "")
	t.Setenv(envGHACommit, "commit123")
	t.Setenv(envGHAChangesID, "")
	t.Setenv(envGHABaseCommit, "")
	t.Setenv(envGHAMaxFileBytes, "0")
	t.Setenv(envGHAInitTime, since.Format(time.RFC3339Nano))

	doneLocalGocacheFallbackUpload(cacheDir, "owner-repo-unit")

	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)
	req := gocache.Request{Commit: "commit123", BuildType: "owner-repo-unit"}
	require.Equal(t, []string{"ab/produced"}, restoredPathsFrom(t, client, req))
}

func TestDoneLocalGocacheFallbackUpload_NoRemoteURLIsNoop(t *testing.T) {
	t.Setenv(envGHARemoteURL, "")
	require.NotPanics(t, func() {
		doneLocalGocacheFallbackUpload(t.TempDir(), "owner-repo-unit")
	})
}
