package local

import (
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
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

// TestSaveFreshNativeCache_SkipsObjectsServerAlreadyHas covers the actual reported waste: many
// parallel jobs rebuilding the same unchanged dependency from an empty local GOCACHE each think
// it's "fresh" locally, but it's only genuinely new to the server the first time. The pre-upload
// existence check must filter it out before it's compressed and uploaded again.
func TestSaveFreshNativeCache_SkipsObjectsServerAlreadyHas(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := NewStore(serverDir, WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(cachehttp.NewHandlerWithPreloadLimit(localStore, nativeStore, "", "", 2))
	t.Cleanup(srv.Close)

	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	preExisting := gocache.FileItem{Path: "ab/shared-dep", Size: int64(len("shared content")), WireSize: int64(len("shared content"))}
	preExisting.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("shared content")), nil
	})
	require.NoError(t, nativeStore.SaveItem(preExisting))

	cacheDir := t.TempDir()
	since := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	writeCacheFile(t, cacheDir, "ab/shared-dep", "shared content", since.Add(time.Minute))
	writeCacheFile(t, cacheDir, "cd/genuinely-new", "new content", since.Add(time.Minute))

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}
	stats, err := SaveFreshNativeCache(cacheDir, client, req, 0, since, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files, "only the genuinely new file should be uploaded")

	require.Equal(t, "0", nativeStore.Stats()["putsExist"], "the pre-existing object must never reach SaveItem")

	// Skipping the re-upload must not also mean the path silently drops out of this
	// commit's manifest -- a later restore for commit123 needs to find it even though it
	// was never re-uploaded in this session.
	commitManifestPath := filepath.Join(serverDir, "native", "manifests", "buildtype-unit", "c", "commit123.zst")
	commitData, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	commitBody, err := cache.DecodeZstd(nil, commitData)
	require.NoError(t, err)
	require.Contains(t, string(commitBody), "ab/shared-dep\n", "skipped-but-existing path must still be merged into the manifest")
	require.Contains(t, string(commitBody), "cd/genuinely-new\n")
}

// TestSaveFreshNativeCache_LogsSkippedLargeFiles covers the actual reported gap: a file excluded
// for exceeding -max-file-bytes is dropped before it's even considered a save candidate
// (CollectFilesToSave), so it never showed up anywhere in the logs -- looking identical to
// "there was nothing new to save" even though real content was silently left uncached. The log
// line must call out both how many objects were excluded this way and their total size.
func TestSaveFreshNativeCache_LogsSkippedLargeFiles(t *testing.T) {
	srv := newFallbackTestServer(t)
	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	cacheDir := t.TempDir()
	writeCacheFile(t, cacheDir, "ab/small", "ok", time.Now())
	writeCacheFile(t, cacheDir, "cd/too-big", strings.Repeat("x", 100), time.Now())

	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	req := gocache.Request{Commit: "commit123", BuildType: "unit"}
	stats, err := SaveFreshNativeCache(cacheDir, client, req, 10, time.Time{}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files, "only the file within -max-file-bytes should be uploaded")

	require.Contains(t, buf.String(), "1 large objects (100 B total) excluded")
}

// TestSaveFreshNativeCache_ManifestIncludesRestoredPathsNotJustNewOnes covers a real production
// incident: a job restores N files from "changes"/"base"/"default" (nothing to do with this
// exact commit yet), then its build only adds a handful of genuinely new ones -- but the
// resulting "commit" manifest ended up with only the handful, never the N it restored, because
// only the not-already-restored delta was ever reported back to MergeSavedPaths. A later rerun
// of the very same commit then had to restore via "commit" and got next to nothing back, forcing
// it to redundantly recompute (and re-discover, already on the server) most of what the first
// run had already resolved.
func TestSaveFreshNativeCache_ManifestIncludesRestoredPathsNotJustNewOnes(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := NewStore(serverDir, WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(cachehttp.NewHandlerWithPreloadLimit(localStore, nativeStore, "", "", 2))
	t.Cleanup(srv.Close)

	client, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	changesReq := gocache.Request{ChangesID: "org/repo#1", BuildType: "unit"}
	seedItem := gocache.FileItem{Path: "cd/from-changes", Size: int64(len("from changes history")), WireSize: int64(len("from changes history"))}
	seedItem.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("from changes history")), nil
	})
	_, err = client.SaveCache(changesReq, gocache.Batch{Items: []gocache.FileItem{seedItem}})
	require.NoError(t, err)

	req := gocache.Request{Commit: "commit123", ChangesID: "org/repo#1", BuildType: "unit"}
	cacheDir := t.TempDir()
	_, err = RestoreNativeCache(cacheDir, client, req, time.Now())
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(cacheDir, "cd", "from-changes"))
	require.NoError(t, err, "sanity check: the changes-scoped file must have actually been restored")

	writeCacheFile(t, cacheDir, "ab/new-file", "genuinely new this job", time.Now())

	stats, err := SaveFreshNativeCache(cacheDir, client, req, 0, time.Time{}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files, "only the genuinely new file should be uploaded")

	commitManifestPath := filepath.Join(serverDir, "native", "manifests", "buildtype-unit", "c", "commit123.zst")
	commitData, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	commitBody, err := cache.DecodeZstd(nil, commitData)
	require.NoError(t, err)
	require.Contains(t, string(commitBody), "cd/from-changes\n", "the restored-but-not-newly-uploaded path must still land in commit's own manifest")
	require.Contains(t, string(commitBody), "ab/new-file\n")
}

func TestInitLocalGocacheMode_FallbackRemoteRestoresWhenCold(t *testing.T) {
	srv := newFallbackTestServer(t)
	seedClient, err := cachehttp.NewClient(srv.URL, "")
	require.NoError(t, err)

	seedDir := t.TempDir()
	writeCacheFile(t, seedDir, "ab/seed", "preexisting remote content", time.Now())
	seedBatch, _, err := gocache.CollectFreshFiles(seedDir, 0)
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
