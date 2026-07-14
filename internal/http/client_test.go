package http_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
	"github.com/vearutop/gocacheprog/internal/gocache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func TestNewClient(t *testing.T) {
	localStore, err := local.NewStore("./testdata")
	require.NoError(t, err)

	now := time.Now()
	var items []cache.ResponseItem
	for i := 0; i < 10; i++ {
		item := cache.ResponseItem{}

		item.ActionID = "actionId" + strconv.Itoa(i)
		body := "body" + strconv.Itoa(i)
		item.Size = int64(len(body))
		item.OutputID = "outputId" + strconv.Itoa(i)
		item.Time = &now
		item.WireSize = item.Size
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBufferString(body)), nil
		})
		items = append(items, item)
	}

	h := http.NewHandler(localStore, "")

	srv := httptest.NewServer(h)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	require.NoError(t, client.Put(cache.Response{Items: items}))

	req := cache.Request{}
	for i := 0; i < 5; i++ {
		req.ActionIDs = append(req.ActionIDs, "actionId"+strconv.Itoa(i))
	}

	require.NoError(t, client.Get(req, func(item cache.ResponseItem) {
		rd, err := item.UncompressedBodyReader()
		require.NoError(t, err)

		defer func() {
			require.NoError(t, rd.Close())
		}()

		b, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(item.ActionID, "actionId", "body"), string(b))
	}))
}

func TestNewClientWithSession_SendsVersionSessionHeaders(t *testing.T) {
	var gotHeaders map[string]string

	srv := httptest.NewServer(nethttp.HandlerFunc(func(rw nethttp.ResponseWriter, r *nethttp.Request) {
		gotHeaders = map[string]string{
			"session":    r.Header.Get("X-Gocacheprog-Session-Id"),
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
	}))
	t.Cleanup(srv.Close)

	startedAt := time.Date(2026, time.May, 12, 0, 20, 0, 123, time.UTC)
	client, err := http.NewClientWithSession(srv.URL, "", &http.SessionInfo{
		SessionID: "session-123",
		StartedAt: startedAt,
		PID:       42,
		CacheDir:  "/tmp/build-cache",
		Params: local.ProxyParams{
			Commit:       "commit123",
			ParentCommit: "parent123",
			ChangesID:    "repo/pr-123",
			BuildType:    "unit",
			BaseCommit:   "base123",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, "session-123", gotHeaders["session"])
	require.Equal(t, startedAt.Format(time.RFC3339Nano), gotHeaders["started_at"])
	require.Equal(t, "42", gotHeaders["pid"])
	require.Equal(t, "/tmp/build-cache", gotHeaders["cache_dir"])
	require.Equal(t, "commit123", gotHeaders["commit"])
	require.Equal(t, "parent123", gotHeaders["parent"])
	require.Equal(t, "repo/pr-123", gotHeaders["changes"])
	require.Equal(t, "unit", gotHeaders["build_type"])
	require.Equal(t, "base123", gotHeaders["base"])
}

func TestClient_PostCacheUsed(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.PostCacheUsed("abcdef1234", "repo/pr-123", "unit", []string{"actionId2", "actionId1", "actionId1"}, false)
	require.NoError(t, err)
	err = client.PostCacheUsed("abcdef1234", "repo/pr-123", "unit", []string{"actionId3", "actionId1"}, false)
	require.NoError(t, err)

	b, err := os.ReadFile(filepath.Join(dir, "manifests", "buildtype-unit", "ab", "abcdef1234"))
	require.NoError(t, err)
	require.Equal(t, "actionId2\nactionId1\nactionId3\n", string(b))

	b, err = os.ReadFile(filepath.Join(dir, "manifests", "buildtype-unit", "changes", "re", "repo%2Fpr-123"))
	require.NoError(t, err)
	require.Equal(t, "actionId2\nactionId1\nactionId3\n", string(b))
}

func TestClient_SaveAndRestoreNativeGOCACHE(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir, local.WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", 2))
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	cacheDir := t.TempDir()
	now := time.Date(2026, time.May, 14, 9, 30, 0, 0, time.UTC)
	targetPath := filepath.Join(cacheDir, "ab", "cache-entry-a")
	require.NoError(t, os.MkdirAll(filepath.Dir(targetPath), 0o750))
	require.NoError(t, os.WriteFile(targetPath, []byte("native-cache-payload"), 0o600))
	require.NoError(t, os.Chtimes(targetPath, now, now))

	batch, err := gocache.CollectFreshFiles(cacheDir, 0)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)

	req := gocache.Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}
	saveStats, err := client.SaveCache(req, batch)
	require.NoError(t, err)
	require.Equal(t, 1, saveStats.Files)
	require.NotZero(t, saveStats.Duration)

	restoreDir := t.TempDir()
	restoreStats, err := client.RestoreCache(req, func(item gocache.FileItem, body io.Reader) error {
		return gocache.RestoreToDir(restoreDir, item, body)
	})
	require.NoError(t, err)
	require.Equal(t, 1, restoreStats.Files)
	require.NotZero(t, restoreStats.Duration)
	require.Equal(t, "commit,changes", client.LastRestoreSources())

	restoredPath := filepath.Join(restoreDir, "ab", "cache-entry-a")
	body, err := os.ReadFile(restoredPath)
	require.NoError(t, err)
	require.Equal(t, "native-cache-payload", string(body))

	info, err := os.Stat(restoredPath)
	require.NoError(t, err)
	require.NotEqual(t, now.Unix(), info.ModTime().Unix())
}

func TestClient_SaveCache_ChunksAndFinalizes(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir, local.WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", 2))
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)
	client.SetSaveCacheChunkBytes(128)

	cacheDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "ab"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "a"), []byte(strings.Repeat("a", 40)), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "b"), []byte(strings.Repeat("b", 40)), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "c"), []byte(strings.Repeat("c", 40)), 0o600))

	batch, err := gocache.CollectFreshFiles(cacheDir, 0)
	require.NoError(t, err)
	require.Len(t, batch.Items, 3)

	req := gocache.Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}
	stats, err := client.SaveCache(req, batch)
	require.NoError(t, err)
	require.Equal(t, 3, stats.Files)

	commitManifestPath := filepath.Join(serverDir, "native", "manifests", "buildtype-unit", "co", "commit123")
	changesManifestPath := filepath.Join(serverDir, "native", "manifests", "buildtype-unit", "changes", "re", "repo%2Fpr-123")

	commitBody, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	require.Equal(t, "ab/a\nab/b\nab/c\n", string(commitBody))

	changesBody, err := os.ReadFile(changesManifestPath)
	require.NoError(t, err)
	require.Equal(t, "ab/a\nab/b\nab/c\n", string(changesBody))
}

func TestClient_SaveCache_SkipsFilesExceedingServerMaxFileBytesWithoutClientFlag(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir, local.WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression(), gocache.WithMaxFileBytes(20))
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", 2))
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)
	client.SetSaveCacheChunkBytes(128)

	cacheDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "ab"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "small"), []byte(strings.Repeat("a", 10)), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ab", "large"), []byte(strings.Repeat("b", 40)), 0o600))

	// Client collects files with no local -max-file-bytes filtering (0, matching real CI usage),
	// relying entirely on the server-advertised limit learned during save-cache-start.
	batch, err := gocache.CollectFreshFiles(cacheDir, 0)
	require.NoError(t, err)
	require.Len(t, batch.Items, 2)

	req := gocache.Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}
	stats, err := client.SaveCache(req, batch)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Files)

	commitManifestPath := filepath.Join(serverDir, "native", "manifests", "buildtype-unit", "co", "commit123")
	commitBody, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	require.Equal(t, "ab/small\n", string(commitBody))
}

func TestSaveCacheFinalize_TruncatedUploadErrorIncludesContext(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir, local.WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression())
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", 2))
	t.Cleanup(srv.Close)

	uploadID := "test-upload"
	startResp, err := srv.Client().Post(srv.URL+"/save-cache-start?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", nil)
	require.NoError(t, err)
	require.NoError(t, startResp.Body.Close())
	require.Equal(t, nethttp.StatusNoContent, startResp.StatusCode)

	item := gocache.FileItem{
		Path:     "ab/a",
		Size:     10,
		WireSize: 10,
	}
	itemJSON, err := json.Marshal(item)
	require.NoError(t, err)

	var chunk bytes.Buffer
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, int32(len(itemJSON))))
	_, err = chunk.Write(itemJSON)
	require.NoError(t, err)
	_, err = chunk.WriteString("short")
	require.NoError(t, err)

	chunkResp, err := srv.Client().Post(srv.URL+"/save-cache-chunk?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", bytes.NewReader(chunk.Bytes()))
	require.NoError(t, err)
	require.NoError(t, chunkResp.Body.Close())
	require.Equal(t, nethttp.StatusNoContent, chunkResp.StatusCode)

	finalizeResp, err := srv.Client().Post(srv.URL+"/save-cache-finalize?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, finalizeResp.Body.Close())
	}()

	body, err := io.ReadAll(finalizeResp.Body)
	require.NoError(t, err)
	msg := string(body)

	require.Equal(t, nethttp.StatusInternalServerError, finalizeResp.StatusCode)
	require.Contains(t, msg, "save-cache upload test-upload finalize failed")
	require.Contains(t, msg, "path=\"ab/a\"")
	require.Contains(t, msg, "wire_size=10")
	require.Contains(t, msg, "read_wire_bytes=5")
	require.Contains(t, msg, "truncated item body")
}

// TestSaveCacheChunk_OversizedItemFollowedByAnotherDoesNotDeadlock guards
// against a real deadlock: when Store.putOne silently skips an item over
// -max-file-bytes, the save-cache callback must not mistake that intentional
// skip for a truncated upload. Doing so used to make the stream-processing
// goroutine return (and stop draining the pipe) while the SaveCacheChunk
// HTTP handler was still writing the rest of that same chunk (the next
// item) into the pipe — an unrecoverable hang with no error on either side.
// A bounded client timeout turns a regression into a clear test failure
// instead of hanging the test suite.
func TestSaveCacheChunk_OversizedItemFollowedByAnotherDoesNotDeadlock(t *testing.T) {
	serverDir := t.TempDir()
	localStore, err := local.NewStore(serverDir, local.WithCompression())
	require.NoError(t, err)

	nativeStore, err := gocache.NewStore(filepath.Join(serverDir, "native"), gocache.WithCompression(), gocache.WithMaxFileBytes(20))
	require.NoError(t, err)

	srv := httptest.NewServer(http.NewHandlerWithPreloadLimit(localStore, nativeStore, "", 2))
	t.Cleanup(srv.Close)

	uploadID := "deadlock-test"
	startResp, err := srv.Client().Post(srv.URL+"/save-cache-start?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", nil)
	require.NoError(t, err)
	require.NoError(t, startResp.Body.Close())
	require.Equal(t, nethttp.StatusNoContent, startResp.StatusCode)

	var chunk bytes.Buffer
	oversized := gocache.FileItem{Path: "ab/oversized", Size: 30, WireSize: 30}
	oversizedJSON, err := json.Marshal(oversized)
	require.NoError(t, err)
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, int32(len(oversizedJSON))))
	_, err = chunk.Write(oversizedJSON)
	require.NoError(t, err)
	_, err = chunk.WriteString(strings.Repeat("x", 30))
	require.NoError(t, err)

	normal := gocache.FileItem{Path: "ab/normal", Size: 10, WireSize: 10}
	normalJSON, err := json.Marshal(normal)
	require.NoError(t, err)
	require.NoError(t, binary.Write(&chunk, binary.BigEndian, int32(len(normalJSON))))
	_, err = chunk.Write(normalJSON)
	require.NoError(t, err)
	_, err = chunk.WriteString(strings.Repeat("y", 10))
	require.NoError(t, err)

	require.NoError(t, binary.Write(&chunk, binary.BigEndian, int32(0)))

	boundedClient := *srv.Client()
	boundedClient.Timeout = 5 * time.Second

	chunkResp, err := boundedClient.Post(srv.URL+"/save-cache-chunk?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", bytes.NewReader(chunk.Bytes()))
	require.NoError(t, err, "chunk request must not hang/time out")
	require.NoError(t, chunkResp.Body.Close())
	require.Equal(t, nethttp.StatusNoContent, chunkResp.StatusCode)

	finalizeResp, err := boundedClient.Post(srv.URL+"/save-cache-finalize?upload-id="+uploadID+"&commit=commit123", "application/octet-stream", nil)
	require.NoError(t, err)
	require.NoError(t, finalizeResp.Body.Close())
	require.Equal(t, nethttp.StatusNoContent, finalizeResp.StatusCode)

	commitManifestPath := filepath.Join(serverDir, "native", "manifests", "default", "co", "commit123")
	commitBody, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	require.Equal(t, "ab/normal\n", string(commitBody))

	require.NoFileExists(t, filepath.Join(serverDir, "native", "objects", "ab", "oversized"))
	require.FileExists(t, filepath.Join(serverDir, "native", "objects", "ab", "normal"))
}

func TestPreload_UsesCommitManifestFilters(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", strings.Repeat("body-2", 1000), &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "", []string{"actionId1", "missingAction"}, false))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "", []string{"actionId3"}, false))

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	var got []string
	err = client.Preload(cache.PreloadRequest{
		MaxSize:      1000,
		ParentCommit: "parent123",
		BaseCommit:   "base123",
	}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"actionId1", "actionId3"}, got)
	require.Equal(t, "parent,base", client.LastPreloadSources())
}

func TestPreload_UsesCurrentCommitManifestForRerun(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", "body-2", &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("current123", "", "", []string{"actionId2"}, false))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "", []string{"actionId1"}, false))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "", []string{"actionId3"}, false))

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	var got []string
	err = client.Preload(cache.PreloadRequest{
		MaxSize:      1000,
		Commit:       "current123",
		ParentCommit: "parent123",
		BaseCommit:   "base123",
	}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"actionId1", "actionId2", "actionId3"}, got)
	require.Equal(t, "commit,parent,base", client.LastPreloadSources())
}

func TestPreload_UsesChangesIDBetweenParentAndBase(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", "body-2", &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "unit", []string{"actionId1"}, false))
	require.NoError(t, localStore.PostCacheUsed("", "repo/pr-123", "unit", []string{"actionId2"}, false))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "unit", []string{"actionId3"}, false))

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	var got []string
	err = client.Preload(cache.PreloadRequest{
		MaxSize:      1000,
		ParentCommit: "parent123",
		ChangesID:    "repo/pr-123",
		BuildType:    "unit",
		BaseCommit:   "base123",
	}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"actionId1", "actionId2", "actionId3"}, got)
	require.Equal(t, "parent,changes,base", client.LastPreloadSources())
}

func TestClient_Preload_HTTPError(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.Preload(cache.PreloadRequest{
		BaseCommit: "'invalid'",
	}, func(item cache.ResponseItem) {})
	require.EqualError(t, err, `preload status 400: invalid commit: "'invalid'"`)
}

func TestClient_PostCacheUsed_HTTPError(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.PostCacheUsed("", strings.Repeat("a", 101), "", []string{"actionId1"}, false)
	require.EqualError(t, err, "cache-used status 400: changes-id too long: 101 > 100")
}

func TestClient_AuthToken(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
	}}))

	h := http.NewHandler(localStore, "secret-token")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	_, err = http.NewClient(srv.URL, "")
	require.EqualError(t, err, "authentication failed: -auth-token <value> is missing or incorrect")

	client, err := http.NewClient(srv.URL, "secret-token")
	require.NoError(t, err)

	var got []string
	err = client.Get(cache.Request{ActionIDs: []string{"actionId1"}}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId1"}, got)
}

func TestClient_StatsTracksRemoteGetRequestsSeparatelyFromHead(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", "body-2", &now),
	}}))

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.Get(cache.Request{ActionIDs: []string{"actionId1"}}, func(item cache.ResponseItem) {})
	require.NoError(t, err)

	err = client.Put(cache.Response{Items: []cache.ResponseItem{
		makeItem("actionId3", "outputId3", "body-3", &now),
	}})
	require.NoError(t, err)

	stats := client.Stats()
	require.Equal(t, "2", stats["get_cnt"])
	require.Equal(t, "1", stats["get_req_cnt"])
	require.Equal(t, "1", stats["put_cnt"])
}

func TestStatus(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, local.WithCompression(), local.WithMaxDiskBytes(123456))
	require.NoError(t, err)
	gocacheStore, err := gocache.NewStore(filepath.Join(dir, "native"), gocache.WithCompression(), gocache.WithMaxDiskBytes(654321))
	require.NoError(t, err)

	h := http.NewHandlerWithPreloadLimit(localStore, gocacheStore, "", 2)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := nethttp.Get(srv.URL + "/status")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, res.Body.Close())
	}()
	require.Equal(t, nethttp.StatusOK, res.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(res.Body).Decode(&body))

	storeStats, ok := body["store"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "123456", storeStats["maxDiskBytes"])
	require.Equal(t, "120.6KB", storeStats["maxDiskBytesHuman"])
	require.Contains(t, storeStats, "lastEviction")

	gocacheStats, ok := body["gocache"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "654321", gocacheStats["maxDiskBytes"])
	require.Contains(t, gocacheStats, "maxDiskBytesHuman")
	require.Contains(t, gocacheStats, "lastEviction")

	runtimeStats, ok := body["runtime"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, runtimeStats, "heapInuseBytes")
	require.Contains(t, runtimeStats, "heapInuse")

	httpStats, ok := body["http"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "2", httpStats["preloadLimit"])
	require.Contains(t, httpStats, "preloadInFlight")
	require.Contains(t, httpStats, "preloadStarted")
	require.Contains(t, httpStats, "preloadCompleted")
}

func makeItem(actionID, outputID, body string, now *time.Time) cache.ResponseItem {
	item := cache.ResponseItem{
		ActionID: actionID,
		Size:     int64(len(body)),
		OutputID: outputID,
		Time:     now,
		WireSize: int64(len(body)),
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString(body)), nil
	})

	return item
}

func TestNewClient_compressed(t *testing.T) {
	localStore, err := local.NewStore("./testdata", local.WithCompression())
	require.NoError(t, err)

	now := time.Now()
	var items []cache.ResponseItem
	for i := 0; i < 10; i++ {
		item := cache.ResponseItem{}

		item.ActionID = "actionId" + strconv.Itoa(i)
		body := strings.Repeat("body", 1000) + strconv.Itoa(i)
		item.Size = int64(len(body))
		item.OutputID = "outputId" + strconv.Itoa(i)
		item.Time = &now
		item.WireSize = item.Size
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBufferString(body)), nil
		})
		items = append(items, item)
	}

	h := http.NewHandler(localStore, "")

	srv := httptest.NewServer(h)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	require.NoError(t, client.Put(cache.Response{Items: items}))

	req := cache.Request{}
	for i := 0; i < 5; i++ {
		req.ActionIDs = append(req.ActionIDs, "actionId"+strconv.Itoa(i))
	}

	resps := make(chan cacheprog.Response, 10)
	store, err := local.NewStore("testdata/proxy")
	require.NoError(t, err)
	pr := local.NewProxy(store, client, resps, local.ProxyParams{})

	err = pr.Preload(cache.PreloadRequest{
		MaxSize: 100000,
	})
	require.NoError(t, err)

	pr.Lookup(cacheprog.Request{
		TS:       123,
		ID:       1,
		Command:  cacheprog.CmdGet,
		ActionID: "actionId5",
	})

	resp := <-resps

	println(resp.DiskPath)
	println(resp.Size)

	require.NoError(t, client.Get(req, func(item cache.ResponseItem) {
		rd, err := item.UncompressedBodyReader()
		require.NoError(t, err)

		defer func() {
			require.NoError(t, rd.Close())
		}()

		b, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(item.ActionID, "actionId", strings.Repeat("body", 1000)), string(b))
	}))
}
