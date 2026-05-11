package http_test

import (
	"bytes"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"github.com/vearutop/gocacheprogd/internal/http"
	"github.com/vearutop/gocacheprogd/internal/local"
)

func TestNewClient(t *testing.T) {
	localStore, err := local.NewStore("./testdata", false)
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

		defer rd.Close()

		b, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(item.ActionID, "actionId", "body"), string(b))
	}))
}

func TestClient_PostCacheUsed(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.PostCacheUsed("abcdef1234", "repo/pr-123", "unit", []string{"actionId2", "actionId1", "actionId1"})
	require.NoError(t, err)

	b, err := os.ReadFile(filepath.Join(dir, "manifests", "buildtype-unit", "ab", "abcdef1234"))
	require.NoError(t, err)
	require.Equal(t, "actionId2\nactionId1\n", string(b))

	b, err = os.ReadFile(filepath.Join(dir, "manifests", "buildtype-unit", "changes", "re", "repo%2Fpr-123"))
	require.NoError(t, err)
	require.Equal(t, "actionId2\nactionId1\n", string(b))
}

func TestPreload_UsesCommitManifestFilters(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", strings.Repeat("body-2", 1000), &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "", []string{"actionId1", "missingAction"}))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "", []string{"actionId3"}))

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

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", "body-2", &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("current123", "", "", []string{"actionId2"}))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "", []string{"actionId1"}))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "", []string{"actionId3"}))

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

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	now := time.Now()
	items := []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
		makeItem("actionId2", "outputId2", "body-2", &now),
		makeItem("actionId3", "outputId3", "body-3", &now),
	}

	require.NoError(t, localStore.Put(cache.Response{Items: items}))
	require.NoError(t, localStore.PostCacheUsed("parent123", "", "unit", []string{"actionId1"}))
	require.NoError(t, localStore.PostCacheUsed("", "repo/pr-123", "unit", []string{"actionId2"}))
	require.NoError(t, localStore.PostCacheUsed("base123", "", "unit", []string{"actionId3"}))

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

	localStore, err := local.NewStore(dir, true)
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

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	h := http.NewHandler(localStore, "")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	client, err := http.NewClient(srv.URL, "")
	require.NoError(t, err)

	err = client.PostCacheUsed("", strings.Repeat("a", 101), "", []string{"actionId1"})
	require.EqualError(t, err, "cache-used status 400: changes-id too long: 101 > 100")
}

func TestClient_AuthToken(t *testing.T) {
	dir := t.TempDir()

	localStore, err := local.NewStore(dir, true)
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, localStore.Put(cache.Response{Items: []cache.ResponseItem{
		makeItem("actionId1", "outputId1", "body-1", &now),
	}}))

	h := http.NewHandler(localStore, "secret-token")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	_, err = http.NewClient(srv.URL, "")
	require.EqualError(t, err, "unexpected version: unauthorized\n")

	client, err := http.NewClient(srv.URL, "secret-token")
	require.NoError(t, err)

	var got []string
	err = client.Get(cache.Request{ActionIDs: []string{"actionId1"}}, func(item cache.ResponseItem) {
		got = append(got, item.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId1"}, got)
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
	localStore, err := local.NewStore("./testdata", true)
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
	pr, err := local.NewProxy("testdata/proxy", client, resps)
	require.NoError(t, err)

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

		defer rd.Close()

		b, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(item.ActionID, "actionId", strings.Repeat("body", 1000)), string(b))
	}))
}
