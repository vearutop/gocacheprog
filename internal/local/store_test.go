package local

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/cache"
)

func TestStorePreload_NoCommitFiltersFallsBackToAllItems(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "small-body", &now),
		testItem("actionId2", "outputId2", "body-that-is-too-large", &now),
	}}))

	var got []string
	err = store.Preload(cache.PreloadRequest{MaxSize: int64(len("small-body"))}, func(resp cache.ResponseItem) {
		got = append(got, resp.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId1"}, got)
}

func TestStorePreload_MissingManifestIsIgnored(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
	}}))

	var got []string
	err = store.Preload(cache.PreloadRequest{
		MaxSize:      1024,
		ParentCommit: "missing-parent",
		BaseCommit:   "missing-base",
	}, func(resp cache.ResponseItem) {
		got = append(got, resp.ActionID)
	})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStorePreload_CurrentCommitManifestUsedForSameCommitRestart(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("current123", "", "", []string{"actionId2"}, false))

	var got []string
	err = store.Preload(cache.PreloadRequest{
		MaxSize: 1024,
		Commit:  "current123",
	}, func(resp cache.ResponseItem) {
		got = append(got, resp.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId2"}, got)
}

func TestStorePreload_ChangesIDManifestUsedAfterParent(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
		testItem("actionId3", "outputId3", "body-3", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("parent123", "", "", []string{"actionId1"}, false))
	require.NoError(t, store.PostCacheUsed("", "repo/pr-123", "", []string{"actionId2"}, false))
	require.NoError(t, store.PostCacheUsed("base123", "", "", []string{"actionId3"}, false))

	var got []string
	err = store.Preload(cache.PreloadRequest{
		MaxSize:      1024,
		ParentCommit: "parent123",
		ChangesID:    "repo/pr-123",
		BaseCommit:   "base123",
	}, func(resp cache.ResponseItem) {
		got = append(got, resp.ActionID)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"actionId1", "actionId2", "actionId3"}, got)

	sources, err := store.PreloadSources(cache.PreloadRequest{
		ParentCommit: "parent123",
		ChangesID:    "repo/pr-123",
		BaseCommit:   "base123",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"parent", "changes", "base"}, sources)
}

func TestStorePreload_BuildTypeIsolated(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("current123", "", "unit", []string{"actionId1"}, false))
	require.NoError(t, store.PostCacheUsed("current123", "", "race", []string{"actionId2"}, false))

	var gotUnit []string
	err = store.Preload(cache.PreloadRequest{
		MaxSize:   1024,
		Commit:    "current123",
		BuildType: "unit",
	}, func(resp cache.ResponseItem) {
		gotUnit = append(gotUnit, resp.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId1"}, gotUnit)

	var gotRace []string
	err = store.Preload(cache.PreloadRequest{
		MaxSize:   1024,
		Commit:    "current123",
		BuildType: "race",
	}, func(resp cache.ResponseItem) {
		gotRace = append(gotRace, resp.ActionID)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"actionId2"}, gotRace)
}

func TestStorePostCacheUsed_MergesWithExistingManifest(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	require.NoError(t, store.PostCacheUsed("current123", "", "", []string{"actionId1", "actionId2"}, false))
	require.NoError(t, store.PostCacheUsed("current123", "", "", []string{"actionId2", "actionId3"}, false))

	got, err := store.loadCommitManifest("current123", "")
	require.NoError(t, err)
	require.Equal(t, []string{"actionId1", "actionId2", "actionId3"}, got)
}

func TestStorePostCacheUsed_ReplaceChangesManifestOnColdStart(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	require.NoError(t, store.PostCacheUsed("", "repo/pr-123", "", []string{"oldAction", "sharedAction"}, false))
	require.NoError(t, store.PostCacheUsed("", "repo/pr-123", "", []string{"newAction", "sharedAction"}, true))

	got, err := store.loadChangesManifest("repo/pr-123", "")
	require.NoError(t, err)
	require.Equal(t, []string{"newAction", "sharedAction"}, got)
}

func TestStoreHasEntries(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)
	require.False(t, store.HasEntries())

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
	}}))

	require.True(t, store.HasEntries())
}

func TestStorePut_WritesEntriesUnderPrefixedDir(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "output/one", "body-1", &now),
	}}))

	_, err = os.Stat(filepath.Join(dir, "entries", "ou", "output_one"))
	require.NoError(t, err)
}

func TestStoreEvictsLeastRecentlyUsedWhenSizeLimitExceeded(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir, WithCompression(), WithMaxDiskBytes(10), WithEvictionDelay(10*time.Millisecond))
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "12345", &now),
	}}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId2", "outputId2", "67890", &now),
	}}))

	require.NoError(t, store.Get(cache.Request{ActionIDs: []string{"actionId1"}}, func(resp cache.ResponseItem) {}))
	store.mu.Lock()
	action1Access := store.index["actionId1"].AccessTimeMicro
	action2Access := store.index["actionId2"].AccessTimeMicro
	store.mu.Unlock()
	require.Greater(t, action1Access, action2Access)
	time.Sleep(2 * time.Millisecond)

	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId3", "outputId3", "abcde", &now),
	}}))

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.currentDiskBytes == int64(10) && len(store.index) == 2
	}, time.Second, 10*time.Millisecond)

	var got []string
	require.NoError(t, store.Get(cache.Request{ActionIDs: []string{"actionId1", "actionId2", "actionId3"}}, func(resp cache.ResponseItem) {
		if !resp.Miss {
			got = append(got, resp.ActionID)
		}
	}))
	require.ElementsMatch(t, []string{"actionId1", "actionId3"}, got)
}

func TestStoreManifestKeyLengthLimit(t *testing.T) {
	store, err := NewStore(t.TempDir(), WithCompression())
	require.NoError(t, err)

	longKey := strings.Repeat("a", maxManifestKeyLen+1)

	_, err = store.PreloadSources(cache.PreloadRequest{ChangesID: longKey})
	require.EqualError(t, err, "changes-id too long: 101 > 100")

	_, err = store.PreloadSources(cache.PreloadRequest{BuildType: longKey, Commit: "commit123"})
	require.EqualError(t, err, "build-type too long: 101 > 100")
}

func testItem(actionID, outputID, body string, now *time.Time) cache.ResponseItem {
	item := cache.ResponseItem{
		ActionID: actionID,
		OutputID: outputID,
		Size:     int64(len(body)),
		Time:     now,
		WireSize: int64(len(body)),
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString(body)), nil
	})

	return item
}
