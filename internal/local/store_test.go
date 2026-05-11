package local

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
)

func TestStorePreload_NoCommitFiltersFallsBackToAllItems(t *testing.T) {
	store, err := NewStore(t.TempDir(), true)
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
	store, err := NewStore(t.TempDir(), true)
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
	store, err := NewStore(t.TempDir(), true)
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("current123", "", "", []string{"actionId2"}))

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
	store, err := NewStore(t.TempDir(), true)
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
		testItem("actionId3", "outputId3", "body-3", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("parent123", "", "", []string{"actionId1"}))
	require.NoError(t, store.PostCacheUsed("", "repo/pr-123", "", []string{"actionId2"}))
	require.NoError(t, store.PostCacheUsed("base123", "", "", []string{"actionId3"}))

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
	store, err := NewStore(t.TempDir(), true)
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
		testItem("actionId2", "outputId2", "body-2", &now),
	}}))
	require.NoError(t, store.PostCacheUsed("current123", "", "unit", []string{"actionId1"}))
	require.NoError(t, store.PostCacheUsed("current123", "", "race", []string{"actionId2"}))

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
