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
