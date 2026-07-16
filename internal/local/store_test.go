package local

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// TestStorePut_ConcurrentSameOutputIDDoesNotRace guards against a race where two Put calls
// for different ActionIDs sharing an OutputID (identical build output content, e.g. one from
// a direct cmd/go CmdPut and one from a concurrent upstream fetch) both write outputFile.tmp
// and race on the rename, failing with "no such file or directory".
func TestStorePut_ConcurrentSameOutputIDDoesNotRace(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	require.NoError(t, err)

	now := time.Now()
	const writers = 20

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = store.Put(cache.Response{Items: []cache.ResponseItem{
				testItem(fmt.Sprintf("action-%d", i), "shared-output", "same-body", &now),
			}})
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	body, err := os.ReadFile(filepath.Join(dir, "entries", "sh", "shared-output"))
	require.NoError(t, err)
	require.Equal(t, "same-body", string(body))
}

func TestStoreMaxFileBytes_SkipsPutAndServe(t *testing.T) {
	dir := t.TempDir()

	unlimitedStore, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, unlimitedStore.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("smallAction", "smallOutput", "1234", &now),
		testItem("largeAction", "largeOutput", "123456", &now),
	}}))
	require.NoError(t, unlimitedStore.Close())

	limitedStore, err := NewStore(dir, WithCompression(), WithMaxFileBytes(5))
	require.NoError(t, err)

	var got []string
	require.NoError(t, limitedStore.Get(cache.Request{ActionIDs: []string{"smallAction", "largeAction"}}, func(resp cache.ResponseItem) {
		if !resp.Miss {
			got = append(got, resp.ActionID)
		}
	}))
	require.Equal(t, []string{"smallAction"}, got)

	limitedPutStore, err := NewStore(t.TempDir(), WithCompression(), WithMaxFileBytes(5))
	require.NoError(t, err)
	require.NoError(t, limitedPutStore.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("smallAction", "smallOutput", "1234", &now),
		testItem("largeAction", "largeOutput", "123456", &now),
	}}))

	var gotAfterPut []string
	require.NoError(t, limitedPutStore.Get(cache.Request{ActionIDs: []string{"smallAction", "largeAction"}}, func(resp cache.ResponseItem) {
		if !resp.Miss {
			gotAfterPut = append(gotAfterPut, resp.ActionID)
		}
	}))
	require.Equal(t, []string{"smallAction"}, gotAfterPut)
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

func TestStoreIntegrityCheck_ReportsNothingWhenEntriesAreHealthy(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "body-1", &now),
	}}))

	report := store.IntegrityCheck(false)
	require.EqualValues(t, 1, report.Checked)
	require.Empty(t, report.Broken)
	require.False(t, report.DryRun)
}

func TestStoreIntegrityCheck_DetectsAndRemovesCorruptedEntry(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "hello-world", &now),
	}}))

	// Corrupt the stored object directly, as if the file no longer matches the index's
	// recorded size - the same condition that produced cache.ErrShortRead on the client.
	require.NoError(t, os.WriteFile(store.OutputFilename("outputId1"), []byte("short"), 0o600))

	report := store.IntegrityCheck(false)
	require.EqualValues(t, 1, report.Checked)
	require.Len(t, report.Broken, 1)
	require.Equal(t, "actionId1", report.Broken[0].ActionID)
	require.Equal(t, "outputId1", report.Broken[0].OutputID)
	require.True(t, report.Broken[0].Removed)
	require.Contains(t, report.Broken[0].Error, "size mismatch")

	got := store.getOne("actionId1")
	require.True(t, got.Miss, "broken entry must have been evicted")
}

func TestStoreIntegrityCheck_DryRunReportsWithoutRemoving(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "hello-world", &now),
	}}))
	require.NoError(t, os.WriteFile(store.OutputFilename("outputId1"), []byte("short"), 0o600))

	report := store.IntegrityCheck(true)
	require.Len(t, report.Broken, 1)
	require.False(t, report.Broken[0].Removed)
	require.True(t, report.DryRun)

	require.Contains(t, store.index, "actionId1", "dry-run must not evict anything")
}

func TestStoreIntegrityCheck_ChecksSharedOutputIDOnceButReportsEveryActionID(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	item1 := testItem("actionId1", "sharedOutput", "hello-world", &now)
	item2 := testItem("actionId2", "sharedOutput", "hello-world", &now)
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{item1, item2}}))
	require.NoError(t, os.WriteFile(store.OutputFilename("sharedOutput"), []byte("short"), 0o600))

	report := store.IntegrityCheck(false)
	require.EqualValues(t, 2, report.Checked)
	require.Len(t, report.Broken, 2, "each ActionID referencing the broken OutputID must be reported")

	require.True(t, store.getOne("actionId1").Miss)
	require.True(t, store.getOne("actionId2").Miss)
}

// TestStoreRemoveIfUnchanged_SkipsEntryReplacedSinceItWasVerified guards against evicting an
// entry that a concurrent Put legitimately replaced while an IntegrityCheck scan was still
// running: eviction must only apply to the exact entry that was verified as broken.
func TestStoreRemoveIfUnchanged_SkipsEntryReplacedSinceItWasVerified(t *testing.T) {
	store, err := NewStore(t.TempDir())
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId1", "hello-world", &now),
	}}))

	staleSnapshot := store.index["actionId1"]

	// Simulate a concurrent Put replacing the entry after the scan snapshotted it.
	require.NoError(t, store.Put(cache.Response{Items: []cache.ResponseItem{
		testItem("actionId1", "outputId2", "a different body", &now),
	}}))

	removed := store.removeIfUnchanged("actionId1", staleSnapshot)
	require.False(t, removed, "must not evict an entry that no longer matches what was verified")

	current, ok := store.index["actionId1"]
	require.True(t, ok)
	require.Equal(t, "outputId2", current.OutputID, "the replacing entry must be untouched")
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
