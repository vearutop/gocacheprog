package gocache

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreRestore_PrunesMissingManifestEntries(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	modTime := time.Date(2026, time.May, 14, 8, 0, 0, 0, time.UTC)
	item := FileItem{
		Path:     "ab/cache-entry-a",
		Size:     int64(len("payload")),
		WireSize: int64(len("payload")),
		ModTime:  &modTime,
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("payload"))), nil
	})

	require.NoError(t, store.Save(Request{Commit: "commit123"}, Batch{Items: []FileItem{item}}))

	manifestPath, err := store.commitManifestPath("commit123", "")
	require.NoError(t, err)
	objectPath := store.objectPath(item.Path)
	require.NoError(t, os.Remove(objectPath))

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit123"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Empty(t, restored)

	manifestBody, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.Equal(t, "", string(manifestBody))
}

func TestCollectFilesToSave_SkipsRestoredPaths(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ab"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ab", "restored-a"), []byte("restored"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ab", "new-a"), []byte("new"), 0o600))

	require.NoError(t, WriteRestoredPaths(dir, []string{"ab/restored-a"}))

	restoredPaths, err := ReadRestoredPaths(dir)
	require.NoError(t, err)

	batch, err := CollectFilesToSave(dir, restoredPaths, 0)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
	require.Equal(t, "ab/new-a", batch.Items[0].Path)
}

func TestCollectFilesToSave_SkipsOversizedFiles(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ab"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ab", "small"), []byte("1234"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ab", "large"), []byte("123456"), 0o600))

	batch, err := CollectFilesToSave(dir, map[string]struct{}{}, 4)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
	require.Equal(t, "ab/small", batch.Items[0].Path)
}

func TestCollectAndRestore_PreservesExecutableMode(t *testing.T) {
	cacheDir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "ab"), 0o750))
	originalPath := filepath.Join(cacheDir, "ab", "covdata")
	require.NoError(t, os.WriteFile(originalPath, []byte("binary"), 0o700))

	batch, err := CollectFilesToSave(cacheDir, map[string]struct{}{}, 0)
	require.NoError(t, err)
	require.Len(t, batch.Items, 1)
	require.Equal(t, uint32(0o700), batch.Items[0].Mode)

	rd, err := os.Open(originalPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rd.Close())
	}()

	restoreDir := t.TempDir()
	require.NoError(t, RestoreToDir(restoreDir, batch.Items[0], rd))

	info, err := os.Stat(filepath.Join(restoreDir, "ab", "covdata"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestRestore_RespectsMaxFileBytes(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	for _, tc := range []struct {
		path string
		body string
	}{
		{path: "small", body: "1234"},
		{path: "large", body: "1234567890"},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, store.Save(Request{Commit: "commit123"}, Batch{Items: []FileItem{item}}))
	}

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit123", MaxFileBytes: 5}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"small"}, restored)
}

func TestRestore_RespectsRestoreLimitBytesOrdering(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	now := time.Date(2026, time.June, 3, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		path string
		body string
		at   time.Time
	}{
		{path: "older", body: "1234", at: now.Add(-2 * time.Minute)},
		{path: "new-large", body: "123456", at: now},
		{path: "new-small", body: "1234", at: now},
		{path: "new-tiny", body: "12", at: now},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
			ModTime:  &tc.at,
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, store.Save(Request{Commit: "commit123"}, Batch{Items: []FileItem{item}}))
	}

	store.mu.Lock()
	for path, at := range map[string]time.Time{
		"older":     now.Add(-2 * time.Minute),
		"new-large": now,
		"new-small": now,
		"new-tiny":  now,
	} {
		ie := store.index[path]
		ie.ModTimeMicro = at.UnixMicro()
		store.index[path] = ie
	}
	store.mu.Unlock()

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit123", RestoreLimitBytes: 6}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"new-tiny", "new-small"}, restored)
}

func TestRestore_RespectsMaxFileBytesBeforeRestoreLimitBytes(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	now := time.Date(2026, time.June, 3, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		path string
		body string
		at   time.Time
	}{
		{path: "too-big", body: "1234567", at: now},
		{path: "fit-a", body: "1234", at: now},
		{path: "fit-b", body: "12", at: now},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
			ModTime:  &tc.at,
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, store.Save(Request{Commit: "commit123"}, Batch{Items: []FileItem{item}}))
	}

	store.mu.Lock()
	for path := range store.index {
		ie := store.index[path]
		ie.ModTimeMicro = now.UnixMicro()
		store.index[path] = ie
	}
	store.mu.Unlock()

	var restored []string
	_, err = store.Restore(Request{Commit: "commit123", MaxFileBytes: 5, RestoreLimitBytes: 6}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"fit-b", "fit-a"}, restored)
}

func TestStoreMaxFileBytes_SkipsSaveAndRestore(t *testing.T) {
	dir := t.TempDir()

	unlimitedStore, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	for _, tc := range []struct {
		path string
		body string
	}{
		{path: "small", body: "1234"},
		{path: "large", body: "1234567890"},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, unlimitedStore.Save(Request{Commit: "commit123"}, Batch{Items: []FileItem{item}}))
	}
	require.NoError(t, unlimitedStore.Close())

	limitedStore, err := NewStore(dir, WithCompression(), WithMaxFileBytes(5))
	require.NoError(t, err)

	var restored []string
	sources, err := limitedStore.Restore(Request{Commit: "commit123"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"small"}, restored)

	saveLimitedStore, err := NewStore(t.TempDir(), WithCompression(), WithMaxFileBytes(5))
	require.NoError(t, err)

	for _, tc := range []struct {
		path string
		body string
	}{
		{path: "small", body: "1234"},
		{path: "large", body: "1234567890"},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, saveLimitedStore.Save(Request{Commit: "commit456"}, Batch{Items: []FileItem{item}}))
	}

	var restoredAfterSave []string
	sources, err = saveLimitedStore.Restore(Request{Commit: "commit456"}, func(item FileItem) {
		restoredAfterSave = append(restoredAfterSave, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"small"}, restoredAfterSave)
}

func TestMergeSavedPaths_ChangesIDMerges(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	req := Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}
	for _, relPath := range []string{"A", "B", "C", "D", "E"} {
		relPath := relPath
		item := FileItem{
			Path:     relPath,
			Size:     int64(len(relPath)),
			WireSize: int64(len(relPath)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(relPath))), nil
		})
		require.NoError(t, store.SaveItem(item))
	}

	require.NoError(t, store.MergeSavedPaths(req, []string{"A", "B", "C"}))
	require.NoError(t, store.MergeSavedPaths(req, []string{"C", "D", "E"}))

	commitManifestPath, err := store.commitManifestPath("commit123", "unit")
	require.NoError(t, err)
	changesManifestPath, err := store.changesManifestPath("repo/pr-123", "unit")
	require.NoError(t, err)

	commitBody, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	require.Equal(t, "A\nB\nC\nD\nE\n", string(commitBody))

	changesBody, err := os.ReadFile(changesManifestPath)
	require.NoError(t, err)
	require.Equal(t, "A\nB\nC\nD\nE\n", string(changesBody))
}

func TestFinalizeUpload_MergesAccumulatedChunkPaths(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	req := Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}
	for _, relPath := range []string{"A", "B", "C"} {
		relPath := relPath
		item := FileItem{
			Path:     relPath,
			Size:     int64(len(relPath)),
			WireSize: int64(len(relPath)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(relPath))), nil
		})
		require.NoError(t, store.SaveItem(item))
	}

	require.NoError(t, store.AppendUploadPaths("upload-1", []string{"A", "B"}))
	require.NoError(t, store.AppendUploadPaths("upload-1", []string{"B", "C"}))
	require.NoError(t, store.FinalizeUpload(req, "upload-1"))

	commitManifestPath, err := store.commitManifestPath("commit123", "unit")
	require.NoError(t, err)
	changesManifestPath, err := store.changesManifestPath("repo/pr-123", "unit")
	require.NoError(t, err)
	uploadPath, err := store.uploadSessionPath("upload-1")
	require.NoError(t, err)

	commitBody, err := os.ReadFile(commitManifestPath)
	require.NoError(t, err)
	require.Equal(t, "A\nB\nC\n", string(commitBody))

	changesBody, err := os.ReadFile(changesManifestPath)
	require.NoError(t, err)
	require.Equal(t, "A\nB\nC\n", string(changesBody))

	_, err = os.Stat(uploadPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestClear_RemovesTargetIdentityAndUnreferencedObjects(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	for _, relPath := range []string{"A", "B", "C"} {
		relPath := relPath
		item := FileItem{
			Path:     relPath,
			Size:     int64(len(relPath)),
			WireSize: int64(len(relPath)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(relPath))), nil
		})
		require.NoError(t, store.SaveItem(item))
	}

	require.NoError(t, store.MergeSavedPaths(Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}, []string{"A", "B"}))
	require.NoError(t, store.MergeSavedPaths(Request{ChangesID: "repo/pr-999", BuildType: "unit"}, []string{"B", "C"}))

	stats, err := store.Clear(Request{ChangesID: "repo/pr-123", BuildType: "unit"})
	require.NoError(t, err)
	require.Equal(t, 1, stats.ManifestsDeleted)
	require.Equal(t, 0, stats.ObjectsDeleted)
	require.Equal(t, 2, stats.ObjectsKept)

	_, err = os.Stat(store.objectPath("A"))
	require.NoError(t, err)
	_, err = os.Stat(store.objectPath("B"))
	require.NoError(t, err)
	_, err = os.Stat(store.objectPath("C"))
	require.NoError(t, err)

	changesManifestPath, err := store.changesManifestPath("repo/pr-123", "unit")
	require.NoError(t, err)
	_, err = os.Stat(changesManifestPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestClear_BuildTypeScopeRemovesOnlyThatScope(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	for _, relPath := range []string{"A", "B", "C"} {
		relPath := relPath
		item := FileItem{
			Path:     relPath,
			Size:     int64(len(relPath)),
			WireSize: int64(len(relPath)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(relPath))), nil
		})
		require.NoError(t, store.SaveItem(item))
	}

	require.NoError(t, store.MergeSavedPaths(Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}, []string{"A", "B"}))
	require.NoError(t, store.MergeSavedPaths(Request{Commit: "commit999", BuildType: "integration"}, []string{"B", "C"}))

	stats, err := store.Clear(Request{BuildType: "unit"})
	require.NoError(t, err)
	require.Equal(t, 2, stats.ManifestsDeleted)
	require.Equal(t, 1, stats.ObjectsDeleted)
	require.Equal(t, 1, stats.ObjectsKept)

	_, err = os.Stat(store.objectPath("A"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(store.objectPath("B"))
	require.NoError(t, err)
	_, err = os.Stat(store.objectPath("C"))
	require.NoError(t, err)

	unitCommitManifestPath, err := store.commitManifestPath("commit123", "unit")
	require.NoError(t, err)
	_, err = os.Stat(unitCommitManifestPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	integrationCommitManifestPath, err := store.commitManifestPath("commit999", "integration")
	require.NoError(t, err)
	_, err = os.Stat(integrationCommitManifestPath)
	require.NoError(t, err)
}

func TestInspect_SummarizesScope(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	for _, tc := range []struct {
		path string
		body string
	}{
		{path: "A", body: strings.Repeat("a", 100)},
		{path: "B", body: strings.Repeat("b", 95)},
		{path: "C", body: strings.Repeat("c", 10)},
	} {
		tc := tc
		item := FileItem{
			Path:     tc.path,
			Size:     int64(len(tc.body)),
			WireSize: int64(len(tc.body)),
		}
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(tc.body))), nil
		})
		require.NoError(t, store.SaveItem(item))
	}

	require.NoError(t, store.MergeSavedPaths(Request{Commit: "commit123", ChangesID: "repo/pr-123", BuildType: "unit"}, []string{"A", "B", "C"}))

	stats, err := store.Inspect(Request{ChangesID: "repo/pr-123", BuildType: "unit"})
	require.NoError(t, err)
	require.Equal(t, 1, stats.ManifestsCount)
	require.Equal(t, 3, stats.FilesCount)
	require.Equal(t, int64(205), stats.UncompressedBytes)
	require.Equal(t, int64(205), stats.CompressedBytes)
	require.Equal(t, int64(100), stats.MaxFileSize)
	require.Equal(t, 2, stats.MaxBandFilesCount)
	require.Equal(t, int64(195), stats.MaxBandUncompressedBytes)
	require.Equal(t, int64(195), stats.MaxBandCompressedBytes)
}

func TestStoreStartup_PrunesExpiredEntriesByMaxAge(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression(), WithMaxAge(0))
	require.NoError(t, err)

	oldTime := time.Now().UTC().Add(-72 * time.Hour)
	item := FileItem{
		Path:     "expired",
		Size:     int64(len("payload")),
		WireSize: int64(len("payload")),
		ModTime:  &oldTime,
	}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("payload"))), nil
	})
	require.NoError(t, store.SaveItem(item))
	store.mu.Lock()
	ie := store.index["expired"]
	ie.ModTimeMicro = oldTime.UnixMicro()
	store.index["expired"] = ie
	store.dirty = true
	store.mu.Unlock()
	require.NoError(t, os.Chtimes(store.objectPath("expired"), oldTime, oldTime))
	require.NoError(t, store.Close())

	store, err = NewStore(dir, WithCompression(), WithMaxAge(48*time.Hour))
	require.NoError(t, err)

	_, err = os.Stat(store.objectPath("expired"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NotContains(t, store.index, "expired")
}

func TestStoreSave_SchedulesAgeEviction(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression(), WithMaxAge(48*time.Hour), WithEvictionDelay(0))
	require.NoError(t, err)

	oldTime := time.Now().UTC().Add(-72 * time.Hour)
	expired := FileItem{
		Path:     "expired",
		Size:     int64(len("payload")),
		WireSize: int64(len("payload")),
		ModTime:  &oldTime,
	}
	expired.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("payload"))), nil
	})
	require.NoError(t, store.SaveItem(expired))
	store.mu.Lock()
	ie := store.index["expired"]
	ie.ModTimeMicro = oldTime.UnixMicro()
	store.index["expired"] = ie
	store.dirty = true
	store.mu.Unlock()
	require.NoError(t, os.Chtimes(store.objectPath("expired"), oldTime, oldTime))

	fresh := FileItem{
		Path:     "fresh",
		Size:     int64(len("fresh")),
		WireSize: int64(len("fresh")),
	}
	fresh.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("fresh"))), nil
	})
	require.NoError(t, store.SaveItem(fresh))

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		_, ok := store.index["expired"]
		return !ok
	}, time.Second, 10*time.Millisecond)
}
