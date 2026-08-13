package gocache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/recordpool"
)

// TestRestorePaths_SupplementsSmallChangesResultWithDefault covers the actual reported production
// symptom: eviction has no awareness of which manifests still reference an object, so a
// long-lived "changes" manifest can quietly hollow out over many eviction cycles (each Restore
// self-heals it down further) while a newer commit's manifest for the same build type stays
// intact. The resolved result should be supplemented with the default source rather than
// silently served small.
func TestRestorePaths_SupplementsSmallChangesResultWithDefault(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{ChangesID: "pr#1", BuildType: "unit"}, "ab/changes-only", "changes-only-payload")

	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-1", "p1")
	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-2", "p2")
	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-3", "p3")

	var restored []string
	sources, err := store.Restore(Request{ChangesID: "pr#1", BuildType: "unit"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"changes", "default"}, sources)
	require.ElementsMatch(t, []string{"ab/changes-only", "cd/newer-1", "cd/newer-2", "cd/newer-3"}, restored)
}

// TestRestorePaths_DefaultExcludesAlreadyMatchedManifest is a regression test for the actual
// reported production incident: the just-matched "changes" manifest is always among the most
// recently written for its build type (restorePaths just self-healed it moments ago), so without
// excluding it, it eats one of only defaultManifestSampleSize candidate slots for nothing -- all
// its content is already in the resolved result. That crowds out a genuinely large, independent
// manifest (commit-big below) that would otherwise have made the cut, silently serving a shrunken
// result even though substantial healthy content exists elsewhere for this build type.
func TestRestorePaths_DefaultExcludesAlreadyMatchedManifest(t *testing.T) {
	require.GreaterOrEqual(t, defaultManifestSampleSize, 3, "test assumes at least 3 sample slots")

	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	// Oldest: filler, never a candidate either way.
	saveItemForTest(t, store, Request{Commit: "commit-a", BuildType: "unit"}, "aa/a", "pa")
	// commit-big: large and independent, but older than commit-c/commit-d -- must lose its slot
	// to "changes" unless "changes" is excluded from the candidate pool.
	for i := 0; i < 20; i++ {
		saveItemForTest(t, store, Request{Commit: "commit-big", BuildType: "unit"}, fmt.Sprintf("bb/big-%d", i), fmt.Sprintf("pb%d", i))
	}
	saveItemForTest(t, store, Request{Commit: "commit-c", BuildType: "unit"}, "cc/c1", "pc1")
	saveItemForTest(t, store, Request{Commit: "commit-c", BuildType: "unit"}, "cc/c2", "pc2")
	saveItemForTest(t, store, Request{Commit: "commit-d", BuildType: "unit"}, "dd/d1", "pd1")
	saveItemForTest(t, store, Request{Commit: "commit-d", BuildType: "unit"}, "dd/d2", "pd2")
	// Written last, so it's the single most recently touched manifest for this build type --
	// and about to be matched (and thus self-healed, keeping it "most recent") by the Restore
	// call below.
	saveItemForTest(t, store, Request{ChangesID: "pr#1", BuildType: "unit"}, "ee/changes-only", "pe")

	var restored []string
	sources, err := store.Restore(Request{ChangesID: "pr#1", BuildType: "unit"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"changes", "default"}, sources)

	var bigFiles int
	for _, p := range restored {
		if strings.HasPrefix(p, "bb/big-") {
			bigFiles++
		}
	}
	require.Equal(t, 20, bigFiles, "commit-big's files must be reachable once \"changes\" is excluded from the candidate pool, not crowded out by it")
}

// TestRestorePaths_DoesNotSupplementNormallySizedResult confirms the heuristic doesn't kick in
// (and doesn't add "default" to sources) when the resolved result isn't actually small relative
// to the default source for the same build type.
func TestRestorePaths_DoesNotSupplementNormallySizedResult(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "commit-a", BuildType: "unit"}, "ab/a-1", "p1")
	saveItemForTest(t, store, Request{Commit: "commit-a", BuildType: "unit"}, "ab/a-2", "p2")

	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-1", "p1")
	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-2", "p2")

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit-a", BuildType: "unit"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.ElementsMatch(t, []string{"ab/a-1", "ab/a-2"}, restored)
}

// TestRestorePaths_SupplementsSmallBaseResultWithDefault confirms the small-result heuristic is
// source-agnostic: it compares the combined resolved result against the default source
// regardless of which of commit/parent/changes/base actually matched, so a small "base"-only
// result gets supplemented exactly like a small "changes"-only one does above -- no per-source
// duplication needed.
func TestRestorePaths_SupplementsSmallBaseResultWithDefault(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "base-commit", BuildType: "unit"}, "ab/base-only", "base-only-payload")

	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-1", "p1")
	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-2", "p2")
	saveItemForTest(t, store, Request{Commit: "commit-newer", BuildType: "unit"}, "cd/newer-3", "p3")

	var restored []string
	sources, err := store.Restore(Request{BaseCommit: "base-commit", BuildType: "unit"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"base", "default"}, sources)
	require.ElementsMatch(t, []string{"ab/base-only", "cd/newer-1", "cd/newer-2", "cd/newer-3"}, restored)
}

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

// TestStoreRestore_SurvivesSelfHealWriteFailure covers a disk-full-on-the-remote scenario: pruning
// a missing manifest entry marks the manifest "changed" and Restore tries to write the cleaned
// version back (see TestStoreRestore_PrunesMissingManifestEntries), but that write-back is pure
// housekeeping. If it fails (e.g. no space left on device), the already-computed, already-correct
// in-memory result must still be served rather than turning a successful restore into an error.
func TestStoreRestore_SurvivesSelfHealWriteFailure(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "commit123"}, "ab/cache-entry-a", "payload-a")
	saveItemForTest(t, store, Request{Commit: "commit123"}, "cd/cache-entry-b", "payload-b")

	manifestPath, err := store.commitManifestPath("commit123", "")
	require.NoError(t, err)
	require.NoError(t, os.Remove(store.objectPath("ab/cache-entry-a")))

	manifestDir := filepath.Dir(manifestPath)
	require.NoError(t, os.Chmod(manifestDir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(manifestDir, 0o750)) })

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit123"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"cd/cache-entry-b"}, restored)
}

// TestStorePutBody_RejectsTruncatedPlainFileWrite covers the actual reported bug: an interrupted
// upload whose body reader EOFs early must not be silently indexed as a complete object -- that
// produced a real production incident where restore-cache served a file a client couldn't
// decompress. The item here is deliberately >4096 bytes of incompressible content, so it's never
// pool-eligible (see TestStorePoolBody_RejectsTruncatedWrite for the pool-eligible path, which
// already had this guard).
func TestStorePutBody_RejectsTruncatedPlainFileWrite(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir)
	require.NoError(t, err)

	full := make([]byte, 5000)
	_, err = rand.New(rand.NewSource(1)).Read(full)
	require.NoError(t, err)

	item := FileItem{Path: "ab/truncated", Size: int64(len(full)), WireSize: int64(len(full))}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(io.LimitReader(bytes.NewReader(full), 3000)), nil
	})

	err = store.SaveItem(item)
	require.Error(t, err)
	require.Contains(t, err.Error(), "save body length 3000 does not match expected 5000")

	require.NoFileExists(t, store.objectPath("ab/truncated"))
	require.Equal(t, "0", store.Stats()["index"])
}

// TestStorePutBody_AcceptsZeroSizeItem is a regression test for a production panic: a zero-size
// item's body reader legitimately returns (nil, nil) (see FileItem.UncompressedBodyReader), and
// wrapping that nil reader unconditionally in countingReader for the truncation-length check
// above made writeAtomic's own `if rd != nil` guard moot -- io.Copy then called Read on a
// countingReader wrapping a nil io.Reader, panicking the whole process.
func TestStorePutBody_AcceptsZeroSizeItem(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir)
	require.NoError(t, err)

	item := FileItem{Path: "ab/empty", Size: 0}
	require.NoError(t, store.SaveItem(item))

	require.FileExists(t, store.objectPath("ab/empty"))
	require.Equal(t, "1", store.Stats()["index"])
}

// TestExistingPaths_ReportsWhatServerAlreadyHas covers the primitive save-cache's pre-upload
// dedup check is built on: a bulk existence check keyed by path, not by ActionID.
func TestExistingPaths_ReportsWhatServerAlreadyHas(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir)
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "commit123"}, "ab/cache-entry-a", "payload-a")

	existing := store.ExistingPaths([]string{"ab/cache-entry-a", "cd/missing"})
	require.Equal(t, []string{"ab/cache-entry-a"}, existing)
}

// TestStoreRestore_SkipsAndRemovesTruncatedPlainFileEntry covers a pre-existing corrupted entry
// (e.g. from before storePutBody validated write length): Restore must not serve a client bytes
// it can't use, and must drop the entry so a later restore doesn't hit it again either.
func TestStoreRestore_SkipsAndRemovesTruncatedPlainFileEntry(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "commit123"}, "ab/cache-entry-a", "payload-a")
	saveItemForTest(t, store, Request{Commit: "commit123"}, "cd/cache-entry-b", "payload-b")

	require.NoError(t, os.Truncate(store.objectPath("ab/cache-entry-a"), 3))

	var restored []string
	sources, err := store.Restore(Request{Commit: "commit123"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"commit"}, sources)
	require.Equal(t, []string{"cd/cache-entry-b"}, restored)

	// The broken entry must be gone from the index, not just skipped this one time.
	restored = nil
	_, err = store.Restore(Request{Commit: "commit123"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"cd/cache-entry-b"}, restored)
}

func saveItemForTest(t *testing.T, store *Store, req Request, path, body string) {
	t.Helper()

	item := FileItem{Path: path, Size: int64(len(body)), WireSize: int64(len(body))}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte(body))), nil
	})

	require.NoError(t, store.Save(req, Batch{Items: []FileItem{item}}))
}

// TestPoolPromotion_PacksSmallRecordsAndRoundTrips covers dynamic promotion end to end: a size
// stays plain files below the pool's breakeven, the item that crosses the threshold lands
// straight in a freshly created page instead of one more plain file, and the read path
// (responseItem's bodyReader closure, not DiskPath) serves the exact bytes back.
func TestPoolPromotion_PacksSmallRecordsAndRoundTrips(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	breakeven := store.pool.Breakeven()
	body := strings.Repeat("x", 50)
	// breakeven-1 plain files: the pool's internal count reaches breakeven-1, still below the
	// promotion threshold.
	for i := 0; i < breakeven-1; i++ {
		saveItemForTest(t, store, Request{Commit: "c"}, fmt.Sprintf("ab/item-%d", i), body)
	}

	pagePath := filepath.Join(dir, "records", recordpool.PageFileName(50, 1))
	require.NoFileExists(t, pagePath)

	// The breakeven-th same-size item crosses the threshold and lands in slot 0 of a fresh page.
	promotingPath := "cd/item-promoting"
	saveItemForTest(t, store, Request{Commit: "c"}, promotingPath, body)
	require.FileExists(t, pagePath)

	store.mu.Lock()
	ie := store.index[promotingPath]
	store.mu.Unlock()
	require.NotZero(t, ie.PoolPage)

	var restoredBody string
	_, err = store.Restore(Request{Commit: "c"}, func(item FileItem) {
		if item.Path != promotingPath {
			return
		}
		require.Empty(t, item.DiskPath, "pool-backed item must not carry a plain file path")
		rc, rerr := item.UncompressedBodyReader()
		require.NoError(t, rerr)
		data, rerr := io.ReadAll(rc)
		require.NoError(t, rerr)
		require.NoError(t, rc.Close())
		restoredBody = string(data)
	})
	require.NoError(t, err)
	require.Equal(t, body, restoredBody)
}

// TestPool_PageRemovedWhenFullyEmptied covers the self-cleaning side of the record pool: once
// every record in a page has been freed, the page file itself is deleted rather than left around
// hollow.
func TestPool_PageRemovedWhenFullyEmptied(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	breakeven := store.pool.Breakeven()
	body := strings.Repeat("y", 60)
	// breakeven-1 plain files, then a handful more that land in the pool once promoted --
	// exercises several slots in the same page, not just the one that triggered promotion.
	const extraPooledItems = 5
	for i := 0; i < breakeven-1+extraPooledItems; i++ {
		saveItemForTest(t, store, Request{Commit: "c2"}, fmt.Sprintf("ab/entry-%d", i), body)
	}

	pagePath := filepath.Join(dir, "records", recordpool.PageFileName(60, 1))
	require.FileExists(t, pagePath)

	_, err = store.Clear(Request{Commit: "c2"})
	require.NoError(t, err)

	require.NoFileExists(t, pagePath)
}

// TestCollectStaleManifests_RemovesOnlyManifestsOlderThanCutoff covers the age-based manifest
// collector: manifests are never touched again once their commit/PR goes cold, so unlike cache
// objects nothing else ever prunes them (see collectStaleManifests).
func TestCollectStaleManifests_RemovesOnlyManifestsOlderThanCutoff(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression(), WithManifestMaxAge(24*time.Hour))
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "stale-commit"}, "ab/stale", "payload")
	saveItemForTest(t, store, Request{Commit: "fresh-commit"}, "cd/fresh", "payload")

	staleManifest, err := store.commitManifestPath("stale-commit", "")
	require.NoError(t, err)
	freshManifest, err := store.commitManifestPath("fresh-commit", "")
	require.NoError(t, err)

	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(staleManifest, old, old))

	store.collectStaleManifests()

	_, err = os.Stat(staleManifest)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(freshManifest)
	require.NoError(t, err)
}

// TestStoreRestore_FallsBackToDefaultManifestWhenNoSourceMatches covers a cold-start scenario:
// after a long pause with nothing relevant built on the target branch, or on a brand new build
// type, none of commit/parent/changes/base has a manifest yet. Restore falls back to the default
// source (see loadDefaultManifestPaths) for this build type, from any unrelated commit or PR.
func TestStoreRestore_FallsBackToDefaultManifestWhenNoSourceMatches(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "unrelated-commit"}, "ab/cache-entry-a", "payload")

	var restored []string
	sources, err := store.Restore(Request{ParentCommit: "missing-parent", BaseCommit: "missing-base"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"default"}, sources)
	require.Equal(t, []string{"ab/cache-entry-a"}, restored)
}

// TestStoreRestore_DefaultFallbackDoesNotOverrideARealMatch guards against the fallback firing
// even when a normal source already matched - it must only ever apply when nothing else did.
func TestStoreRestore_DefaultFallbackDoesNotOverrideARealMatch(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "unrelated-commit"}, "ab/unrelated", "payload-1")
	saveItemForTest(t, store, Request{Commit: "base123"}, "cd/relevant", "payload-2")

	sources, err := store.RestoreSources(Request{BaseCommit: "base123"})
	require.NoError(t, err)
	require.Equal(t, []string{"base"}, sources)
}

// TestStoreRestore_DefaultFallbackIsScopedToBuildType guards against leaking cache relevance
// across unrelated build types, which typically have different dependency footprints.
func TestStoreRestore_DefaultFallbackIsScopedToBuildType(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "unrelated-commit", BuildType: "other-build-type"}, "ab/entry", "payload")

	var restored []string
	sources, err := store.Restore(Request{ParentCommit: "missing-parent", BuildType: "this-build-type"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"none"}, sources)
	require.Empty(t, restored, "a manifest from a different build type must not be used as a fallback")
}

// TestStoreRestore_DefaultFallbackUnionsMostRecentlyWrittenManifests confirms the default source
// unions defaultManifestSampleSize of a build type's most recently written manifests rather than
// just a single newest one -- a lone newest pick is fragile (e.g. a brand-new PR's first save
// would count as "newest" despite being nowhere near representative), so unioning a handful is
// more robust. With only two manifests here (fewer than the sample size), both are included.
func TestStoreRestore_DefaultFallbackUnionsMostRecentlyWrittenManifests(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "older-commit"}, "ab/older", "payload-1")
	saveItemForTest(t, store, Request{Commit: "newer-commit"}, "cd/newer", "payload-2")

	olderPath, err := store.commitManifestPath("older-commit", "")
	require.NoError(t, err)
	newerPath, err := store.commitManifestPath("newer-commit", "")
	require.NoError(t, err)

	older := time.Now().Add(-time.Hour)
	newer := time.Now()
	require.NoError(t, os.Chtimes(olderPath, older, older))
	require.NoError(t, os.Chtimes(newerPath, newer, newer))

	var restored []string
	sources, err := store.Restore(Request{ParentCommit: "missing-parent"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"default"}, sources)
	require.ElementsMatch(t, []string{"ab/older", "cd/newer"}, restored)
}

// TestStoreRestore_DefaultFallbackExcludesManifestsOlderThanSampleSize confirms the union is
// bounded: with more manifests than defaultManifestSampleSize, the oldest ones are left out
// rather than the default source growing without limit.
func TestStoreRestore_DefaultFallbackExcludesManifestsOlderThanSampleSize(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir, WithCompression())
	require.NoError(t, err)

	require.Greater(t, defaultManifestSampleSize, 0)
	commits := make([]string, defaultManifestSampleSize+1)
	for i := range commits {
		commits[i] = fmt.Sprintf("commit-%d", i)
		saveItemForTest(t, store, Request{Commit: commits[i]}, fmt.Sprintf("ab/entry-%d", i), fmt.Sprintf("payload-%d", i))

		manifestPath, err := store.commitManifestPath(commits[i], "")
		require.NoError(t, err)
		// Strictly increasing mtimes, oldest first, so commits[0] is the one sample size should
		// exclude.
		modTime := time.Now().Add(time.Duration(i) * time.Hour)
		require.NoError(t, os.Chtimes(manifestPath, modTime, modTime))
	}

	var restored []string
	sources, err := store.Restore(Request{ParentCommit: "missing-parent"}, func(item FileItem) {
		restored = append(restored, item.Path)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"default"}, sources)
	require.NotContains(t, restored, "ab/entry-0", "the oldest manifest should be excluded once there are more than defaultManifestSampleSize candidates")
	require.Len(t, restored, defaultManifestSampleSize)
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

// TestReadStream_DrainsSkippedItemBody guards against stream desync: if a
// callback (e.g. Store.putOne skipping an oversized item) returns without
// reading an item's body, ReadStream must still discard the unread bytes
// itself so the next item's header is read from the correct offset.
func TestReadStream_DrainsSkippedItemBody(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf)

	skipped := FileItem{Path: "large", Size: 10, WireSize: 10}
	skipped.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("1234567890")), nil
	})
	require.NoError(t, sw.WriteItem(skipped))

	kept := FileItem{Path: "small", Size: 4, WireSize: 4}
	kept.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("abcd")), nil
	})
	require.NoError(t, sw.WriteItem(kept))
	require.NoError(t, sw.Close())

	var paths []string
	var bodies []string
	_, err := ReadStream(&buf, func(item FileItem, body io.Reader) error {
		if item.Path == "large" {
			// Simulate putOne skipping an oversized item without reading its body.
			return nil
		}
		paths = append(paths, item.Path)
		data, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		bodies = append(bodies, string(data))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"small"}, paths)
	require.Equal(t, []string{"abcd"}, bodies)
}

// TestReadStream_AbortsOnShortItemInsteadOfCorruptingLaterItems guards against silent stream
// desync: if an item's actual body on the wire falls short of its declared WireSize (e.g. a
// stale index entry vs. the real object), ReadStream must fail loudly on that item rather than
// silently continuing to read every later item from the wrong offset.
func TestReadStream_AbortsOnShortItemInsteadOfCorruptingLaterItems(t *testing.T) {
	var buf bytes.Buffer

	writeRawItem := func(item FileItem, body string) {
		jsonData, err := json.Marshal(item)
		require.NoError(t, err)
		require.NoError(t, binary.Write(&buf, binary.BigEndian, int32(len(jsonData))))
		buf.Write(jsonData)
		buf.WriteString(body)
	}

	writeRawItem(FileItem{Path: "first", Size: 5, WireSize: 5}, "hello")
	// second declares 10 bytes but the stream only actually has 4 - as if the index entry
	// overstated the real object size - then ends entirely.
	writeRawItem(FileItem{Path: "second", Size: 10, WireSize: 10}, "shrt")

	var seen []string
	_, err := ReadStream(&buf, func(item FileItem, body io.Reader) error {
		seen = append(seen, item.Path)
		_, _ = io.ReadAll(body) //nolint:errcheck // best-effort, like a real decompressing consumer; only ReadStream's own error matters here.
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrShortRead)
	require.Equal(t, []string{"first", "second"}, seen)
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

// TestInspect_SupportsBaseCommitScope is a regression test for a real gap: targetManifestPaths
// (shared by Inspect and Clear) never handled req.BaseCommit at all, even though Request has the
// field and restorePaths treats it as an equal-weight source -- there was no way to inspect a
// base commit's manifest size via the HTTP /inspect endpoint to check whether it's been hollowed
// out by eviction.
func TestInspect_SupportsBaseCommitScope(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStore(dir)
	require.NoError(t, err)

	saveItemForTest(t, store, Request{Commit: "base-commit", BuildType: "unit"}, "ab/base-a", "payload-a")
	saveItemForTest(t, store, Request{Commit: "base-commit", BuildType: "unit"}, "cd/base-b", "payload-b")

	// A second, unrelated commit manifest for the same build type: proves base-commit really
	// scopes to just its own manifest, rather than silently falling through to "every manifest
	// for this build type" (which would count this too, and wouldn't have caught the bug where
	// the aggregate-shortcut branch's guard forgot to also exclude BaseCommit).
	saveItemForTest(t, store, Request{Commit: "other-commit", BuildType: "unit"}, "ef/other", "payload-c")

	stats, err := store.Inspect(Request{BaseCommit: "base-commit", BuildType: "unit"})
	require.NoError(t, err)
	require.Equal(t, 1, stats.ManifestsCount)
	require.Equal(t, 2, stats.FilesCount)
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
