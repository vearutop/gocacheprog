package local

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadLocalGocacheStats_MissingFileReturnsEmpty(t *testing.T) {
	stats, err := loadLocalGocacheStats(t.TempDir())
	require.NoError(t, err)
	require.NotNil(t, stats.BuildTypes)
	require.Empty(t, stats.BuildTypes)
}

func TestLoadLocalGocacheStats_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, localGocacheStatsFilename), []byte("not json"), 0o600))

	_, err := loadLocalGocacheStats(dir)
	require.Error(t, err)
}

func TestSaveAndLoadLocalGocacheStats_RoundTrips(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)

	stats := localGocacheStats{BuildTypes: map[string]localGocacheBuildTypeStats{
		"owner-repo-unit": {First: now, Last: now, Count: 3},
	}}
	require.NoError(t, saveLocalGocacheStats(dir, stats))

	loaded, err := loadLocalGocacheStats(dir)
	require.NoError(t, err)
	require.Equal(t, int64(3), loaded.BuildTypes["owner-repo-unit"].Count)
	require.True(t, now.Equal(loaded.BuildTypes["owner-repo-unit"].First))

	// no leftover temp file
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, localGocacheStatsFilename, entries[0].Name())
}

func TestRecordLocalGocacheUsage_FirstUse(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	stats, err := recordLocalGocacheUsage(dir, "owner-repo-unit", now)
	require.NoError(t, err)

	entry := stats.BuildTypes["owner-repo-unit"]
	require.Equal(t, int64(1), entry.Count)
	require.True(t, now.Equal(entry.First))
	require.True(t, now.Equal(entry.Last))
}

func TestRecordLocalGocacheUsage_AccumulatesAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	first := time.Now().UTC()
	second := first.Add(time.Hour)

	_, err := recordLocalGocacheUsage(dir, "owner-repo-unit", first)
	require.NoError(t, err)
	stats, err := recordLocalGocacheUsage(dir, "owner-repo-unit", second)
	require.NoError(t, err)

	entry := stats.BuildTypes["owner-repo-unit"]
	require.Equal(t, int64(2), entry.Count)
	require.True(t, first.Equal(entry.First), "first-used time should not change on subsequent calls")
	require.True(t, second.Equal(entry.Last))
}

func TestRecordLocalGocacheUsage_ReloadsFromDiskInsteadOfOverwriting(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	// Simulate a concurrent job writing a different build_type's entry directly to disk between
	// two recordLocalGocacheUsage calls in this job.
	_, err := recordLocalGocacheUsage(dir, "owner-repo-unit", now)
	require.NoError(t, err)

	external, err := loadLocalGocacheStats(dir)
	require.NoError(t, err)
	external.BuildTypes["other-repo-race"] = localGocacheBuildTypeStats{First: now, Last: now, Count: 5}
	require.NoError(t, saveLocalGocacheStats(dir, external))

	stats, err := recordLocalGocacheUsage(dir, "owner-repo-unit", now.Add(time.Minute))
	require.NoError(t, err)

	require.Equal(t, int64(2), stats.BuildTypes["owner-repo-unit"].Count)
	require.Equal(t, int64(5), stats.BuildTypes["other-repo-race"].Count, "concurrently written entry should survive the reload-before-write")
}

func TestRecordLocalGocacheUsage_EmptyBuildTypeUsesDefaultKey(t *testing.T) {
	dir := t.TempDir()
	stats, err := recordLocalGocacheUsage(dir, "", time.Now().UTC())
	require.NoError(t, err)
	require.Contains(t, stats.BuildTypes, localGocacheStatsKey)
}

func TestLogLocalGocacheStats_EmptyStats(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	logLocalGocacheStats("github-actions-init", localGocacheStats{})
	require.Contains(t, buf.String(), "no recorded usage yet")
}

func TestLogLocalGocacheStats_MostRecentFirst(t *testing.T) {
	older := time.Now().UTC().Add(-time.Hour)
	newer := time.Now().UTC()

	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	logLocalGocacheStats("github-actions-init", localGocacheStats{BuildTypes: map[string]localGocacheBuildTypeStats{
		"old-type": {First: older, Last: older, Count: 1},
		"new-type": {First: newer, Last: newer, Count: 2},
	}})

	out := buf.String()
	require.Less(t, indexOf(t, out, "new-type"), indexOf(t, out, "old-type"))
}

func indexOf(t *testing.T, s, substr string) int {
	t.Helper()
	i := strings.Index(s, substr)
	require.GreaterOrEqual(t, i, 0, "expected %q to contain %q", s, substr)
	return i
}

func mustScanCacheDir(t *testing.T, dir string) []cacheFileEntry {
	t.Helper()
	entries, err := scanCacheDir(dir)
	require.NoError(t, err)
	return entries
}

func TestEvictOldestUntilFits_NoOpWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), make([]byte, 100), 0o600))

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), 0)

	_, err := os.Stat(filepath.Join(dir, "a"))
	require.NoError(t, err)
}

func TestEvictOldestUntilFits_NoOpUnderLimit(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), make([]byte, 100), 0o600))

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), 1000)

	_, err := os.Stat(filepath.Join(dir, "a"))
	require.NoError(t, err)
}

func TestEvictOldestUntilFits_RemovesOldestFirstBelow90Percent(t *testing.T) {
	dir := t.TempDir()

	// Four 100-byte files, oldest ("a") to newest ("d"); limit of 250 bytes means we must evict
	// down to strictly below a 90%-of-limit target of 225 bytes, i.e. evict "a" and "b"
	// (400 -> 200), since evicting only "a" would leave 300, still over the 225-byte target.
	names := []string{"a", "b", "c", "d"}
	now := time.Now()
	for i, name := range names {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, make([]byte, 100), 0o600))
		modTime := now.Add(time.Duration(i) * time.Minute)
		require.NoError(t, os.Chtimes(path, modTime, modTime))
	}

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), 250)

	for _, name := range []string{"a", "b"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(err), "%s should have been evicted", name)
	}
	for _, name := range []string{"c", "d"} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "%s should have been kept", name)
	}
}

func TestEvictOldestUntilFits_OverLimitEndsUpStrictlyUnder90Percent(t *testing.T) {
	dir := t.TempDir()

	const unit = int64(1) << 20 // 1MiB; kept small so the test runs fast, not gigabytes on disk.
	now := time.Now()

	// Eleven 1MiB files (11MiB total) against a 10MiB limit: total exceeds the limit, so
	// eviction must trigger and land strictly below the 90%-of-limit target of 9MiB — e.g. a
	// real 10GB limit should trim the cache to slightly less than 9GB, never merely at or below it.
	for i := range 11 {
		path := filepath.Join(dir, fmt.Sprintf("f%d", i))
		require.NoError(t, os.WriteFile(path, make([]byte, unit), 0o600))
		modTime := now.Add(time.Duration(i) * time.Minute)
		require.NoError(t, os.Chtimes(path, modTime, modTime))
	}

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), 10*unit)

	_, size, err := DirStats(dir)
	require.NoError(t, err)
	require.Less(t, size, 9*unit, "cache dir should end up strictly below the 90%% target after an over-limit trim")
}

func TestEvictOldestUntilFits_ExactlyAtLimitDoesNotEvict(t *testing.T) {
	dir := t.TempDir()

	const unit = int64(1) << 20
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), make([]byte, unit), 0o600))

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), unit)

	_, err := os.Stat(filepath.Join(dir, "a"))
	require.NoError(t, err, "being exactly at the limit is not \"too big\"; nothing should be evicted")
}

func TestEvictOldestUntilFits_SkipsLockAndStatsFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()

	for _, name := range []string{localGocacheLockFilename, localGocacheStatsFilename, "a"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, make([]byte, 100), 0o600))
		require.NoError(t, os.Chtimes(path, now, now))
	}

	evictOldestUntilFits(dir, mustScanCacheDir(t, dir), 1)

	for _, name := range []string{localGocacheLockFilename, localGocacheStatsFilename} {
		_, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, "%s must never be evicted", name)
	}
	_, err := os.Stat(filepath.Join(dir, "a"))
	require.True(t, os.IsNotExist(err), "a should have been evicted")
}
