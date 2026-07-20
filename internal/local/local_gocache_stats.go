package local

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// localGocacheStatsFilename is the per-cache-dir usage tracker for -github-actions-init/-done's
// local-gocache mode: a small JSON file recording, per build_type, how many jobs on this host
// have used the cache dir, and when. It's the only bit of bookkeeping local-gocache mode keeps,
// since otherwise a persistent-home self-hosted runner's GOCACHE is an opaque blob with no
// visibility into which build types (and, transitively, which repos/jobs) actually share it.
const localGocacheStatsFilename = "gocacheprog.json"

// localGocacheLockFilename is cmd/go's own lock file at the root of GOCACHE; it must never be
// treated as an evictable cache object.
const localGocacheLockFilename = "lock"

// localGocacheBuildTypeStats is one build_type's entry in localGocacheStatsFilename.
type localGocacheBuildTypeStats struct {
	First time.Time `json:"first"`
	Last  time.Time `json:"last"`
	Count int64     `json:"count"`
}

// localGocacheStats is the full contents of localGocacheStatsFilename.
type localGocacheStats struct {
	BuildTypes map[string]localGocacheBuildTypeStats `json:"build_types"`
}

// localGocacheStatsKey is the map key used for an empty build_type, so the stats file never
// carries a blank JSON object key.
const localGocacheStatsKey = "(default)"

func localGocacheStatsBuildTypeKey(buildType string) string {
	if buildType == "" {
		return localGocacheStatsKey
	}

	return buildType
}

// loadLocalGocacheStats reads localGocacheStatsFilename from cacheDir. A missing file is not an
// error: it just means no job has recorded usage yet.
func loadLocalGocacheStats(cacheDir string) (localGocacheStats, error) {
	data, err := os.ReadFile(filepath.Join(cacheDir, localGocacheStatsFilename)) //nolint:gosec // cacheDir is the configured cache dir.
	if err != nil {
		if os.IsNotExist(err) {
			return localGocacheStats{BuildTypes: map[string]localGocacheBuildTypeStats{}}, nil
		}

		return localGocacheStats{}, fmt.Errorf("read %s: %w", localGocacheStatsFilename, err)
	}

	var stats localGocacheStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return localGocacheStats{}, fmt.Errorf("parse %s: %w", localGocacheStatsFilename, err)
	}
	if stats.BuildTypes == nil {
		stats.BuildTypes = map[string]localGocacheBuildTypeStats{}
	}

	return stats, nil
}

// saveLocalGocacheStats writes stats to localGocacheStatsFilename via a per-process temp file
// plus rename, so a reader never observes a half-written file, and two processes racing to save
// can't corrupt each other's write mid-flight (each writes its own temp file; the rename that
// lands last simply wins).
func saveLocalGocacheStats(cacheDir string, stats localGocacheStats) error {
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", localGocacheStatsFilename, err)
	}

	path := filepath.Join(cacheDir, localGocacheStatsFilename)
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())

	if err := os.WriteFile(tmpPath, data, 0o600); err != nil { //nolint:gosec // tmpPath is derived from the configured cache dir.
		return fmt.Errorf("write %s: %w", localGocacheStatsFilename, err)
	}

	if err := os.Rename(tmpPath, path); err != nil { //nolint:gosec // path/tmpPath are derived from the configured cache dir.
		return fmt.Errorf("rename %s: %w", localGocacheStatsFilename, err)
	}

	return nil
}

// recordLocalGocacheUsage reloads localGocacheStatsFilename from disk, bumps buildType's
// count/last-used (and first-used, if this is its first appearance), and saves it back. It
// reloads immediately before writing rather than reusing a stats value read earlier in the job,
// since a persistent-home self-hosted runner may run several jobs against the same cache dir
// concurrently — this keeps the race window as small as it can be without a real file lock.
func recordLocalGocacheUsage(cacheDir, buildType string, now time.Time) (localGocacheStats, error) {
	stats, err := loadLocalGocacheStats(cacheDir)
	if err != nil {
		return localGocacheStats{}, err
	}

	key := localGocacheStatsBuildTypeKey(buildType)

	entry := stats.BuildTypes[key]
	if entry.First.IsZero() {
		entry.First = now
	}
	entry.Last = now
	entry.Count++
	stats.BuildTypes[key] = entry

	if err := saveLocalGocacheStats(cacheDir, stats); err != nil {
		return localGocacheStats{}, err
	}

	return stats, nil
}

// logLocalGocacheStats logs every build_type's usage, most recently used first.
func logLocalGocacheStats(prefix string, stats localGocacheStats) {
	if len(stats.BuildTypes) == 0 {
		log.Printf("%s: %s has no recorded usage yet", prefix, localGocacheStatsFilename)
		return
	}

	type row struct {
		buildType string
		stats     localGocacheBuildTypeStats
	}

	rows := make([]row, 0, len(stats.BuildTypes))
	for buildType, s := range stats.BuildTypes {
		rows = append(rows, row{buildType, s})
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].stats.Last.Equal(rows[j].stats.Last) {
			return rows[i].stats.Last.After(rows[j].stats.Last)
		}
		return rows[i].buildType < rows[j].buildType
	})

	log.Printf("%s: %s usage (%d build type(s)):", prefix, localGocacheStatsFilename, len(rows))
	for _, r := range rows {
		log.Printf("%s:   %s: count=%d first=%s last=%s", prefix, r.buildType, r.stats.Count,
			r.stats.First.Format(time.RFC3339), r.stats.Last.Format(time.RFC3339))
	}
}

// isLocalGocacheProtectedFile reports whether name must never be evicted: cmd/go's own lock
// file, the usage-stats file, or one of its in-flight temp files from saveLocalGocacheStats.
func isLocalGocacheProtectedFile(name string) bool {
	return name == localGocacheLockFilename || name == localGocacheStatsFilename ||
		strings.HasPrefix(name, localGocacheStatsFilename+".tmp.")
}

// evictOldestUntilFits deletes the oldest (by mtime) entries first, skipping
// localGocacheStatsFilename/its temp files and cmd/go's own lock file, until the remaining total
// is strictly below 90% of maxBytes (e.g. a 10GB limit trims down to just under 9GB). Trimming to
// that 10% margin rather than to the limit itself avoids evicting again on almost every
// subsequent job once the cache dir settles near the limit. entries is expected to come from a
// single scanCacheDir call the caller already made for logging purposes, so this never re-walks
// the cache dir itself. maxBytes<=0 disables eviction entirely. Deleting arbitrary native GOCACHE
// object files is safe: it's a content-addressed cache, so a missing file is just a cache miss,
// never a corruption.
func evictOldestUntilFits(cacheDir string, entries []cacheFileEntry, maxBytes int64) {
	if maxBytes <= 0 {
		return
	}

	var total int64
	for _, e := range entries {
		total += e.size
	}

	if total <= maxBytes {
		return
	}

	before := total
	target := maxBytes - maxBytes/10 // strictly below this once we've evicted at least one file

	evictable := make([]cacheFileEntry, 0, len(entries))
	for _, e := range entries {
		if !isLocalGocacheProtectedFile(filepath.Base(e.path)) {
			evictable = append(evictable, e)
		}
	}
	sort.Slice(evictable, func(i, j int) bool {
		return evictable[i].modTime.Before(evictable[j].modTime)
	})

	var removedFiles int
	var removedBytes int64

	for _, e := range evictable {
		if total < target {
			break
		}

		if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
			log.Printf("github-actions-done: evict %s: %s", e.path, err.Error())
			continue
		}

		total -= e.size
		removedFiles++
		removedBytes += e.size
	}

	log.Printf(
		"github-actions-done: cache dir %s over limit (%s > %s), evicted %d oldest file(s) (%s): %s -> %s (target < %s)",
		cacheDir, humanBytesBinary(before), humanBytesBinary(maxBytes), removedFiles, humanBytesBinary(removedBytes),
		humanBytesBinary(before), humanBytesBinary(total), humanBytesBinary(target),
	)
}
