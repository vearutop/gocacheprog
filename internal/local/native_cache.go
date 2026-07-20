package local

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vearutop/gocacheprog/internal/gocache"
	cachehttp "github.com/vearutop/gocacheprog/internal/http"
)

// ResolveNativeCacheDir determines the native GOCACHE directory to use for
// -restore-cache/-save-cache and github-actions gocache mode: an explicit
// dir, else $GOCACHE, else the user cache dir.
func ResolveNativeCacheDir(dir string) (string, error) {
	if dir != "" {
		return resolveAbsPath(dir)
	}

	if envDir := strings.TrimSpace(os.Getenv("GOCACHE")); envDir != "" {
		return resolveAbsPath(envDir)
	}

	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("user cache dir: %w", err)
	}

	return filepath.Join(userCacheDir, "go-build"), nil
}

// DirStats reports the number of regular files and their combined size under dir. A missing
// dir is reported as zero files/bytes rather than an error, since callers use it right after
// creating the dir (or before it's ever been populated).
func DirStats(dir string) (files int, size int64, err error) {
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error { //nolint:gosec
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		files++
		size += info.Size()

		return nil
	})

	return files, size, err
}

func resolveAbsPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("user home dir: %w", err)
		}
		if path == "~" {
			path = homeDir
		} else {
			path = filepath.Join(homeDir, path[2:])
		}
	}

	return filepath.Abs(path)
}

// RestoreNativeCache restores native GOCACHE files from the remote server into cacheDir.
func RestoreNativeCache(cacheDir string, client *cachehttp.Client, req gocache.Request, startedAt time.Time) (gocache.TransferStats, error) {
	restoredPaths := make([]string, 0)
	stats, err := client.RestoreCache(req, func(item gocache.FileItem, body io.Reader) error {
		restoredPaths = append(restoredPaths, item.Path)
		return gocache.RestoreToDir(cacheDir, item, body)
	})
	if err != nil {
		return gocache.TransferStats{}, err
	}
	restorePrepareTime, restoreTotalTime := client.LastRestoreTimings()
	log.Printf(
		"restore-cache completed: files=%d download_time=%s compressed=%s compressed_rate=%s uncompressed=%s uncompressed_rate=%s server_prepare_time=%q server_total_time=%q; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q sources=%q",
		stats.Files,
		stats.Duration,
		humanBytesBinary(stats.CompressedBytes),
		humanBytesPerSecondBinary(stats.CompressedBytes, stats.Duration),
		humanBytesBinary(stats.UncompressedBytes),
		humanBytesPerSecondBinary(stats.UncompressedBytes, stats.Duration),
		restorePrepareTime,
		restoreTotalTime,
		req.Commit,
		req.ChangesID,
		req.BuildType,
		req.BaseCommit,
		req.ParentCommit,
		client.LastRestoreSources(),
	)

	if err := gocache.WriteRestoredPaths(cacheDir, restoredPaths); err != nil {
		return gocache.TransferStats{}, err
	}

	return stats, gocache.WriteJobStartMarker(cacheDir, startedAt)
}

// SaveNativeCache uploads freshly created native GOCACHE files from cacheDir to the remote server.
func SaveNativeCache(cacheDir string, client *cachehttp.Client, req gocache.Request, maxFileBytes int64) (gocache.TransferStats, error) {
	batch, err := gocache.CollectFreshFiles(cacheDir, maxFileBytes)
	if err != nil {
		return gocache.TransferStats{}, err
	}
	if len(batch.Items) == 0 {
		log.Printf(
			"save-cache completed: files=0 upload_time=0s compressed=0 B uncompressed=0 B; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q",
			req.Commit,
			req.ChangesID,
			req.BuildType,
			req.BaseCommit,
			req.ParentCommit,
		)
		return gocache.TransferStats{}, nil
	}

	stats, err := client.SaveCache(req, batch)
	if err != nil {
		return gocache.TransferStats{}, err
	}
	saveTotalTime := client.LastSaveTiming()
	log.Printf(
		"save-cache completed: files=%d upload_time=%s compressed=%s uncompressed=%s server_total_time=%q; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q",
		stats.Files,
		stats.Duration,
		humanBytesBinary(stats.CompressedBytes),
		humanBytesBinary(stats.UncompressedBytes),
		saveTotalTime,
		req.Commit,
		req.ChangesID,
		req.BuildType,
		req.BaseCommit,
		req.ParentCommit,
	)
	return stats, nil
}

func humanBytesBinary(v int64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%d B", v)
	}

	div, exp := int64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(v)/float64(div), "KMGTPE"[exp])
}

func humanBytesPerSecondBinary(bytes int64, d time.Duration) string {
	if bytes <= 0 || d <= 0 {
		return "0 B/s"
	}

	return humanBytesBinary(int64(float64(bytes)/d.Seconds())) + "/s"
}
