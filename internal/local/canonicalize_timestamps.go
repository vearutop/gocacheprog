package local

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const canonicalBaseUnix = int64(1684178360)

type dirEntryPath struct {
	fullPath string
	relPath  string
}

func CanonicalizeTimestamps(repoRoot string) error {
	repoRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return fmt.Errorf("abs repo root: %w", err)
	}

	var dirs []dirEntryPath
	filesUpdated := 0

	err = filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == repoRoot {
			return nil
		}

		relPath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}

		if d.IsDir() {
			dirs = append(dirs, dirEntryPath{fullPath: path, relPath: relPath})
			return nil
		}

		modTime, err := canonicalFileModTime(path)
		if err != nil {
			return fmt.Errorf("hash %s: %w", relPath, err)
		}

		if err := os.Chtimes(path, modTime, modTime); err != nil {
			return fmt.Errorf("chtimes file %s: %w", relPath, err)
		}

		filesUpdated++
		return nil
	})
	if err != nil {
		return err
	}

	sort.Slice(dirs, func(i, j int) bool {
		if len(dirs[i].fullPath) == len(dirs[j].fullPath) {
			return dirs[i].fullPath < dirs[j].fullPath
		}
		return len(dirs[i].fullPath) > len(dirs[j].fullPath)
	})

	dirTime := time.Unix(canonicalBaseUnix, 0)
	for _, d := range dirs {
		if err := os.Chtimes(d.fullPath, dirTime, dirTime); err != nil {
			return fmt.Errorf("chtimes dir %s: %w", d.relPath, err)
		}
	}

	log.Printf("canonicalized timestamps under %s; files=%d dirs=%d", repoRoot, filesUpdated, len(dirs))
	return nil
}

func canonicalFileModTime(path string) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return time.Time{}, err
	}

	sum := h.Sum(nil)
	var prefix uint64
	for _, b := range sum[:5] {
		prefix = (prefix << 8) | uint64(b)
	}

	modUnix := canonicalBaseUnix - int64(prefix%10000)
	return time.Unix(modUnix, 0), nil
}
