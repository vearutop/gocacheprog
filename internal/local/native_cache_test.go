package local

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprog/internal/gocache"
)

// TestLogSaveCacheSkips covers all four combinations logSaveCacheSkips can see: neither category
// skipped anything (silent, no log line at all), only the already-on-server skip, only the
// large-file skip, and both at once landing on the same line -- the actual point of this
// function, since either skip category alone looks identical to "there was nothing new to save"
// without it.
func TestLogSaveCacheSkips(t *testing.T) {
	capture := func(f func()) string {
		var buf bytes.Buffer
		origOutput := log.Writer()
		log.SetOutput(&buf)
		defer log.SetOutput(origOutput)

		f()
		return buf.String()
	}

	require.Empty(t, capture(func() { logSaveCacheSkips(0, 10, gocache.SkippedLargeFiles{}) }), "neither skip category should log anything")

	out := capture(func() { logSaveCacheSkips(3, 10, gocache.SkippedLargeFiles{}) })
	require.Contains(t, out, "skipping 3/10 objects the server already has")
	require.NotContains(t, out, "large objects")

	out = capture(func() { logSaveCacheSkips(0, 10, gocache.SkippedLargeFiles{Count: 2, Bytes: 5 * 1024 * 1024}) })
	require.NotContains(t, out, "already has")
	require.Contains(t, out, "2 large objects (5.0 MiB total) excluded")

	out = capture(func() { logSaveCacheSkips(3, 10, gocache.SkippedLargeFiles{Count: 2, Bytes: 5 * 1024 * 1024}) })
	require.Contains(t, out, "skipping 3/10 objects the server already has, 2 large objects (5.0 MiB total) excluded", "both categories must land on the same line")
}

func TestHumanBytesPerSecondBinary(t *testing.T) {
	require.Equal(t, "0 B/s", humanBytesPerSecondBinary(0, time.Second))
	require.Equal(t, "0 B/s", humanBytesPerSecondBinary(1024, 0))
	require.Equal(t, "1.0 KiB/s", humanBytesPerSecondBinary(2048, 2*time.Second))
	require.Equal(t, "1.5 KiB/s", humanBytesPerSecondBinary(1536, time.Second))
}

func TestDirStats_MissingDirIsZero(t *testing.T) {
	files, size, err := DirStats(filepath.Join(t.TempDir(), "nonexistent"))
	require.NoError(t, err)
	require.Equal(t, 0, files)
	require.Equal(t, int64(0), size)
}

func TestDirStats_CountsFilesAndSizeRecursively(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("12345"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "b"), []byte("1234567890"), 0o600))

	files, size, err := DirStats(dir)
	require.NoError(t, err)
	require.Equal(t, 2, files)
	require.Equal(t, int64(15), size)
}

func TestResolveAbsPath_ExpandsHomeDir(t *testing.T) {
	home, err := os.UserHomeDir()
	require.NoError(t, err)

	resolved, err := resolveAbsPath("~/foo/bar")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(home, "foo", "bar"), resolved)
}
