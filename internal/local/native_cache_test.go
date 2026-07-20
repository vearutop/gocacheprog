package local

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
