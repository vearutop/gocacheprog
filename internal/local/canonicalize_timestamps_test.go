package local

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanonicalizeTimestamps(t *testing.T) {
	root := t.TempDir()

	visibleDir := filepath.Join(root, "visible")
	require.NoError(t, os.MkdirAll(visibleDir, 0o755))
	hiddenDir := filepath.Join(root, ".hidden")
	require.NoError(t, os.MkdirAll(hiddenDir, 0o755))

	visibleFile := filepath.Join(visibleDir, "file.txt")
	require.NoError(t, os.WriteFile(visibleFile, []byte("hello world"), 0o600))
	hiddenFile := filepath.Join(hiddenDir, "secret.txt")
	require.NoError(t, os.WriteFile(hiddenFile, []byte("secret"), 0o600))

	require.NoError(t, CanonicalizeTimestamps(root))

	visibleInfo, err := os.Stat(visibleFile)
	require.NoError(t, err)
	require.NotEqual(t, time.Unix(canonicalBaseUnix, 0), visibleInfo.ModTime())
	require.LessOrEqual(t, visibleInfo.ModTime().Unix(), canonicalBaseUnix)
	require.GreaterOrEqual(t, visibleInfo.ModTime().Unix(), canonicalBaseUnix-9999)

	visibleDirInfo, err := os.Stat(visibleDir)
	require.NoError(t, err)
	require.Equal(t, canonicalBaseUnix, visibleDirInfo.ModTime().Unix())

	hiddenAfter, err := os.Stat(hiddenFile)
	require.NoError(t, err)
	require.NotZero(t, hiddenAfter.ModTime().Unix())
	require.LessOrEqual(t, hiddenAfter.ModTime().Unix(), canonicalBaseUnix)
	require.GreaterOrEqual(t, hiddenAfter.ModTime().Unix(), canonicalBaseUnix-9999)

	hiddenDirInfo, err := os.Stat(hiddenDir)
	require.NoError(t, err)
	require.Equal(t, canonicalBaseUnix, hiddenDirInfo.ModTime().Unix())
}
