package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEntry(t *testing.T) {
	e, ok := parseEntry(`HASH[moduleIndex]: "file foo.go 2026-05-11 12:00:00 +0000 UTC 123\n"`)
	require.True(t, ok)
	require.Equal(t, "moduleIndex", e.Kind)
	require.Equal(t, "file foo.go 2026-05-11 12:00:00 +0000 UTC 123", e.Payload)
}

func TestParseTestCacheEntry(t *testing.T) {
	e, ok := parseEntry(`testcache: github.com/vearutop/gocacheprogd/internal/http: input list not found: cache entry not found`)
	require.True(t, ok)
	require.Equal(t, "testcache", e.Kind)
	require.Equal(t, "github.com/vearutop/gocacheprogd/internal/http", e.Package)
	require.Equal(t, "input list not found: cache entry not found", e.Payload)
}

func TestScrubGoBuildDir(t *testing.T) {
	in := `stat /var/folders/vm/x/T/go-build3163942882/b001/_testmain.go abc`
	out := scrubGoBuildDir(in)
	require.Contains(t, out, "/TMPROOT/")
	require.Contains(t, out, "go-build<id>")
}

func TestParseEntriesFocusInputs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "hash.log")
	logData := "" +
		`HASH[moduleIndex]: "file foo.go 2026-05-11 12:00:00 +0000 UTC 123\n"` + "\n" +
		`HASH[build fmt]: "compile"` + "\n" +
		`HASH[testInputs]: "env TMPDIR /var/folders/vm/x/T/\n"` + "\n" +
		`testcache: github.com/vearutop/gocacheprogd/internal/http: input list not found: cache entry not found` + "\n"
	require.NoError(t, os.WriteFile(logPath, []byte(logData), 0o600))

	entries, err := parseEntries(logPath, "all", "inputs")
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "moduleIndex", entries[0].Kind)
	require.Equal(t, "testInputs", entries[1].Kind)
	require.Equal(t, "testcache", entries[2].Kind)
}

func TestParseTestCacheReason(t *testing.T) {
	moduleRoot := "/repo"

	reason, ok := parseTestCacheReason(moduleRoot, entry{
		Kind:    "testcache",
		Package: "example.com/repo/internal/http",
		Payload: "input list not found: cache entry not found",
	})
	require.True(t, ok)
	require.Equal(t, "input list not found", reason)

	reason, ok = parseTestCacheReason(moduleRoot, entry{
		Kind:    "testcache",
		Package: "example.com/repo/internal/http",
		Payload: "input file /repo/internal/http/testdata/outputId0: file used as input is too new",
	})
	require.True(t, ok)
	require.Equal(t, "input file too new: internal/http/testdata/outputId0", reason)
}
