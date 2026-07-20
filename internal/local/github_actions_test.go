package local

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseGithubActionsDSN(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com?auth=secret&cache_dir=./build-cache&preload_size=42&build_type=unit&mode=gocache&canonicalize_timestamps=.&skip_preload=true")
	require.NoError(t, err)
	require.Equal(t, "https://gocache.example.com", cfg.remoteURL)
	require.Equal(t, "secret", cfg.authToken)
	require.Equal(t, "./build-cache", cfg.cacheDir)
	require.Equal(t, int64(42), cfg.maxFileBytes)
	require.Equal(t, "unit", cfg.buildType)
	require.Equal(t, "gocache", cfg.mode)
	require.Equal(t, ".", cfg.canonicalize)
	require.True(t, cfg.skipPreload)
}

func TestParseGithubActionsDSN_Defaults(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com")
	require.NoError(t, err)
	require.Equal(t, "https://gocache.example.com", cfg.remoteURL)
	require.Equal(t, "shim", cfg.mode)
	require.Equal(t, defaultGithubActionsPreloadSize, cfg.maxFileBytes)
	require.Equal(t, ".", cfg.canonicalize)
	require.False(t, cfg.skipPreload)
}

func TestParseGithubActionsDSN_CanonicalizeTimestampsEmptyValueDefaultsToRepoRoot(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com?canonicalize_timestamps=")
	require.NoError(t, err)
	require.Equal(t, ".", cfg.canonicalize)
}

func TestParseGithubActionsDSN_CanonicalizeTimestampsCustomPath(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com?canonicalize_timestamps=./sub")
	require.NoError(t, err)
	require.Equal(t, "./sub", cfg.canonicalize)
}

func TestParseGithubActionsDSN_SkipCanonicalizeTimestamps(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com?skip_canonicalize_timestamps=true")
	require.NoError(t, err)
	require.Empty(t, cfg.canonicalize)
}

func TestParseGithubActionsDSN_SkipCanonicalizeTimestampsFalseKeepsDefault(t *testing.T) {
	cfg, err := parseGithubActionsDSN("https://gocache.example.com?skip_canonicalize_timestamps=false")
	require.NoError(t, err)
	require.Equal(t, ".", cfg.canonicalize)
}

func TestParseGithubActionsDSN_InvalidSkipCanonicalizeTimestamps(t *testing.T) {
	_, err := parseGithubActionsDSN("https://gocache.example.com?skip_canonicalize_timestamps=not-a-bool")
	require.Error(t, err)
}

func TestParseGithubActionsDSN_InvalidPreloadSize(t *testing.T) {
	_, err := parseGithubActionsDSN("https://gocache.example.com?preload_size=not-a-number")
	require.Error(t, err)
}

func TestParseGithubActionsDSN_LocalGocacheMode(t *testing.T) {
	cfg, err := parseGithubActionsDSN("?mode=local-gocache&cache_dir=~/foo")
	require.NoError(t, err)
	require.Equal(t, "local-gocache", cfg.mode)
	require.Equal(t, "~/foo", cfg.cacheDir)
	require.Empty(t, cfg.remoteURL)
}

func TestInitLocalGocacheMode_SetsGocacheAndModeEnv(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	githubEnv := filepath.Join(t.TempDir(), "github_env")
	t.Setenv("GITHUB_ENV", githubEnv)

	cfg := githubActionsConfig{cacheDir: cacheDir}
	require.NoError(t, initLocalGocacheMode(cfg, time.Now()))

	_, err := os.Stat(cacheDir)
	require.NoError(t, err, "cache dir should be created")

	data, err := os.ReadFile(githubEnv)
	require.NoError(t, err)
	require.Contains(t, string(data), "GOCACHE="+cacheDir)
	require.Contains(t, string(data), envGHAMode+"=local-gocache")
	require.Contains(t, string(data), envGHACacheDir+"="+cacheDir)
}

func TestDoneLocalGocacheMode_NoCacheDirRecorded(t *testing.T) {
	t.Setenv(envGHACacheDir, "")
	require.NoError(t, doneLocalGocacheMode())
}

func TestDoneLocalGocacheMode_LogsCacheDirStats(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("hello"), 0o600))
	t.Setenv(envGHACacheDir, dir)

	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	require.NoError(t, doneLocalGocacheMode())
	require.Contains(t, buf.String(), "1 file(s)")
}

func TestRepoScopedBuildType(t *testing.T) {
	t.Setenv("GITHUB_REPOSITORY", "owner/repo")
	require.Equal(t, "owner-repo-unit", repoScopedBuildType("unit"))
	require.Equal(t, "owner-repo", repoScopedBuildType(""))
}

func TestRepoScopedBuildType_NoRepositoryEnv(t *testing.T) {
	t.Setenv("GITHUB_REPOSITORY", "")
	require.Equal(t, "unit", repoScopedBuildType("unit"))
	require.Empty(t, repoScopedBuildType(""))
}

func TestGithubContext_PullRequest(t *testing.T) {
	eventPath := filepath.Join(t.TempDir(), "event.json")
	writeFile(t, eventPath, `{"pull_request":{"number":42,"base":{"sha":"base-sha"},"head":{"sha":"head-sha"}}}`)

	t.Setenv("GITHUB_EVENT_NAME", "pull_request")
	t.Setenv("GITHUB_EVENT_PATH", eventPath)
	t.Setenv("GITHUB_REPOSITORY", "owner/repo")
	t.Setenv("GITHUB_SHA", "should-not-be-used")

	commit, baseCommit, changesID, err := githubContext()
	require.NoError(t, err)
	require.Equal(t, "head-sha", commit)
	require.Equal(t, "base-sha", baseCommit)
	require.Equal(t, "owner/repo#42", changesID)
}

func TestGithubContext_Push(t *testing.T) {
	t.Setenv("GITHUB_EVENT_NAME", "push")
	t.Setenv("GITHUB_SHA", "push-sha")

	commit, baseCommit, changesID, err := githubContext()
	require.NoError(t, err)
	require.Equal(t, "push-sha", commit)
	require.Empty(t, baseCommit)
	require.Empty(t, changesID)
}

func TestGithubContext_PullRequestMissingEventPath(t *testing.T) {
	t.Setenv("GITHUB_EVENT_NAME", "pull_request")
	t.Setenv("GITHUB_EVENT_PATH", "")

	_, _, _, err := githubContext() //nolint:dogsled // all three scope values are irrelevant when an error is expected.
	require.Error(t, err)
}

func TestCommonScopeArgs(t *testing.T) {
	cfg := githubActionsConfig{authToken: "tok", buildType: "unit"}
	args := commonScopeArgs(cfg, "commit-sha", "base-sha", "changes-id")
	require.Equal(t, []string{
		"-auth-token", "tok",
		"-commit", "commit-sha",
		"-changes-id", "changes-id",
		"-build-type", "unit",
		"-base-commit", "base-sha",
	}, args)
}

func TestCommonScopeArgs_OmitsEmptyFields(t *testing.T) {
	require.Empty(t, commonScopeArgs(githubActionsConfig{}, "", "", ""))
}

func TestShellJoin(t *testing.T) {
	require.Equal(t, "/bin/gocacheprog -a b -c d", shellJoin("/bin/gocacheprog", []string{"-a", "b", "-c", "d"}))
}

func TestTailFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.txt")
	writeFile(t, path, "0123456789")

	require.Equal(t, "6789", tailFile(path, 4))
	require.Equal(t, "0123456789", tailFile(path, 100))
}

func TestWaitForShimSocket_TimesOutWhenNothingListens(t *testing.T) {
	err := waitForShimSocket(filepath.Join(t.TempDir(), "nonexistent.sock"), 300*time.Millisecond)
	require.Error(t, err)
}

func TestAppendQuietRunStats_NoopWithoutDir(t *testing.T) {
	require.NoError(t, AppendQuietRunStats("", StatsSummary{Hits: 1}))
}

func TestAppendQuietRunStats_AppendsJSONLines(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 1, Misses: 2, Puts: 3}))
	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 4, Misses: 5, Puts: 6}))

	data, err := os.ReadFile(filepath.Join(dir, quietRunStatsFilename))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
}

func TestDoneDirectMode_NoCacheDirRecorded(t *testing.T) {
	t.Setenv(envGHACacheDir, "")
	require.NoError(t, doneDirectMode())
}

func TestDoneDirectMode_NoStatsFile(t *testing.T) {
	t.Setenv(envGHACacheDir, t.TempDir())
	require.NoError(t, doneDirectMode())
}

func TestDoneDirectMode_SingleInvocation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envGHACacheDir, dir)
	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 8, Misses: 2, Puts: 1, HitRate: "80.0%"}))

	require.NoError(t, doneDirectMode())

	_, err := os.Stat(filepath.Join(dir, quietRunStatsFilename))
	require.ErrorIs(t, err, os.ErrNotExist, "stats file should be removed after being reported")
}

func TestDoneDirectMode_MultipleInvocationsAggregatesCounts(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envGHACacheDir, dir)
	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 3, Misses: 1, Puts: 1}))
	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 5, Misses: 1, Puts: 2}))

	require.NoError(t, doneDirectMode())
}

func TestAppendQuietRunStats_RecordsParentPID(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, AppendQuietRunStats(dir, StatsSummary{Hits: 1}))

	data, err := os.ReadFile(filepath.Join(dir, quietRunStatsFilename))
	require.NoError(t, err)

	var record directRunRecord
	require.NoError(t, json.Unmarshal(data, &record))
	require.Equal(t, os.Getppid(), record.ParentPID)
}

func TestParentCommandLine_DoesNotPanic(t *testing.T) {
	// ParentCmd is best-effort (Linux /proc only); this just guards against a panic or hang,
	// not any particular content, since the value is environment-dependent.
	require.NotPanics(t, func() { parentCommandLine() })
}

func TestLogParentCommandBreakdown_GroupsAndSortsByCount(t *testing.T) {
	var buf bytes.Buffer
	origOutput := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origOutput)

	logParentCommandBreakdown([]directRunRecord{
		{ParentCmd: "go test -parallel 20 -mod=vendor ./..."},
		{ParentCmd: "go test -parallel 20 -mod=vendor ./..."},
		{ParentCmd: "go build ./..."},
		{ParentPID: 1234}, // no ParentCmd available
	})

	out := buf.String()
	require.Contains(t, out, "3 distinct parent command(s)")

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.True(t, strings.Contains(lines[1], "[2x]"), "most frequent command should be listed first: %q", lines[1])
	require.Contains(t, out, "[1x] go build ./...")
	require.Contains(t, out, "[1x] (unknown, parent_pid=1234)")
}

func TestSumStatsSummaries_AggregatesCountsRoundTripTimeAndBytes(t *testing.T) {
	total := sumStatsSummaries([]StatsSummary{
		{Hits: 3, Misses: 1, Puts: 1, GetTotalTime: "100ms", BytesRead: "500KB", BytesWritten: "1KB"},
		{Hits: 5, Misses: 1, Puts: 2, GetTotalTime: "208.513365ms", BytesRead: "1MB", BytesWritten: "2KB"},
		{Hits: 2, Misses: 0, Puts: 0}, // no upstream, no GetTotalTime/bytes
	})

	require.Equal(t, int64(10), total.Hits)
	require.Equal(t, int64(2), total.Misses)
	require.Equal(t, int64(3), total.Puts)
	require.Equal(t, "83.3%", total.HitRate)
	require.Equal(t, "308.513365ms", total.GetTotalTime)
	require.Equal(t, "1.5MB", total.BytesRead)
	require.Equal(t, "3KB", total.BytesWritten)
	require.Equal(t, "hits=10 misses=2 puts=3 hit_rate=83.3% bytes_read=1.5MB bytes_written=3KB round_trip_time=308.513365ms", total.String())
}

func TestSumStatsSummaries_SumsRoundTrips(t *testing.T) {
	total := sumStatsSummaries([]StatsSummary{
		{Hits: 1, RoundTrips: 3},
		{Hits: 1, RoundTrips: 5},
	})

	require.Equal(t, int64(8), total.RoundTrips)
	require.Contains(t, total.String(), "round_trips=8")
}

func TestElapsedSinceInit_MissingEnvVar(t *testing.T) {
	t.Setenv(envGHAInitTime, "")
	_, ok := elapsedSinceInit()
	require.False(t, ok)
}

func TestElapsedSinceInit_InvalidEnvVar(t *testing.T) {
	t.Setenv(envGHAInitTime, "not-a-timestamp")
	_, ok := elapsedSinceInit()
	require.False(t, ok)
}

func TestElapsedSinceInit_ComputesElapsedDuration(t *testing.T) {
	t.Setenv(envGHAInitTime, time.Now().Add(-5*time.Second).UTC().Format(time.RFC3339Nano))

	elapsed, ok := elapsedSinceInit()
	require.True(t, ok)
	require.GreaterOrEqual(t, elapsed, 5*time.Second)
	require.Less(t, elapsed, 15*time.Second)
}

func TestParseByteSize_RoundTripsWithFormatByteSize(t *testing.T) {
	for _, tc := range []struct {
		input string
		bytes int64
	}{
		{"0B", 0},
		{"512B", 512},
		{"1KB", 1024},
		{"1.5KB", 1536},
		{"1MB", 1 << 20},
		{"2.5GB", int64(2.5 * (1 << 30))},
	} {
		n, err := parseByteSize(tc.input)
		require.NoError(t, err, tc.input)
		require.Equal(t, tc.bytes, n, tc.input)
		require.Equal(t, tc.input, formatByteSize(tc.bytes), tc.input)
	}
}

func TestParseByteSize_InvalidInput(t *testing.T) {
	_, err := parseByteSize("not-a-size")
	require.Error(t, err)
}

func TestSumStatsSummaries_NoRoundTripTimeWhenNoneRecorded(t *testing.T) {
	total := sumStatsSummaries([]StatsSummary{
		{Hits: 1, Misses: 0, Puts: 0},
		{Hits: 2, Misses: 0, Puts: 0},
	})

	require.Empty(t, total.GetTotalTime)
	require.NotContains(t, total.String(), "round_trip_time")
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}
