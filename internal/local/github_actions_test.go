package local

import (
	"os"
	"path/filepath"
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

	_, _, _, err := githubContext()
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

func TestAddDefaultGOMAXPROCS(t *testing.T) {
	t.Setenv("GOMAXPROCS", "")
	env := map[string]string{}
	addDefaultGOMAXPROCS(env)
	require.Equal(t, "100", env["GOMAXPROCS"])

	t.Setenv("GOMAXPROCS", "4")
	env = map[string]string{}
	addDefaultGOMAXPROCS(env)
	require.NotContains(t, env, "GOMAXPROCS")
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

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}
