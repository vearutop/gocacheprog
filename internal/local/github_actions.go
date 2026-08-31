// Package local implements gocacheprog's local-side building blocks: the on-disk cache
// store, the daemon/shim pair, and (this file) -github-actions-init/-github-actions-done,
// a condensed, single-DSN wrapper around the existing direct/shim/native-GOCACHE CLI modes
// aimed at GitHub Actions jobs that want sane defaults instead of hand-rolled bash plumbing.
//
// DSN format for -github-actions-init:
//
//	<remote-url>?auth=<token>&cache_dir=<dir>&max_file_bytes=<bytes>&build_type=<type>&mode=direct|shim|gocache|local-gocache&canonicalize_timestamps=<path>&skip_canonicalize_timestamps=<bool>&skip_preload=<bool>&max_cache_bytes=<bytes>&report_<name>=<path>
//
// Only the remote URL is required; every query parameter is optional:
//
//   - auth: bearer token for the remote server and (in shim mode) the local daemon socket
//   - cache_dir: local cache/GOCACHE directory; empty picks gocacheprog's own default; a
//     leading "~/" is resolved against the user's home directory
//   - max_file_bytes: maps to -max-file-bytes, the largest single object this job's
//     preload/restore or save will transfer. When absent, falls back to the server's own
//     max-file-bytes default for this build type (see Handler.MaxFileBytesSettings, retunable
//     without touching this DSN), and only if the server has none configured either, to a
//     hardcoded 3,000,000. A DSN value always wins over the server default when both are set.
//   - build_type: maps to -build-type, e.g. "unit" or "race"; always prefixed with
//     $GITHUB_REPOSITORY (e.g. "owner-repo-unit") so manifests and the /inspect and /clear
//     admin endpoints stay isolated per repository when multiple repos share one server
//   - mode: "direct" (no daemon, one gocacheprog per go invocation), "shim" (background
//     daemon + GOCACHEPROG pointed at its local socket, default), "gocache" (native
//     GOCACHE restore-cache/save-cache, no GOCACHEPROG involved), or "local-gocache" (native
//     GOCACHE pointed straight at cache_dir, no remote involved at all; for self-hosted
//     runners with a persistent home directory across jobs)
//   - canonicalize_timestamps: repo root to canonicalize before anything else; defaults to
//     "." (the checkout root) since fresh CI checkouts almost always need it for stable
//     cache keys; set skip_canonicalize_timestamps=true to opt out entirely
//   - skip_canonicalize_timestamps: when true, skips timestamp canonicalization entirely
//   - skip_preload: when true, skips the explicit preload pass entirely (direct/shim only)
//   - max_cache_bytes: local-gocache mode only; total cache_dir size limit in bytes, checked
//     and enforced on -github-actions-done by evicting the oldest files first; 0 (default)
//     disables eviction entirely
//   - fallback_remote: local-gocache mode only; when true, if gocacheprog.json (see
//     localGocacheStats) has no recorded usage for the current build_type — meaning this host's
//     persistent cache dir is cold for it, e.g. after a self-hosted runner rotation — restores
//     from the remote at DSN's remote-url into cache_dir on -github-actions-init, and, having
//     done so, uploads back on -github-actions-done only the files created since init (not the
//     rest of cache_dir, which may hold unrelated build types); false (default) never touches a
//     remote in local-gocache mode
//   - report_<name>=<path>: any number of these, each naming a local file to read on
//     -github-actions-done and attach to that session's sessions.jsonl line under the key
//     "<name>" -- if the file's content parses as JSON it's inlined as that value, otherwise
//     it's reported as a literal string (the file's own extension plays no part in that
//     choice). Read at done time, not init, since the file (e.g. a coverage summary, or
//     teststat's -metrics-json) is typically produced during the job itself. A missing/unreadable
//     file is logged and skipped, same as every other cache-is-optional failure path in this
//     file. Works in every mode except local-gocache's fully-local (no fallback_remote) case,
//     which never talks to a remote at all and so has no session to attach a report to.
//   - testcache_keys=<path>: gocache mode only. Names a local JSON file (produced during the
//     job, e.g. by teststat's -testcache-keys) of {package: {hits: [...], misses: [...]}}
//     GOCACHE object keys. On -github-actions-done, each distinct miss key is checked against
//     the remote via /inspect-keys and logged per-package: whether it's in the resolved
//     manifest(s), exists on the remote, and its size/age -- forensics for "why did this package
//     miss its test cache" (never saved vs. saved-but-not-restored vs. restored-but-stale). Hit
//     keys are read and kept alongside but not yet acted on (see investigateTestcacheKeys) --
//     reserved for a future priority-restore manifest built from known-good keys, so that feature
//     won't need a second DSN param/file on top of this one.
//
// Commit, changes-id, and base-commit are derived automatically from GitHub Actions'
// own environment instead of being passed in: pull_request(_target) events use
// event.pull_request.head/base.sha and "<repo>#<number>"; every other event uses
// $GITHUB_SHA alone.
//
// GOCACHEPROG helper instances started for direct/shim mode (the ones cmd/go invokes directly)
// always pass -quiet, so only a fatal error ever prints there instead of routine cache logging
// mixing into go build/test output. -github-actions-done reports a final StatsSummary
// (hits/misses/puts, bytes read/written, and round-trip time) once it finishes: shim mode reads
// it back from the daemon's stop response, gocache mode combines restore (persisted via
// GOCACHEPROG_GHA_RESTORE_STATS) and save stats, and direct mode aggregates whatever each -quiet
// invocation appended via AppendQuietRunStats to quietRunStatsFilename next to the cache dir.
// local-gocache mode talks to a remote only if fallback_remote is set and this build_type's local
// cache is cold; otherwise init just points GOCACHE at cache_dir. Either way, both init and done
// report the cache dir's file count/size plus its per-build-type usage stats (see
// localGocacheStats), and done additionally enforces max_cache_bytes by eviction if set.
//
// -github-actions-init generates one session ID for the whole job (see GithubActionsInit) and
// threads it into every process it spawns (via -session-id, see commonScopeArgs) so every
// request across the job -- preload, per-invocation helpers, the shim daemon -- reports as one
// session rather than each spawned process touching its own. -github-actions-done calls
// MarkSessionDone in every mode except local-gocache's fully-local case (see above), which flags
// that session done on the remote and attaches the extras above; server-side session time
// (first touch to done) and report_<name> extras then work identically regardless of mode.
//
// Direct mode's per-invocation records also carry best-effort parent process context (PID and,
// on Linux, the parent's command line read from /proc) purely for diagnosing an unexpectedly
// high invocation count: if a job reports far more invocations than the workflow YAML's own `go`
// commands would suggest, -github-actions-done breaks them down by parent command so the actual
// caller (a Makefile target, a test-splitting tool, a per-package loop, etc.) is visible directly
// in the job log without any extra tracing steps.
package local

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/vearutop/gocacheprog/internal/gocache"
	cachehttp "github.com/vearutop/gocacheprog/internal/http"
)

const (
	defaultGithubActionsPreloadSize int64 = 3_000_000
	// maxFileBytesUnset marks a githubActionsConfig fresh out of parseGithubActionsDSN as not
	// having an explicit max_file_bytes -- resolveMaxFileBytesDefault replaces it with
	// the server's own configured default, or defaultGithubActionsPreloadSize if the server has
	// none either.
	maxFileBytesUnset           int64 = -1
	githubActionsShimSocketWait       = 10 * time.Second
	githubActionsLogTailBytes   int64 = 8_000

	remoteClientMaxRetries = 3
	remoteClientRetryDelay = 5 * time.Second

	envGHAMode          = "GOCACHEPROG_GHA_MODE"
	envGHASocket        = "GOCACHEPROG_GHA_SOCKET"
	envGHAAuth          = "GOCACHEPROG_GHA_AUTH"
	envGHAPIDFile       = "GOCACHEPROG_GHA_PID_FILE"
	envGHALogFile       = "GOCACHEPROG_GHA_LOG_FILE"
	envGHACacheDir      = "GOCACHEPROG_GHA_CACHE_DIR"
	envGHARemoteURL     = "GOCACHEPROG_GHA_REMOTE_URL"
	envGHACommit        = "GOCACHEPROG_GHA_COMMIT"
	envGHAChangesID     = "GOCACHEPROG_GHA_CHANGES_ID"
	envGHABuildType     = "GOCACHEPROG_GHA_BUILD_TYPE"
	envGHABaseCommit    = "GOCACHEPROG_GHA_BASE_COMMIT"
	envGHAMaxFileBytes  = "GOCACHEPROG_GHA_MAX_FILE_BYTES"
	envGHARestoreStats  = "GOCACHEPROG_GHA_RESTORE_STATS"
	envGHAInitTime      = "GOCACHEPROG_GHA_INIT_TIME"
	envGHAMaxCacheBytes = "GOCACHEPROG_GHA_MAX_CACHE_BYTES"
	envGHALocalFallback = "GOCACHEPROG_GHA_LOCAL_FALLBACK"
	envGHASessionID     = "GOCACHEPROG_GHA_SESSION_ID"
	envGHAReportFiles   = "GOCACHEPROG_GHA_REPORT_FILES"
	envGHATestcacheKeys = "GOCACHEPROG_GHA_TESTCACHE_KEYS"
)

type githubActionsConfig struct {
	remoteURL      string
	authToken      string
	cacheDir       string
	buildType      string
	mode           string
	canonicalize   string
	maxFileBytes   int64
	maxCacheBytes  int64
	skipPreload    bool
	fallbackRemote bool
	// reportFiles maps a sessions.jsonl extra-field name to a local file path (see the "report_"
	// DSN query params), read back and reported at -github-actions-done time -- not at init,
	// since a report file (e.g. a coverage summary) is typically produced during the job itself
	// and wouldn't exist yet when -github-actions-init runs.
	reportFiles map[string]string
	// testcacheKeys is the "testcache_keys" DSN param: a local path (produced during the job,
	// e.g. by teststat's -testcache-keys) to a {package: {hits: [...], misses: [...]}} JSON file.
	// gocache mode only -- see doneGocacheMode's investigateTestcacheKeys.
	testcacheKeys string
}

// GithubActionsInit sets up caching for a GitHub Actions job from a single DSN. See the
// package doc comment above for the DSN format.
func GithubActionsInit(dsn string) error {
	initStartedAt := time.Now().UTC()

	cfg, err := parseGithubActionsDSN(dsn)
	if err != nil {
		return fmt.Errorf("github-actions-init: %w", err)
	}

	cfg.buildType = repoScopedBuildType(cfg.buildType)

	if cfg.maxFileBytes == maxFileBytesUnset {
		cfg.maxFileBytes = resolveMaxFileBytesDefault(cfg.remoteURL, cfg.authToken, cfg.buildType)
	}

	log.Printf("github-actions-init: mode=%q remote_url=%q cache_dir=%q build_type=%q max_file_bytes=%d skip_preload=%t max_cache_bytes=%d fallback_remote=%t",
		cfg.mode, cfg.remoteURL, cfg.cacheDir, cfg.buildType, cfg.maxFileBytes, cfg.skipPreload, cfg.maxCacheBytes, cfg.fallbackRemote)

	if cfg.canonicalize != "" {
		log.Printf("github-actions-init: canonicalizing timestamps under %q", cfg.canonicalize)
		if err := CanonicalizeTimestamps(cfg.canonicalize); err != nil {
			return fmt.Errorf("github-actions-init: canonicalize timestamps: %w", err)
		}
	}

	commit, baseCommit, changesID, err := githubContext()
	if err != nil {
		return fmt.Errorf("github-actions-init: %w", err)
	}
	log.Printf("github-actions-init: derived commit=%q changes_id=%q base_commit=%q", commit, changesID, baseCommit)

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("github-actions-init: resolve gocacheprog executable: %w", err)
	}

	// One session ID for the whole job, shared by every process this init spawns (and, in
	// gocache mode, this process itself) -- see MarkSessionDone and the package doc's "done"
	// paragraph. Without a shared ID, each spawned process would touch its own random session,
	// leaving -github-actions-done with nothing stable to mark done later.
	sessionID := fmt.Sprintf("%d-%d", os.Getpid(), initStartedAt.UnixNano())

	switch cfg.mode {
	case "direct":
		return initDirectMode(self, cfg, commit, baseCommit, changesID, sessionID, initStartedAt)
	case "shim":
		return initShimMode(self, cfg, commit, baseCommit, changesID, sessionID, initStartedAt)
	case "gocache":
		return initGocacheMode(cfg, commit, baseCommit, changesID, sessionID, initStartedAt)
	case "local-gocache":
		return initLocalGocacheMode(cfg, commit, baseCommit, changesID, sessionID, initStartedAt)
	default:
		return fmt.Errorf("github-actions-init: unsupported mode %q (expected direct, shim, gocache, or local-gocache)", cfg.mode)
	}
}

// GithubActionsDone finalizes caching started by -github-actions-init. It reads back the
// state -github-actions-init left in $GITHUB_ENV: it stops the daemon (shim mode), uploads
// freshly-built cache entries (gocache mode), or just prints a final cache summary (direct
// mode, which has no other background state to finalize).
func GithubActionsDone() error {
	mode := os.Getenv(envGHAMode)
	log.Printf("github-actions-done: mode=%q", mode)

	switch mode {
	case "direct":
		return doneDirectMode()
	case "shim":
		return doneShimMode()
	case "gocache":
		return doneGocacheMode()
	case "local-gocache":
		return doneLocalGocacheMode()
	case "":
		return fmt.Errorf("github-actions-done: %s is not set; did -github-actions-init run earlier in this job?", envGHAMode)
	default:
		return fmt.Errorf("github-actions-done: unknown mode %q in %s", mode, envGHAMode)
	}
}

func parseGithubActionsDSN(dsn string) (githubActionsConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return githubActionsConfig{}, fmt.Errorf("parse DSN: %w", err)
	}

	q := u.Query()

	cfg := githubActionsConfig{
		authToken: q.Get("auth"),
		cacheDir:  q.Get("cache_dir"),
		buildType: q.Get("build_type"),
		mode:      q.Get("mode"),
		// maxFileBytesUnset until proven otherwise: resolveMaxFileBytesDefault (called once
		// buildType is fully scoped) fills this in from the server's own default, and only then
		// the hardcoded fallback, so a bare -1 here must never reach a mode's own init function.
		maxFileBytes: maxFileBytesUnset,
		canonicalize: ".",
	}

	if cfg.mode == "" {
		cfg.mode = "shim"
	}

	if v := q.Get("max_file_bytes"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid max_file_bytes %q: %w", v, err)
		}
		cfg.maxFileBytes = n
	}

	if v := q.Get("max_cache_bytes"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid max_cache_bytes %q: %w", v, err)
		}
		cfg.maxCacheBytes = n
	}

	if v := q.Get("skip_preload"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid skip_preload %q: %w", v, err)
		}
		cfg.skipPreload = b
	}

	if v := q.Get("fallback_remote"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid fallback_remote %q: %w", v, err)
		}
		cfg.fallbackRemote = b
	}

	if q.Has("canonicalize_timestamps") {
		cfg.canonicalize = q.Get("canonicalize_timestamps")
		if cfg.canonicalize == "" {
			cfg.canonicalize = "."
		}
	}

	if v := q.Get("skip_canonicalize_timestamps"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid skip_canonicalize_timestamps %q: %w", v, err)
		}
		if b {
			cfg.canonicalize = ""
		}
	}

	cfg.reportFiles = parseReportFileParams(q)
	cfg.testcacheKeys = q.Get("testcache_keys")

	u.RawQuery = ""
	cfg.remoteURL = u.String()

	return cfg, nil
}

// parseReportFileParams extracts the DSN's report_<name>=<path> params (see the package doc
// comment) into a name->path map, or nil if there are none.
func parseReportFileParams(q url.Values) map[string]string {
	var reportFiles map[string]string
	for key, vals := range q {
		name, ok := strings.CutPrefix(key, "report_")
		if !ok || name == "" || len(vals) == 0 {
			continue
		}
		if reportFiles == nil {
			reportFiles = make(map[string]string)
		}
		reportFiles[name] = vals[0]
	}

	return reportFiles
}

var invalidBuildTypeChar = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// repoScopedBuildType prefixes buildType with $GITHUB_REPOSITORY so manifests and the
// /inspect and /clear admin endpoints stay isolated per repository when multiple repos
// share one gocacheprog server; an empty buildType scopes to the repository alone rather
// than falling back to the server-wide "default" manifest scope.
func repoScopedBuildType(buildType string) string {
	repo := invalidBuildTypeChar.ReplaceAllString(os.Getenv("GITHUB_REPOSITORY"), "-")
	if repo == "" {
		return buildType
	}
	if buildType == "" {
		return repo
	}

	return repo + "-" + buildType
}

// resolveMaxFileBytesDefault fills in max_file_bytes' default when the DSN didn't set one
// (see maxFileBytesUnset): the server's own configured default for buildType (see
// Handler.MaxFileBytesSettings), and only if the server has none either, the hardcoded
// defaultGithubActionsPreloadSize. An unreachable server just falls back to the hardcoded value
// too -- this is an optimization, not a build dependency, same reasoning as every other
// best-effort remote call in this file.
func resolveMaxFileBytesDefault(remoteURL, authToken, buildType string) int64 {
	client, err := cachehttp.NewClient(remoteURL, authToken)
	if err != nil {
		log.Printf("github-actions-init: WARNING: resolve max-file-bytes default: %s; using hardcoded default", err.Error())
		return defaultGithubActionsPreloadSize
	}

	bytes, err := client.MaxFileBytesFor(buildType)
	if err != nil {
		log.Printf("github-actions-init: WARNING: resolve max-file-bytes default: %s; using hardcoded default", err.Error())
		return defaultGithubActionsPreloadSize
	}

	if bytes <= 0 {
		return defaultGithubActionsPreloadSize
	}

	return bytes
}

type ghPullRequestEvent struct {
	Number int `json:"number"`
	Base   struct {
		SHA string `json:"sha"`
	} `json:"base"`
	Head struct {
		SHA string `json:"sha"`
	} `json:"head"`
}

type ghEvent struct {
	PullRequest *ghPullRequestEvent `json:"pull_request"`
}

// githubContext derives commit, base-commit, and changes-id from GitHub Actions' own
// environment: pull_request(_target) events use the event payload's head/base SHAs and
// PR number; every other event uses $GITHUB_SHA alone, matching the intended usage shown
// in test-unit-shim.yml.
func githubContext() (commit, baseCommit, changesID string, err error) {
	eventName := os.Getenv("GITHUB_EVENT_NAME")

	if eventName != "pull_request" && eventName != "pull_request_target" {
		return os.Getenv("GITHUB_SHA"), "", "", nil
	}

	eventPath := os.Getenv("GITHUB_EVENT_PATH")
	if eventPath == "" {
		return "", "", "", fmt.Errorf("GITHUB_EVENT_PATH is not set for a %s event", eventName)
	}

	data, err := os.ReadFile(eventPath) //nolint:gosec // GITHUB_EVENT_PATH is provided by the GitHub Actions runner.
	if err != nil {
		return "", "", "", fmt.Errorf("read GITHUB_EVENT_PATH: %w", err)
	}

	var event ghEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return "", "", "", fmt.Errorf("parse GITHUB_EVENT_PATH: %w", err)
	}

	if event.PullRequest == nil {
		return "", "", "", fmt.Errorf("%s event payload is missing the pull_request field", eventName)
	}

	repo := os.Getenv("GITHUB_REPOSITORY")
	changesID = fmt.Sprintf("%s#%d", repo, event.PullRequest.Number)

	return event.PullRequest.Head.SHA, event.PullRequest.Base.SHA, changesID, nil
}

func commonScopeArgs(cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string) []string {
	var args []string

	if cfg.authToken != "" {
		args = append(args, "-auth-token", cfg.authToken)
	}
	if commit != "" {
		args = append(args, "-commit", commit)
	}
	if changesID != "" {
		args = append(args, "-changes-id", changesID)
	}
	if cfg.buildType != "" {
		args = append(args, "-build-type", cfg.buildType)
	}
	if baseCommit != "" {
		args = append(args, "-base-commit", baseCommit)
	}
	if sessionID != "" {
		args = append(args, "-session-id", sessionID)
	}

	return args
}

// setReportFilesEnv marshals reportFiles (see the "report_" DSN query params) into env under
// envGHAReportFiles, for -github-actions-done's collectReportExtras to read back later. A no-op
// if reportFiles is empty. Marshal failure is logged and swallowed rather than failing init over
// it -- reportFiles is always map[string]string, which can't actually fail to marshal, but
// swallowing keeps this consistent with every other "reporting is optional" path in this file.
func setReportFilesEnv(env map[string]string, reportFiles map[string]string) {
	if len(reportFiles) == 0 {
		return
	}

	reportFilesJSON, err := json.Marshal(reportFiles)
	if err != nil {
		log.Printf("github-actions-init: marshal report files: %s", err.Error())
		return
	}

	env[envGHAReportFiles] = string(reportFilesJSON)
}

func resolveHelperCacheDir(dir string) (string, error) {
	if dir != "" {
		return resolveAbsPath(dir)
	}

	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("user cache dir: %w", err)
	}

	return filepath.Join(userCacheDir, "gocacheprog"), nil
}

// runPreloadOnly runs a synchronous -preload-only pass against cacheDir so that the
// daemon/direct invocation that follows can safely pass -skip-preload. Failures are
// logged and swallowed: a cold cache is slower, not incorrect.
func runPreloadOnly(self, cacheDir string, cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string) {
	if cfg.skipPreload {
		log.Printf("github-actions-init: skip_preload is set, not preloading %s", cacheDir)
		return
	}

	args := []string{
		"-cache-dir", cacheDir,
		"-remote-url", cfg.remoteURL,
		"-preload-only",
		"-max-file-bytes", strconv.FormatInt(cfg.maxFileBytes, 10),
	}
	args = append(args, commonScopeArgs(cfg, commit, baseCommit, changesID, sessionID)...)

	log.Printf("github-actions-init: preloading %s: %s", cacheDir, shellJoin(self, args))

	cmd := exec.Command(self, args...) //nolint:gosec // self is the resolved gocacheprog executable path.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	startedAt := time.Now()
	if err := cmd.Run(); err != nil {
		log.Printf("github-actions-init: preload failed after %s, continuing without it: %s", time.Since(startedAt), err.Error())
		return
	}

	log.Printf("github-actions-init: preload finished in %s", time.Since(startedAt))
}

func initDirectMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string, initStartedAt time.Time) error {
	cacheDir, err := resolveHelperCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	runPreloadOnly(self, cacheDir, cfg, commit, baseCommit, changesID, sessionID)

	args := []string{
		"-cache-dir", cacheDir,
		"-remote-url", cfg.remoteURL,
		"-skip-preload",
		"-quiet",
		"-max-file-bytes", strconv.FormatInt(cfg.maxFileBytes, 10),
	}
	args = append(args, commonScopeArgs(cfg, commit, baseCommit, changesID, sessionID)...)

	env := map[string]string{
		"GOCACHEPROG":   shellJoin(self, args),
		envGHAMode:      "direct",
		envGHACacheDir:  cacheDir,
		envGHARemoteURL: cfg.remoteURL,
		envGHAAuth:      cfg.authToken,
		envGHASessionID: sessionID,
		envGHAInitTime:  initStartedAt.Format(time.RFC3339Nano),
	}
	setReportFilesEnv(env, cfg.reportFiles)

	log.Printf("github-actions-init: direct mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

func initShimMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string, initStartedAt time.Time) error {
	cacheDir, err := resolveHelperCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	runPreloadOnly(self, cacheDir, cfg, commit, baseCommit, changesID, sessionID)

	socket := filepath.Join(os.TempDir(), "gocacheprog.sock")
	pidFile := filepath.Join(os.TempDir(), "gocacheprog.pid")
	logFile := filepath.Join(os.TempDir(), "gocacheprog-daemon.log")

	daemonArgs := []string{
		"-http", "unix://" + socket,
		"-cache-dir", cacheDir,
		"-remote-url", cfg.remoteURL,
		"-skip-preload",
		"-max-file-bytes", strconv.FormatInt(cfg.maxFileBytes, 10),
	}
	daemonArgs = append(daemonArgs, commonScopeArgs(cfg, commit, baseCommit, changesID, sessionID)...)

	logOut, err := os.Create(logFile) //nolint:gosec // logFile is a fixed path under os.TempDir().
	if err != nil {
		return fmt.Errorf("create daemon log file: %w", err)
	}
	defer func() {
		if closeErr := logOut.Close(); closeErr != nil {
			log.Printf("github-actions-init: close daemon log file: %s", closeErr.Error())
		}
	}()

	log.Printf("github-actions-init: starting daemon: %s", shellJoin(self, daemonArgs))

	cmd := exec.Command(self, daemonArgs...) //nolint:gosec // self is the resolved gocacheprog executable path.
	cmd.Stdout = logOut
	cmd.Stderr = logOut

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start gocacheprog daemon: %w", err)
	}

	log.Printf("github-actions-init: daemon started, pid=%d socket=%s log=%s", cmd.Process.Pid, socket, logFile)

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil { //nolint:gosec // pid file only needs to be readable.
		return fmt.Errorf("write daemon pid file: %w", err)
	}

	log.Printf("github-actions-init: waiting up to %s for daemon socket %s to accept connections", githubActionsShimSocketWait, socket)
	if err := waitForShimSocket(socket, githubActionsShimSocketWait); err != nil {
		return fmt.Errorf("gocacheprog daemon did not become ready: %w\n--- daemon log tail (%s) ---\n%s", err, logFile, tailFile(logFile, githubActionsLogTailBytes))
	}
	log.Printf("github-actions-init: daemon socket %s is ready", socket)

	clientArgs := []string{"-remote-url", "unix://" + socket, "-quiet"}
	if cfg.authToken != "" {
		clientArgs = append(clientArgs, "-auth-token", cfg.authToken)
	}

	env := map[string]string{
		"GOCACHEPROG":   shellJoin(self, clientArgs),
		envGHAMode:      "shim",
		envGHASocket:    socket,
		envGHAAuth:      cfg.authToken,
		envGHAPIDFile:   pidFile,
		envGHALogFile:   logFile,
		envGHARemoteURL: cfg.remoteURL,
		envGHASessionID: sessionID,
		envGHAInitTime:  initStartedAt.Format(time.RFC3339Nano),
	}
	setReportFilesEnv(env, cfg.reportFiles)

	log.Printf("github-actions-init: shim mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

// newRemoteClientWithRetry retries a failed connection to the remote cache server (it not being
// up yet, a brief network blip) up to remoteClientMaxRetries times before giving up, since a
// single transient failure here would otherwise cost the whole job its cache for no good reason.
func newRemoteClientWithRetry(remoteURL, authToken string, sessionInfo *cachehttp.SessionInfo) (*cachehttp.Client, error) {
	var lastErr error

	for attempt := 1; attempt <= remoteClientMaxRetries+1; attempt++ {
		client, err := cachehttp.NewClientWithSession(remoteURL, authToken, sessionInfo)
		if err == nil {
			return client, nil
		}

		lastErr = err
		if attempt > remoteClientMaxRetries {
			break
		}
		log.Printf("remote client connect attempt %d/%d failed, retrying in %s: %s", attempt, remoteClientMaxRetries+1, remoteClientRetryDelay, err.Error())
		time.Sleep(remoteClientRetryDelay)
	}

	return nil, lastErr
}

func initGocacheMode(cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string, initStartedAt time.Time) error {
	cacheDir, err := ResolveNativeCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure native cache dir: %w", err)
	}

	startedAt := time.Now().UTC()
	client, err := newRemoteClientWithRetry(cfg.remoteURL, cfg.authToken, &cachehttp.SessionInfo{
		SessionID: sessionID,
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		JobURL:    GithubActionsJobURL(),
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  cfg.buildType,
			BaseCommit: baseCommit,
		},
	})
	var restoreStats gocache.TransferStats
	if err != nil {
		// Same reasoning as the RestoreNativeCache failure below: a remote that can't even be
		// reached is just a cold GOCACHE, not a broken job.
		log.Printf("github-actions-init: WARNING: remote client: %s; continuing with a cold cache", err.Error())
	} else {
		req := gocache.Request{
			Commit:       commit,
			ChangesID:    changesID,
			BuildType:    cfg.buildType,
			BaseCommit:   baseCommit,
			MaxFileBytes: cfg.maxFileBytes,
		}

		log.Printf("github-actions-init: restoring native GOCACHE into %s from %s", cacheDir, cfg.remoteURL)
		restoreStats, err = RestoreNativeCache(cacheDir, client, req, startedAt)
		if err != nil {
			// The cache is an optimization, not a build dependency: a broken/overloaded/out-of-space
			// remote shouldn't fail the job, just cost it a cold GOCACHE (cache miss).
			log.Printf("github-actions-init: WARNING: restore native cache: %s; continuing with a cold cache", err.Error())
			restoreStats = gocache.TransferStats{}
		}
	}

	restoreStatsJSON, err := json.Marshal(restoreStats)
	if err != nil {
		return fmt.Errorf("marshal restore stats: %w", err)
	}

	env := map[string]string{
		"GOCACHE":          cacheDir,
		envGHAMode:         "gocache",
		envGHACacheDir:     cacheDir,
		envGHARemoteURL:    cfg.remoteURL,
		envGHAAuth:         cfg.authToken,
		envGHACommit:       commit,
		envGHAChangesID:    changesID,
		envGHABuildType:    cfg.buildType,
		envGHABaseCommit:   baseCommit,
		envGHAMaxFileBytes: strconv.FormatInt(cfg.maxFileBytes, 10),
		envGHARestoreStats: string(restoreStatsJSON),
		envGHAInitTime:     initStartedAt.Format(time.RFC3339Nano),
		envGHASessionID:    sessionID,
	}
	if cfg.testcacheKeys != "" {
		env[envGHATestcacheKeys] = cfg.testcacheKeys
	}
	setReportFilesEnv(env, cfg.reportFiles)

	log.Printf("github-actions-init: gocache mode ready, GOCACHE=%q", cacheDir)

	return setGitHubEnv(env)
}

// initLocalGocacheMode points GOCACHE straight at cache_dir with no remote server involved by
// default: no restore, no preload, nothing to authenticate. It's a fallback to classic local
// GOCACHE reuse for self-hosted runners with a persistent home directory across jobs, where the
// best possible cache hit rate comes from just letting `go` read/write its own on-disk cache in
// place rather than paying for a remote round trip.
//
// If cfg.fallbackRemote is set and gocacheprog.json has no recorded usage for cfg.buildType —
// meaning this host's persistent cache dir is cold for it, e.g. right after a self-hosted runner
// got rotated out and back in with an empty disk — it restores from the remote once here, and
// leaves a marker in $GITHUB_ENV so -github-actions-done knows to upload back only what this job
// actually produced (see doneLocalGocacheFallbackUpload).
func initLocalGocacheMode(cfg githubActionsConfig, commit, baseCommit, changesID, sessionID string, initStartedAt time.Time) error {
	cacheDir, err := ResolveNativeCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure native cache dir: %w", err)
	}

	logCacheDirStats("github-actions-init", cacheDir)

	stats, err := loadLocalGocacheStats(cacheDir)
	if err != nil {
		log.Printf("github-actions-init: load %s: %s", localGocacheStatsFilename, err.Error())
		stats = localGocacheStats{BuildTypes: map[string]localGocacheBuildTypeStats{}}
	} else {
		logLocalGocacheStats("github-actions-init", stats)
	}

	env := map[string]string{
		"GOCACHE":       cacheDir,
		envGHAMode:      "local-gocache",
		envGHACacheDir:  cacheDir,
		envGHABuildType: cfg.buildType,
		envGHAInitTime:  initStartedAt.Format(time.RFC3339Nano),
	}
	if cfg.maxCacheBytes > 0 {
		env[envGHAMaxCacheBytes] = strconv.FormatInt(cfg.maxCacheBytes, 10)
	}

	// Session/report tracking for local-gocache mode only piggybacks on fallback_remote's own
	// restore call (see initLocalGocacheFallbackRestore), never as an unconditional extra
	// network touch: this mode's whole point in the common (warm, no-fallback) case is
	// avoiding the remote entirely, and unconditionally constructing a session client here
	// would undermine that -- including retrying for several seconds against an address that
	// was never going to work when remoteURL is empty (a legitimate, fully-local setup) or
	// unreachable.
	if cfg.fallbackRemote {
		if err := initLocalGocacheFallbackRestore(cfg, commit, baseCommit, changesID, sessionID, cacheDir, stats, initStartedAt, env); err != nil {
			return err
		}
	}

	log.Printf("github-actions-init: local-gocache mode ready, GOCACHE=%q", cacheDir)

	return setGitHubEnv(env)
}

// initLocalGocacheFallbackRestore is fallback_remote's cold-start path, split out of
// initLocalGocacheMode to keep that function flat: if build_type already has recorded usage, it's
// a no-op (no network touched at all, preserving local-gocache mode's core promise); otherwise it
// restores from the remote into cacheDir and fills env with everything doneLocalGocacheFallbackUpload
// will need later to upload back what this job produces. The same client this restore uses is also
// what makes this session trackable/reportable at -github-actions-done time (see MarkSessionDone):
// its construction alone (a /version handshake carrying session headers) starts the session, no
// separate touch needed. sessionID is shared with the rest of the job (see GithubActionsInit) so
// the restore and the later done-time MarkSessionDone call land on the same session.
func initLocalGocacheFallbackRestore(cfg githubActionsConfig, commit, baseCommit, changesID, sessionID, cacheDir string, stats localGocacheStats, initStartedAt time.Time, env map[string]string) error {
	if _, warm := stats.BuildTypes[localGocacheStatsBuildTypeKey(cfg.buildType)]; warm {
		log.Printf("github-actions-init: fallback_remote is set but build_type=%q already has recorded usage, skipping remote restore", cfg.buildType)
		return nil
	}

	if cfg.remoteURL == "" {
		return errors.New("github-actions-init: fallback_remote requires a remote URL in the DSN")
	}

	log.Printf("github-actions-init: fallback_remote is set and build_type=%q has no recorded usage, restoring from %s into %s", cfg.buildType, cfg.remoteURL, cacheDir)

	client, err := newRemoteClientWithRetry(cfg.remoteURL, cfg.authToken, &cachehttp.SessionInfo{
		SessionID: sessionID,
		StartedAt: initStartedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		JobURL:    GithubActionsJobURL(),
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  cfg.buildType,
			BaseCommit: baseCommit,
		},
	})
	if err != nil {
		// Same reasoning as below: a remote that can't even be reached leaves cacheDir cold
		// (cache miss), it shouldn't fail the job.
		log.Printf("github-actions-init: WARNING: fallback_remote client: %s; continuing with a cold local cache", err.Error())
		return nil
	}

	req := gocache.Request{
		Commit:       commit,
		ChangesID:    changesID,
		BuildType:    cfg.buildType,
		BaseCommit:   baseCommit,
		MaxFileBytes: cfg.maxFileBytes,
	}

	if _, err := RestoreNativeCache(cacheDir, client, req, initStartedAt); err != nil {
		// Same reasoning as initGocacheMode: a failed warm-up leaves cacheDir cold (cache miss),
		// it shouldn't fail the job. Skip the fallback env vars below too, so -github-actions-done
		// doesn't try to upload back to a remote that just failed a restore.
		log.Printf("github-actions-init: WARNING: fallback_remote restore: %s; continuing with a cold local cache", err.Error())
		return nil
	}

	env[envGHALocalFallback] = "1"
	env[envGHARemoteURL] = cfg.remoteURL
	env[envGHAAuth] = cfg.authToken
	env[envGHASessionID] = sessionID
	env[envGHACommit] = commit
	env[envGHAChangesID] = changesID
	env[envGHABaseCommit] = baseCommit
	env[envGHAMaxFileBytes] = strconv.FormatInt(cfg.maxFileBytes, 10)
	setReportFilesEnv(env, cfg.reportFiles)

	return nil
}

// logCacheDirStats logs cacheDir's current file count/size, best-effort: a stat failure is
// logged and swallowed rather than failing the caller, since it's purely informational.
func logCacheDirStats(prefix, cacheDir string) {
	files, size, err := DirStats(cacheDir)
	if err != nil {
		log.Printf("%s: stat cache dir %s: %s", prefix, cacheDir, err.Error())
		return
	}

	log.Printf("%s: cache dir %s currently has %d file(s), %s", prefix, cacheDir, files, humanBytesBinary(size))
}

// elapsedSinceInit returns wall-clock time since -github-actions-init started, read back from
// envGHAInitTime. ok is false if that env var is missing or unparseable (e.g. -github-actions-done
// run without a preceding -github-actions-init in this job).
func elapsedSinceInit() (elapsed time.Duration, ok bool) {
	raw := os.Getenv(envGHAInitTime)
	if raw == "" {
		return 0, false
	}

	initStartedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		log.Printf("github-actions-done: parse %s: %s", envGHAInitTime, err.Error())
		return 0, false
	}

	return time.Since(initStartedAt), true
}

// GithubActionsJobURL returns a link back to the current GitHub Actions run, if running under
// one, so a session on the remote server's status page can link straight to its job log instead
// of an operator having to go hunting for which run a given commit/changes-id/build-type came
// from. GITHUB_SERVER_URL/GITHUB_REPOSITORY/GITHUB_RUN_ID are set by GitHub Actions itself on
// every job (https://docs.github.com/en/actions/learn-github-actions/variables); empty outside
// GitHub Actions, or if any of them is unset. Points at the run rather than a specific job within
// it: resolving a job's own URL needs its numeric job ID, which isn't exposed as an env var and
// would need an extra API call to look up.
func GithubActionsJobURL() string {
	serverURL := os.Getenv("GITHUB_SERVER_URL")
	repo := os.Getenv("GITHUB_REPOSITORY")
	runID := os.Getenv("GITHUB_RUN_ID")
	if serverURL == "" || repo == "" || runID == "" {
		return ""
	}

	return serverURL + "/" + repo + "/actions/runs/" + runID
}

func doneShimMode() error {
	socket := os.Getenv(envGHASocket)
	auth := os.Getenv(envGHAAuth)
	logFile := os.Getenv(envGHALogFile)
	pidFile := os.Getenv(envGHAPIDFile)
	remoteURL := os.Getenv(envGHARemoteURL)
	sessionID := os.Getenv(envGHASessionID)

	if socket == "" {
		return fmt.Errorf("github-actions-done: %s is not set; did -github-actions-init run in shim mode earlier in this job?", envGHASocket)
	}

	log.Printf("github-actions-done: stopping daemon on socket %s", socket)
	resp, stopErr := StopShimServer("unix://"+socket, auth)

	if stopErr != nil {
		log.Printf("github-actions-done: graceful stop failed: %s", stopErr.Error())
		if pidFile != "" {
			killByPIDFile(pidFile)
		}
		if logFile != "" {
			log.Printf("github-actions-done: daemon log tail (%s):\n%s", logFile, tailFile(logFile, githubActionsLogTailBytes))
		}
		// Best-effort even on a failed stop: whatever report_<name> files exist are still
		// worth attaching, and the session shouldn't look permanently abandoned on the status
		// page just because the graceful stop didn't work.
		markRemoteSessionDone(remoteURL, auth, sessionID, sessionExtras("shim", StatsSummary{}))
		return nil
	}

	log.Printf("github-actions-done: daemon stopped gracefully")

	stats := resp.Stats
	stats.ForcedCloses = CountShimForcedCloses("unix://" + socket)
	if elapsed, ok := elapsedSinceInit(); ok {
		stats.TotalTime = elapsed.String()
	}
	log.Printf("github-actions-done: cache summary: %s", stats.String())

	markRemoteSessionDone(remoteURL, auth, sessionID, sessionExtras("shim", stats))

	return nil
}

func doneGocacheMode() error {
	cacheDir := os.Getenv(envGHACacheDir)
	remoteURL := os.Getenv(envGHARemoteURL)
	auth := os.Getenv(envGHAAuth)
	commit := os.Getenv(envGHACommit)
	changesID := os.Getenv(envGHAChangesID)
	buildType := os.Getenv(envGHABuildType)
	baseCommit := os.Getenv(envGHABaseCommit)

	if cacheDir == "" || remoteURL == "" {
		return fmt.Errorf("github-actions-done: %s/%s are not set; did -github-actions-init run in gocache mode earlier in this job?", envGHACacheDir, envGHARemoteURL)
	}

	maxFileBytes, err := strconv.ParseInt(os.Getenv(envGHAMaxFileBytes), 10, 64)
	if err != nil {
		maxFileBytes = 0
	}

	startedAt := time.Now().UTC()
	sessionID := os.Getenv(envGHASessionID)
	if sessionID == "" {
		sessionID = fmt.Sprintf("%d-%d", os.Getpid(), startedAt.UnixNano())
	}
	client, err := newRemoteClientWithRetry(remoteURL, auth, &cachehttp.SessionInfo{
		SessionID: sessionID,
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		JobURL:    GithubActionsJobURL(),
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  buildType,
			BaseCommit: baseCommit,
		},
	})
	if err != nil {
		// Caching is a build optimization, not a build dependency: the job already finished by
		// this point, so a remote cache server being unreachable just means the next job pays
		// for a cache miss, not a broken job (same swallow-and-log as
		// doneLocalGocacheFallbackUpload below).
		log.Printf("github-actions-done: WARNING: remote client: %s; skipping cache upload", err.Error())
		return nil
	}

	req := gocache.Request{
		Commit:       commit,
		ChangesID:    changesID,
		BuildType:    buildType,
		BaseCommit:   baseCommit,
		MaxFileBytes: maxFileBytes,
	}

	// Inspect testcache misses before saving: SaveFreshNativeCache below is about to upload this
	// run's own freshly-computed results, and a miss's key is very often exactly what this run
	// just produced -- inspecting after that upload would show exists_remote=true for a key that
	// was genuinely absent at restore time, the opposite of what this forensics is for.
	investigateTestcacheKeys(client, req)

	var since time.Time
	if raw := os.Getenv(envGHAInitTime); raw != "" {
		if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			since = t
		}
	}

	log.Printf("github-actions-done: saving native GOCACHE from %s to %s", cacheDir, remoteURL)
	saveStats, skipStats, err := SaveFreshNativeCache(cacheDir, client, req, maxFileBytes, since, nil)
	if err != nil {
		// The build already finished by this point; a failed upload just means the next job
		// pays for a cache miss, not a broken job (same swallow-and-log as
		// doneLocalGocacheFallbackUpload below).
		log.Printf("github-actions-done: WARNING: save native cache: %s", err.Error())
		saveStats = gocache.TransferStats{}
	}

	var restoreStats gocache.TransferStats
	if raw := os.Getenv(envGHARestoreStats); raw != "" {
		if err := json.Unmarshal([]byte(raw), &restoreStats); err != nil {
			log.Printf("github-actions-done: parse restore stats: %s", err.Error())
		}
	}

	summary := fmt.Sprintf(
		"restore(files=%d compressed=%s uncompressed=%s time=%s) save(files=%d compressed=%s uncompressed=%s time=%s)",
		restoreStats.Files,
		humanBytesBinary(restoreStats.CompressedBytes),
		humanBytesBinary(restoreStats.UncompressedBytes),
		restoreStats.Duration,
		saveStats.Files,
		humanBytesBinary(saveStats.CompressedBytes),
		humanBytesBinary(saveStats.UncompressedBytes),
		saveStats.Duration,
	)
	if elapsed, ok := elapsedSinceInit(); ok {
		summary += " total_time=" + elapsed.String()
	}
	log.Printf("github-actions-done: cache summary: %s", summary)

	extra := map[string]any{
		"mode": "gocache",
		// Same numbers as logSaveCacheSkips' "skipping N/M objects the server already has" log
		// line, so a sessions.jsonl consumer doesn't have to scrape logs to get them.
		"save_skipped_existing":    skipStats.ExistingSkipped,
		"save_considered":          skipStats.Considered,
		"save_skipped_large_count": skipStats.LargeSkipped.Count,
		"save_skipped_large_bytes": skipStats.LargeSkipped.Bytes,
	}
	maps.Copy(extra, collectReportExtras())

	if err := client.MarkSessionDone(extra); err != nil {
		log.Printf("github-actions-done: WARNING: mark session done: %s", err.Error())
	}

	return nil
}

// testcachePkgKeys mirrors teststat's -testcache-keys output for one package: Hits are keys
// go's own cache actually matched, Misses are keys it went looking for and didn't find. Only
// Misses are inspected today (see investigateTestcacheKeys below); Hits are decoded and counted
// so a future priority-restore manifest built from known-good keys can reuse this same
// file/DSN param rather than needing a second one.
type testcachePkgKeys struct {
	Hits   []string `json:"hits,omitempty"`
	Misses []string `json:"misses,omitempty"`
}

// investigateTestcacheKeys is the testcache_keys DSN param's done-time half (see the package
// doc comment): reads back the {package: {hits, misses}} file named at init time, checks every
// distinct miss key against the remote in one batched /inspect-keys call, and logs one line per
// (package, key) with what was found -- e.g. in_manifest=true exists_remote=false (never saved)
// vs. exists_remote=true (saved, but Go's own cache still missed it locally) -- forensics for a
// test-result cache miss, not a build dependency, so any failure here is logged and swallowed
// rather than propagated. Must run before this run's own SaveFreshNativeCache upload (see the
// caller): a miss's key is very often exactly what this run just computed and is about to save,
// so checking after that upload would report exists_remote=true for a key that was genuinely
// absent at restore time -- the one case this forensics exists to catch.
func investigateTestcacheKeys(client *cachehttp.Client, req gocache.Request) {
	path := os.Getenv(envGHATestcacheKeys)
	if path == "" {
		return
	}

	// TEMPORARY: see the analytics/networks/dupchecker investigation. A confirmed-restored,
	// confirmed-correctly-sized, confirmed-in-manifest entry (dupchecker) still misses locally --
	// ruling out size limits, restore selection, and manifest scoping. The remaining candidate is
	// go's own testexpire.txt mechanism (go clean -testcache writes an expiry time there; any
	// cached entry older than it is ignored regardless of validity). Remove once confirmed/ruled
	// out.
	debugLogTestExpire(os.Getenv(envGHACacheDir))

	data, err := os.ReadFile(path) //nolint:gosec // path comes from our own DSN config, not user input.
	if err != nil {
		log.Printf("github-actions-done: read testcache keys file %s: %s", path, err.Error())
		return
	}

	var pkgKeys map[string]testcachePkgKeys
	if err := json.Unmarshal(data, &pkgKeys); err != nil {
		log.Printf("github-actions-done: parse testcache keys file %s: %s", path, err.Error())
		return
	}

	hitCount := 0
	seen := map[string]bool{}
	var missKeys []string
	for _, pk := range pkgKeys {
		hitCount += len(pk.Hits)
		for _, k := range pk.Misses {
			if !seen[k] {
				seen[k] = true
				missKeys = append(missKeys, k)
			}
		}
	}
	if hitCount > 0 {
		// Not acted on yet -- reserved for a future priority-restore manifest (see
		// testcachePkgKeys' doc comment) -- just surfaced so it's visible the data exists.
		log.Printf("github-actions-done: testcache-keys: %d hit keys captured, not yet used", hitCount)
	}
	if len(missKeys) == 0 {
		return
	}

	// TEMPORARY: see debugWatchRestorePaths' doc comment. Must run before SaveFreshNativeCache
	// (which it already does -- see doneGocacheMode) so CollectFilesToSave's walk picks these up.
	// Logged unconditionally, not just when CollectFilesToSave's walk finds one on disk: a watched
	// key never appearing in a later "DEBUG watch:" line is itself informative -- it means this
	// run's cacheDir never had that exact relPath at all, a different question than "had it but
	// excluded it."
	gocache.SetDebugWatchRestorePaths(missKeys)
	log.Printf("DEBUG watch: tracking %d miss key(s) this run: %s", len(missKeys), strings.Join(missKeys, ", "))

	results, err := client.InspectKeys(req, missKeys)
	if err != nil {
		log.Printf("github-actions-done: inspect testcache misses: %s", err.Error())
		return
	}

	byKey := make(map[string]gocache.KeyInspection, len(results))
	for _, r := range results {
		byKey[r.Key] = r
	}

	pkgs := make([]string, 0, len(pkgKeys))
	for pkg := range pkgKeys {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)

	for _, pkg := range pkgs {
		for _, k := range pkgKeys[pkg].Misses {
			ki, ok := byKey[k]
			if !ok {
				continue
			}
			age := time.Duration(ki.AgeSeconds * float64(time.Second)).Round(time.Second)
			log.Printf("github-actions-done: testcache-misses %s: %s in_manifest=%t exists_remote=%t size=%d age=%s",
				pkg, k, ki.InManifest, ki.ExistsRemote, ki.Size, age)
		}
	}
}

// debugLogTestExpire is TEMPORARY, see investigateTestcacheKeys' call site. go writes this file's
// content (a raw UnixNano timestamp) via "go clean -testcache"; any cached entry whose own
// encoded time predates it is treated as expired regardless of otherwise being valid. cacheDir is
// empty or the file absent in the overwhelmingly common case, logged plainly either way so a
// "why did dupchecker still miss with everything else confirmed fine" report doesn't need to
// guess. Remove alongside the rest of this investigation's instrumentation.
func debugLogTestExpire(cacheDir string) {
	if cacheDir == "" {
		return
	}

	data, err := os.ReadFile(filepath.Join(cacheDir, "testexpire.txt")) //nolint:gosec // cacheDir comes from our own DSN config, not user input.
	if err != nil {
		log.Printf("DEBUG watch: testexpire.txt: %s", err.Error())
		return
	}

	raw := strings.TrimSpace(string(data))

	nanos, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		log.Printf("DEBUG watch: testexpire.txt: unparseable content %q: %s", raw, perr.Error())
		return
	}

	log.Printf("DEBUG watch: testexpire.txt: expire_before=%s", time.Unix(0, nanos).UTC().Format(time.RFC3339Nano))
}

// collectReportExtras reads back the report_<name>=<path> files named at -github-actions-init
// time (see envGHAReportFiles), keyed by the same names they'll appear under in this session's
// sessions.jsonl line. A file that's missing or unreadable is logged and skipped -- same
// "caching/reporting is optional, not a build dependency" reasoning as everywhere else in this
// file -- rather than failing -github-actions-done over it.
func collectReportExtras() map[string]any {
	raw := os.Getenv(envGHAReportFiles)
	if raw == "" {
		return nil
	}

	var files map[string]string
	if err := json.Unmarshal([]byte(raw), &files); err != nil {
		log.Printf("github-actions-done: parse report files: %s", err.Error())
		return nil
	}

	extras := make(map[string]any, len(files))
	for name, path := range files {
		data, err := os.ReadFile(path) //nolint:gosec // path comes from the -github-actions-init DSN, an operator-controlled input.
		if err != nil {
			log.Printf("github-actions-done: read report file %q for %q: %s", path, name, err.Error())
			continue
		}
		extras[name] = reportFileValue(data)
	}

	return extras
}

// reportFileValue is a report file's sessions.jsonl representation: its content inlined as a
// JSON value when it parses as JSON, or reported as a literal string otherwise -- the file's own
// extension plays no part in that choice, only whether the content itself is valid JSON.
func reportFileValue(data []byte) any {
	if json.Valid(data) {
		var v any
		if err := json.Unmarshal(data, &v); err == nil {
			return v
		}
	}

	return string(data)
}

// sessionExtras assembles the "extra" map passed to MarkSessionDone: statsSummary (its own
// json-tagged fields, flattened -- marshal of a fixed struct never fails) merged with
// collectReportExtras' report_<name> file contents, plus "mode" for telling sessions.jsonl lines
// apart when multiple modes' sessions are analyzed together.
func sessionExtras(mode string, statsSummary StatsSummary) map[string]any {
	extra := map[string]any{"mode": mode}

	if data, err := json.Marshal(statsSummary); err != nil {
		log.Printf("github-actions-done: marshal stats summary: %s", err.Error())
	} else {
		var flat map[string]any
		if err := json.Unmarshal(data, &flat); err == nil {
			maps.Copy(extra, flat)
		}
	}

	maps.Copy(extra, collectReportExtras())

	return extra
}

// markRemoteSessionDone opens a throwaway session-tagged client purely to call MarkSessionDone --
// used by done*Mode functions that don't otherwise need a live remote connection at done time
// (direct, shim, local-gocache's fallback path). Best-effort: a client that can't be built or a
// call that fails is logged and swallowed, same "reporting is optional" reasoning as everywhere
// else in this file -- the job already finished by this point either way.
func markRemoteSessionDone(remoteURL, auth, sessionID string, extra map[string]any) {
	if remoteURL == "" || sessionID == "" {
		return
	}

	client, err := cachehttp.NewClientWithSession(remoteURL, auth, &cachehttp.SessionInfo{SessionID: sessionID})
	if err != nil {
		log.Printf("github-actions-done: WARNING: session client: %s; skipping session-done report", err.Error())
		return
	}

	if err := client.MarkSessionDone(extra); err != nil {
		log.Printf("github-actions-done: WARNING: mark session done: %s", err.Error())
	}
}

// doneLocalGocacheMode has no remote state to finalize in the common case: local-gocache mode's
// whole point is letting the persistent cache dir accumulate across jobs on the runner's own
// disk, so by default it just reports the cache dir's final file count/size so the effect of a
// job is visible in the log. The one exception is fallback_remote: if -github-actions-init had to
// restore from a remote because this build_type's local cache was cold (see envGHALocalFallback),
// this uploads back only what this job produced since init (doneLocalGocacheFallbackUpload).
func doneLocalGocacheMode() error {
	cacheDir := os.Getenv(envGHACacheDir)
	if cacheDir == "" {
		log.Printf("github-actions-done: local-gocache mode recorded no cache dir, nothing to summarize")
		return nil
	}

	buildType := os.Getenv(envGHABuildType)
	if stats, err := recordLocalGocacheUsage(cacheDir, buildType, time.Now().UTC()); err != nil {
		log.Printf("github-actions-done: update %s: %s", localGocacheStatsFilename, err.Error())
	} else {
		logLocalGocacheStats("github-actions-done", stats)
	}

	fallback := os.Getenv(envGHALocalFallback) == "1"
	if fallback {
		doneLocalGocacheFallbackUpload(cacheDir, buildType)
	}

	// Scanned once here and reused for the size log below, eviction, and (if fallback_remote
	// talked to a remote this run) the session-done report, rather than scanning cache_dir
	// multiple times.
	var localCacheFiles int
	var localCacheBytes int64
	entries, err := scanCacheDir(cacheDir)
	if err != nil {
		log.Printf("github-actions-done: stat cache dir %s: %s", cacheDir, err.Error())
	} else {
		localCacheFiles = len(entries)
		for _, e := range entries {
			localCacheBytes += e.size
		}
		log.Printf("github-actions-done: cache dir %s currently has %d file(s), %s", cacheDir, localCacheFiles, humanBytesBinary(localCacheBytes))

		if maxCacheBytes, err := strconv.ParseInt(os.Getenv(envGHAMaxCacheBytes), 10, 64); err == nil && maxCacheBytes > 0 {
			evictOldestUntilFits(cacheDir, entries, maxCacheBytes)
		}
	}

	if elapsed, ok := elapsedSinceInit(); ok {
		log.Printf("github-actions-done: total_time=%s", elapsed)
	}

	// Session/report tracking only exists for this run if fallback_remote's own restore
	// already talked to the remote (see initLocalGocacheFallbackRestore) -- the common,
	// fully-local case never touches the network here either, consistent with init.
	if fallback {
		extra := sessionExtras("local-gocache", StatsSummary{})
		extra["local_cache_files"] = localCacheFiles
		extra["local_cache_bytes"] = localCacheBytes
		markRemoteSessionDone(os.Getenv(envGHARemoteURL), os.Getenv(envGHAAuth), os.Getenv(envGHASessionID), extra)
	}

	return nil
}

// doneLocalGocacheFallbackUpload uploads the files fallback_remote's -github-actions-init created
// since it restored into cacheDir (i.e. everything with an mtime at or after envGHAInitTime, minus
// whatever that restore itself wrote and local-gocache mode's own lock/stats bookkeeping files) back
// to the same remote. It deliberately does not upload the rest of cacheDir: that's a persistent,
// potentially large cache dir this build_type merely shares with unrelated build types/repos on the
// same self-hosted runner, and re-uploading all of it on every cold-start would be both wasteful and
// wrong (it isn't this job's cache to claim credit for). Errors are logged and swallowed: a failed
// upload just means the next cold start pays for another remote restore, not a broken job.
func doneLocalGocacheFallbackUpload(cacheDir, buildType string) {
	remoteURL := os.Getenv(envGHARemoteURL)
	if remoteURL == "" {
		log.Printf("github-actions-done: %s is set but %s is not; skipping fallback_remote upload", envGHALocalFallback, envGHARemoteURL)
		return
	}

	since, err := time.Parse(time.RFC3339Nano, os.Getenv(envGHAInitTime))
	if err != nil {
		log.Printf("github-actions-done: parse %s for fallback_remote upload: %s", envGHAInitTime, err.Error())
		return
	}

	auth := os.Getenv(envGHAAuth)
	commit := os.Getenv(envGHACommit)
	changesID := os.Getenv(envGHAChangesID)
	baseCommit := os.Getenv(envGHABaseCommit)

	maxFileBytes, err := strconv.ParseInt(os.Getenv(envGHAMaxFileBytes), 10, 64)
	if err != nil {
		maxFileBytes = 0
	}

	startedAt := time.Now().UTC()
	client, err := newRemoteClientWithRetry(remoteURL, auth, &cachehttp.SessionInfo{
		SessionID: fmt.Sprintf("%d-%d", os.Getpid(), startedAt.UnixNano()),
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		JobURL:    GithubActionsJobURL(),
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  buildType,
			BaseCommit: baseCommit,
		},
	})
	if err != nil {
		log.Printf("github-actions-done: fallback_remote client: %s", err.Error())
		return
	}

	req := gocache.Request{
		Commit:       commit,
		ChangesID:    changesID,
		BuildType:    buildType,
		BaseCommit:   baseCommit,
		MaxFileBytes: maxFileBytes,
	}

	log.Printf("github-actions-done: fallback_remote was used at init, uploading %s files created since %s to %s", cacheDir, since.Format(time.RFC3339), remoteURL)

	if _, _, err := SaveFreshNativeCache(cacheDir, client, req, maxFileBytes, since, isLocalGocacheProtectedFile); err != nil {
		log.Printf("github-actions-done: fallback_remote upload: %s", err.Error())
	}
}

// quietRunStatsFilename is where each -quiet direct-mode invocation appends its final
// directRunRecord (one JSON line per invocation) so doneDirectMode can report a job-level
// summary, and trace an unexpectedly high invocation count back to whatever called it.
const quietRunStatsFilename = ".gocacheprog-run-stats.jsonl"

// directRunRecord is one line of the run stats file: the cache StatsSummary plus best-effort
// parent process context (Linux only, via /proc; empty elsewhere), so a job with far more
// invocations than expected can be traced back to whatever actually spawned each one.
type directRunRecord struct {
	StatsSummary
	ParentPID int    `json:"parent_pid,omitempty"`
	ParentCmd string `json:"parent_cmd,omitempty"`
}

// AppendQuietRunStats appends one run record to the per-cache-dir run stats file used by
// direct mode's -github-actions-done to report a final cache summary. A no-op if dir is empty.
func AppendQuietRunStats(dir string, summary StatsSummary) error {
	if dir == "" {
		return nil
	}

	record := directRunRecord{
		StatsSummary: summary,
		ParentPID:    os.Getppid(),
		ParentCmd:    parentCommandLine(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal run stats: %w", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, quietRunStatsFilename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // dir is the configured cache dir.
	if err != nil {
		return fmt.Errorf("open run stats file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("close run stats file: %s", closeErr.Error())
		}
	}()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write run stats: %w", err)
	}

	return nil
}

// parentCommandLine best-effort reads this process's parent's command line from /proc (Linux
// only); it returns "" on any error, including on platforms without /proc.
func parentCommandLine() string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", os.Getppid()))
	if err != nil {
		return ""
	}

	args := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")

	return strings.Join(args, " ")
}

// sumStatsSummaries aggregates hits/misses/puts, round-trip time, and bytes read/written
// across multiple direct-mode invocations. Bytes are recovered from their already-rounded,
// human-readable form (e.g. "1.2MB", one decimal digit of precision) via parseByteSize, so the
// summed total carries the same small rounding error as each individual reading.
func sumStatsSummaries(summaries []StatsSummary) StatsSummary {
	var total StatsSummary

	var roundTripTime time.Duration
	var bytesRead, bytesWritten int64
	var haveBytes bool

	for _, s := range summaries {
		total.Hits += s.Hits
		total.Misses += s.Misses
		total.Puts += s.Puts
		total.RoundTrips += s.RoundTrips

		if s.GetTotalTime != "" {
			if d, err := time.ParseDuration(s.GetTotalTime); err == nil {
				roundTripTime += d
			}
		}

		if s.BytesRead != "" {
			if n, err := parseByteSize(s.BytesRead); err == nil {
				bytesRead += n
				haveBytes = true
			}
		}
		if s.BytesWritten != "" {
			if n, err := parseByteSize(s.BytesWritten); err == nil {
				bytesWritten += n
				haveBytes = true
			}
		}
	}

	total.HitRate = percent(total.Hits, total.Hits+total.Misses)
	if roundTripTime > 0 {
		total.GetTotalTime = roundTripTime.String()
	}
	if haveBytes {
		total.BytesRead = formatByteSize(bytesRead)
		total.BytesWritten = formatByteSize(bytesWritten)
	}

	return total
}

// byteSizeUnits mirrors internal/http.byteSize's thresholds/suffixes (largest first) so
// parseByteSize/formatByteSize can invert and reproduce that same "1.2MB"-style formatting.
var byteSizeUnits = []struct {
	suffix string
	factor int64
}{
	{"EB", 1 << 60},
	{"PB", 1 << 50},
	{"TB", 1 << 40},
	{"GB", 1 << 30},
	{"MB", 1 << 20},
	{"KB", 1 << 10},
}

// parseByteSize inverts internal/http.byteSize's "1.2MB"-style formatting back to a byte
// count, accurate to that format's one decimal digit of precision.
func parseByteSize(s string) (int64, error) {
	for _, u := range byteSizeUnits {
		if numStr, ok := strings.CutSuffix(s, u.suffix); ok {
			value, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
			}

			return int64(value * float64(u.factor)), nil
		}
	}

	numStr, ok := strings.CutSuffix(s, "B")
	if !ok {
		return 0, fmt.Errorf("invalid byte size %q: unrecognized unit", s)
	}

	value, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}

	return int64(value), nil
}

// formatByteSize matches internal/http.byteSize's formatting exactly, so a summed total reads
// consistently with the per-invocation values it was derived from.
func formatByteSize(bytes int64) string {
	for _, u := range byteSizeUnits {
		if bytes >= u.factor {
			result := strconv.FormatFloat(float64(bytes)/float64(u.factor), 'f', 1, 64)
			result = strings.TrimSuffix(result, ".0")

			return result + u.suffix
		}
	}

	return strconv.FormatInt(bytes, 10) + "B"
}

// doneDirectMode has no daemon or native GOCACHE state to finalize, so it just reports the
// final cache summary accumulated by direct-mode invocation(s) recorded via AppendQuietRunStats.
func doneDirectMode() error {
	cacheDir := os.Getenv(envGHACacheDir)
	if cacheDir == "" {
		log.Printf("github-actions-done: direct mode recorded no cache dir, nothing to summarize")
		return nil
	}

	remoteURL := os.Getenv(envGHARemoteURL)
	auth := os.Getenv(envGHAAuth)
	sessionID := os.Getenv(envGHASessionID)

	statsPath := filepath.Join(cacheDir, quietRunStatsFilename)
	data, err := os.ReadFile(statsPath) //nolint:gosec // statsPath is derived from the configured cache dir.
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("github-actions-done: no run stats recorded at %s", statsPath)
			// No go invocations happened, but the session/report_<name> files are still worth
			// reporting -- their value doesn't depend on any cache activity having occurred.
			markRemoteSessionDone(remoteURL, auth, sessionID, sessionExtras("direct", StatsSummary{}))
			return nil
		}
		return fmt.Errorf("read run stats: %w", err)
	}

	var records []directRunRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record directRunRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			log.Printf("github-actions-done: parse run stats line: %s", err.Error())
			continue
		}
		records = append(records, record)
	}

	elapsed, haveElapsed := elapsedSinceInit()

	var final StatsSummary
	switch len(records) {
	case 0:
		log.Printf("github-actions-done: no run stats recorded at %s", statsPath)
	case 1:
		stats := records[0].StatsSummary
		if haveElapsed {
			stats.TotalTime = elapsed.String()
		}
		log.Printf("github-actions-done: cache summary: %s", stats.String())
		final = stats
	default:
		summaries := make([]StatsSummary, len(records))
		for i, r := range records {
			summaries[i] = r.StatsSummary
		}
		total := sumStatsSummaries(summaries)
		if haveElapsed {
			total.TotalTime = elapsed.String()
		}
		log.Printf("github-actions-done: cache summary across %d go invocations: %s", len(records), total.String())
		logParentCommandBreakdown(records)
		final = total
	}

	markRemoteSessionDone(remoteURL, auth, sessionID, sessionExtras("direct", final))

	if err := os.Remove(statsPath); err != nil && !os.IsNotExist(err) { //nolint:gosec // statsPath is derived from the configured cache dir.
		log.Printf("github-actions-done: remove run stats file: %s", err.Error())
	}

	return nil
}

// logParentCommandBreakdown reports which parent command lines actually spawned each
// direct-mode GOCACHEPROG invocation, most frequent first. This is the trace that answers "why
// are there N invocations" when N is far higher than the number of `go` commands in the workflow
// YAML itself — e.g. a Makefile target or test-splitting tool looping over packages.
func logParentCommandBreakdown(records []directRunRecord) {
	counts := map[string]int{}
	for _, r := range records {
		key := r.ParentCmd
		if key == "" {
			key = fmt.Sprintf("(unknown, parent_pid=%d)", r.ParentPID)
		}
		counts[key]++
	}

	type parentCount struct {
		cmd   string
		count int
	}

	entries := make([]parentCount, 0, len(counts))
	for cmd, count := range counts {
		entries = append(entries, parentCount{cmd, count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].cmd < entries[j].cmd
	})

	log.Printf("github-actions-done: %d distinct parent command(s) invoked GOCACHEPROG:", len(entries))
	for _, e := range entries {
		log.Printf("  [%dx] %s", e.count, e.cmd)
	}
}

func waitForShimSocket(socket string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", socket, 500*time.Millisecond)
		if err == nil {
			return conn.Close()
		}

		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}

	return lastErr
}

func killByPIDFile(pidFile string) {
	data, err := os.ReadFile(pidFile) //nolint:gosec // pidFile is a fixed path under os.TempDir().
	if err != nil {
		log.Printf("github-actions-done: read pid file %s: %s", pidFile, err.Error())
		return
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		log.Printf("github-actions-done: parse pid file %s: %s", pidFile, err.Error())
		return
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		log.Printf("github-actions-done: find daemon process %d: %s", pid, err.Error())
		return
	}

	log.Printf("github-actions-done: sending SIGTERM to daemon pid %d as a fallback", pid)
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		log.Printf("github-actions-done: signal daemon pid %d: %s", pid, err.Error())
	}
}

func tailFile(path string, maxBytes int64) string {
	data, err := os.ReadFile(path) //nolint:gosec // path is a fixed log path under os.TempDir().
	if err != nil {
		return fmt.Sprintf("(could not read log: %s)", err.Error())
	}

	if int64(len(data)) > maxBytes {
		data = data[int64(len(data))-maxBytes:]
	}

	return string(data)
}

func shellJoin(bin string, args []string) string {
	return strings.Join(append([]string{bin}, args...), " ")
}

func setGitHubEnv(vars map[string]string) error {
	githubEnv := os.Getenv("GITHUB_ENV")
	if githubEnv == "" {
		return errors.New("GITHUB_ENV environment variable is not set (not running in GitHub Actions?)")
	}

	file, err := os.OpenFile(githubEnv, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // GITHUB_ENV is provided by the GitHub Actions runner.
	if err != nil {
		return fmt.Errorf("open GITHUB_ENV: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("close GITHUB_ENV: %s", closeErr.Error())
		}
	}()

	for key, value := range vars {
		if _, err := fmt.Fprintf(file, "%s=%s\n", key, value); err != nil {
			return fmt.Errorf("write %s to GITHUB_ENV: %w", key, err)
		}
	}

	return nil
}
