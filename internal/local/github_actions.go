// Package local implements gocacheprog's local-side building blocks: the on-disk cache
// store, the daemon/shim pair, and (this file) -github-actions-init/-github-actions-done,
// a condensed, single-DSN wrapper around the existing direct/shim/native-GOCACHE CLI modes
// aimed at GitHub Actions jobs that want sane defaults instead of hand-rolled bash plumbing.
//
// DSN format for -github-actions-init:
//
//	<remote-url>?auth=<token>&cache_dir=<dir>&preload_size=<bytes>&build_type=<type>&mode=direct|shim|gocache|local-gocache&canonicalize_timestamps=<path>&skip_canonicalize_timestamps=<bool>&skip_preload=<bool>&max_cache_bytes=<bytes>
//
// Only the remote URL is required; every query parameter is optional:
//
//   - auth: bearer token for the remote server and (in shim mode) the local daemon socket
//   - cache_dir: local cache/GOCACHE directory; empty picks gocacheprog's own default; a
//     leading "~/" is resolved against the user's home directory
//   - preload_size: maps to -max-file-bytes (default 3,000,000)
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
// local-gocache mode never talks to a remote at all (init just points GOCACHE at cache_dir), so
// both init and done report the cache dir's file count/size plus its per-build-type usage stats
// (see localGocacheStats), and done additionally enforces max_cache_bytes by eviction if set.
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
	githubActionsShimSocketWait           = 10 * time.Second
	githubActionsLogTailBytes       int64 = 8_000

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
)

type githubActionsConfig struct {
	remoteURL     string
	authToken     string
	cacheDir      string
	buildType     string
	mode          string
	canonicalize  string
	maxFileBytes  int64
	maxCacheBytes int64
	skipPreload   bool
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

	log.Printf("github-actions-init: mode=%q remote_url=%q cache_dir=%q build_type=%q preload_size=%d skip_preload=%t max_cache_bytes=%d",
		cfg.mode, cfg.remoteURL, cfg.cacheDir, cfg.buildType, cfg.maxFileBytes, cfg.skipPreload, cfg.maxCacheBytes)

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

	switch cfg.mode {
	case "direct":
		return initDirectMode(self, cfg, commit, baseCommit, changesID, initStartedAt)
	case "shim":
		return initShimMode(self, cfg, commit, baseCommit, changesID, initStartedAt)
	case "gocache":
		return initGocacheMode(cfg, commit, baseCommit, changesID, initStartedAt)
	case "local-gocache":
		return initLocalGocacheMode(cfg, initStartedAt)
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
		authToken:    q.Get("auth"),
		cacheDir:     q.Get("cache_dir"),
		buildType:    q.Get("build_type"),
		mode:         q.Get("mode"),
		maxFileBytes: defaultGithubActionsPreloadSize,
		canonicalize: ".",
	}

	if cfg.mode == "" {
		cfg.mode = "shim"
	}

	if v := q.Get("preload_size"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return githubActionsConfig{}, fmt.Errorf("invalid preload_size %q: %w", v, err)
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

	u.RawQuery = ""
	cfg.remoteURL = u.String()

	return cfg, nil
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

func commonScopeArgs(cfg githubActionsConfig, commit, baseCommit, changesID string) []string {
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

	return args
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
func runPreloadOnly(self, cacheDir string, cfg githubActionsConfig, commit, baseCommit, changesID string) {
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
	args = append(args, commonScopeArgs(cfg, commit, baseCommit, changesID)...)

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

func initDirectMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID string, initStartedAt time.Time) error {
	cacheDir, err := resolveHelperCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	runPreloadOnly(self, cacheDir, cfg, commit, baseCommit, changesID)

	args := []string{
		"-cache-dir", cacheDir,
		"-remote-url", cfg.remoteURL,
		"-skip-preload",
		"-quiet",
		"-max-file-bytes", strconv.FormatInt(cfg.maxFileBytes, 10),
	}
	args = append(args, commonScopeArgs(cfg, commit, baseCommit, changesID)...)

	env := map[string]string{
		"GOCACHEPROG":  shellJoin(self, args),
		envGHAMode:     "direct",
		envGHACacheDir: cacheDir,
		envGHAInitTime: initStartedAt.Format(time.RFC3339Nano),
	}

	log.Printf("github-actions-init: direct mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

func initShimMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID string, initStartedAt time.Time) error {
	cacheDir, err := resolveHelperCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	runPreloadOnly(self, cacheDir, cfg, commit, baseCommit, changesID)

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
	daemonArgs = append(daemonArgs, commonScopeArgs(cfg, commit, baseCommit, changesID)...)

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
		"GOCACHEPROG":  shellJoin(self, clientArgs),
		envGHAMode:     "shim",
		envGHASocket:   socket,
		envGHAAuth:     cfg.authToken,
		envGHAPIDFile:  pidFile,
		envGHALogFile:  logFile,
		envGHAInitTime: initStartedAt.Format(time.RFC3339Nano),
	}

	log.Printf("github-actions-init: shim mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

func initGocacheMode(cfg githubActionsConfig, commit, baseCommit, changesID string, initStartedAt time.Time) error {
	cacheDir, err := ResolveNativeCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure native cache dir: %w", err)
	}

	startedAt := time.Now().UTC()
	client, err := cachehttp.NewClientWithSession(cfg.remoteURL, cfg.authToken, &cachehttp.SessionInfo{
		SessionID: fmt.Sprintf("%d-%d", os.Getpid(), startedAt.UnixNano()),
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  cfg.buildType,
			BaseCommit: baseCommit,
		},
	})
	if err != nil {
		return fmt.Errorf("remote client: %w", err)
	}

	req := gocache.Request{
		Commit:       commit,
		ChangesID:    changesID,
		BuildType:    cfg.buildType,
		BaseCommit:   baseCommit,
		MaxFileBytes: cfg.maxFileBytes,
	}

	log.Printf("github-actions-init: restoring native GOCACHE into %s from %s", cacheDir, cfg.remoteURL)
	restoreStats, err := RestoreNativeCache(cacheDir, client, req, startedAt)
	if err != nil {
		return fmt.Errorf("restore native cache: %w", err)
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
	}

	log.Printf("github-actions-init: gocache mode ready, GOCACHE=%q", cacheDir)

	return setGitHubEnv(env)
}

// initLocalGocacheMode points GOCACHE straight at cache_dir with no remote server involved at
// all: no restore, no preload, nothing to authenticate. It's a fallback to classic local GOCACHE
// reuse for self-hosted runners with a persistent home directory across jobs, where the best
// possible cache hit rate comes from just letting `go` read/write its own on-disk cache in place
// rather than paying for a remote round trip.
func initLocalGocacheMode(cfg githubActionsConfig, initStartedAt time.Time) error {
	cacheDir, err := ResolveNativeCacheDir(cfg.cacheDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure native cache dir: %w", err)
	}

	logCacheDirStats("github-actions-init", cacheDir)

	if stats, err := loadLocalGocacheStats(cacheDir); err != nil {
		log.Printf("github-actions-init: load %s: %s", localGocacheStatsFilename, err.Error())
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

	log.Printf("github-actions-init: local-gocache mode ready, GOCACHE=%q", cacheDir)

	return setGitHubEnv(env)
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

func doneShimMode() error {
	socket := os.Getenv(envGHASocket)
	auth := os.Getenv(envGHAAuth)
	logFile := os.Getenv(envGHALogFile)
	pidFile := os.Getenv(envGHAPIDFile)

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
		return nil
	}

	log.Printf("github-actions-done: daemon stopped gracefully")

	stats := resp.Stats
	stats.ForcedCloses = CountShimForcedCloses("unix://" + socket)
	if elapsed, ok := elapsedSinceInit(); ok {
		stats.TotalTime = elapsed.String()
	}
	log.Printf("github-actions-done: cache summary: %s", stats.String())

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
	client, err := cachehttp.NewClientWithSession(remoteURL, auth, &cachehttp.SessionInfo{
		SessionID: fmt.Sprintf("%d-%d", os.Getpid(), startedAt.UnixNano()),
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		Params: ProxyParams{
			Commit:     commit,
			ChangesID:  changesID,
			BuildType:  buildType,
			BaseCommit: baseCommit,
		},
	})
	if err != nil {
		return fmt.Errorf("remote client: %w", err)
	}

	req := gocache.Request{
		Commit:       commit,
		ChangesID:    changesID,
		BuildType:    buildType,
		BaseCommit:   baseCommit,
		MaxFileBytes: maxFileBytes,
	}

	log.Printf("github-actions-done: saving native GOCACHE from %s to %s", cacheDir, remoteURL)
	saveStats, err := SaveNativeCache(cacheDir, client, req, maxFileBytes)
	if err != nil {
		return err
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

	return nil
}

// doneLocalGocacheMode has no remote state to finalize and, unlike doneGocacheMode, uploads
// nothing back to a remote: local-gocache mode's whole point is letting the persistent cache
// dir accumulate across jobs on the runner's own disk. It just reports the cache dir's final
// file count/size so the effect of a job is visible in the log.
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

	// Scanned once here and reused for both the size log below and eviction, rather than
	// scanning cache_dir a second time regardless of whether max_cache_bytes is even set.
	entries, err := scanCacheDir(cacheDir)
	if err != nil {
		log.Printf("github-actions-done: stat cache dir %s: %s", cacheDir, err.Error())
	} else {
		var size int64
		for _, e := range entries {
			size += e.size
		}
		log.Printf("github-actions-done: cache dir %s currently has %d file(s), %s", cacheDir, len(entries), humanBytesBinary(size))

		if maxCacheBytes, err := strconv.ParseInt(os.Getenv(envGHAMaxCacheBytes), 10, 64); err == nil && maxCacheBytes > 0 {
			evictOldestUntilFits(cacheDir, entries, maxCacheBytes)
		}
	}

	if elapsed, ok := elapsedSinceInit(); ok {
		log.Printf("github-actions-done: total_time=%s", elapsed)
	}

	return nil
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

	statsPath := filepath.Join(cacheDir, quietRunStatsFilename)
	data, err := os.ReadFile(statsPath) //nolint:gosec // statsPath is derived from the configured cache dir.
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("github-actions-done: no run stats recorded at %s", statsPath)
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

	switch len(records) {
	case 0:
		log.Printf("github-actions-done: no run stats recorded at %s", statsPath)
	case 1:
		stats := records[0].StatsSummary
		if haveElapsed {
			stats.TotalTime = elapsed.String()
		}
		log.Printf("github-actions-done: cache summary: %s", stats.String())
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
	}

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
