// Package local implements gocacheprog's local-side building blocks: the on-disk cache
// store, the daemon/shim pair, and (this file) -github-actions-init/-github-actions-done,
// a condensed, single-DSN wrapper around the existing direct/shim/native-GOCACHE CLI modes
// aimed at GitHub Actions jobs that want sane defaults instead of hand-rolled bash plumbing.
//
// DSN format for -github-actions-init:
//
//	<remote-url>?auth=<token>&cache_dir=<dir>&preload_size=<bytes>&build_type=<type>&mode=direct|shim|gocache&canonicalize_timestamps=<path>&skip_canonicalize_timestamps=<bool>&skip_preload=<bool>
//
// Only the remote URL is required; every query parameter is optional:
//
//   - auth: bearer token for the remote server and (in shim mode) the local daemon socket
//   - cache_dir: local cache/GOCACHE directory; empty picks gocacheprog's own default
//   - preload_size: maps to -max-file-bytes (default 3,000,000)
//   - build_type: maps to -build-type, e.g. "unit" or "race"; always prefixed with
//     $GITHUB_REPOSITORY (e.g. "owner-repo-unit") so manifests and the /inspect and /clear
//     admin endpoints stay isolated per repository when multiple repos share one server
//   - mode: "direct" (no daemon, one gocacheprog per go invocation), "shim" (background
//     daemon + GOCACHEPROG pointed at its local socket, default), or "gocache" (native
//     GOCACHE restore-cache/save-cache, no GOCACHEPROG involved)
//   - canonicalize_timestamps: repo root to canonicalize before anything else; defaults to
//     "." (the checkout root) since fresh CI checkouts almost always need it for stable
//     cache keys; set skip_canonicalize_timestamps=true to opt out entirely
//   - skip_canonicalize_timestamps: when true, skips timestamp canonicalization entirely
//   - skip_preload: when true, skips the explicit preload pass entirely (direct/shim only)
//
// Commit, changes-id, and base-commit are derived automatically from GitHub Actions'
// own environment instead of being passed in: pull_request(_target) events use
// event.pull_request.head/base.sha and "<repo>#<number>"; every other event uses
// $GITHUB_SHA alone.
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

	envGHAMode         = "GOCACHEPROG_GHA_MODE"
	envGHASocket       = "GOCACHEPROG_GHA_SOCKET"
	envGHAAuth         = "GOCACHEPROG_GHA_AUTH"
	envGHAPIDFile      = "GOCACHEPROG_GHA_PID_FILE"
	envGHALogFile      = "GOCACHEPROG_GHA_LOG_FILE"
	envGHACacheDir     = "GOCACHEPROG_GHA_CACHE_DIR"
	envGHARemoteURL    = "GOCACHEPROG_GHA_REMOTE_URL"
	envGHACommit       = "GOCACHEPROG_GHA_COMMIT"
	envGHAChangesID    = "GOCACHEPROG_GHA_CHANGES_ID"
	envGHABuildType    = "GOCACHEPROG_GHA_BUILD_TYPE"
	envGHABaseCommit   = "GOCACHEPROG_GHA_BASE_COMMIT"
	envGHAMaxFileBytes = "GOCACHEPROG_GHA_MAX_FILE_BYTES"
)

type githubActionsConfig struct {
	remoteURL    string
	authToken    string
	cacheDir     string
	buildType    string
	mode         string
	canonicalize string
	maxFileBytes int64
	skipPreload  bool
}

// GithubActionsInit sets up caching for a GitHub Actions job from a single DSN. See the
// package doc comment above for the DSN format.
func GithubActionsInit(dsn string) error {
	cfg, err := parseGithubActionsDSN(dsn)
	if err != nil {
		return fmt.Errorf("github-actions-init: %w", err)
	}

	cfg.buildType = repoScopedBuildType(cfg.buildType)

	log.Printf("github-actions-init: mode=%q remote_url=%q cache_dir=%q build_type=%q preload_size=%d skip_preload=%t",
		cfg.mode, cfg.remoteURL, cfg.cacheDir, cfg.buildType, cfg.maxFileBytes, cfg.skipPreload)

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
		return initDirectMode(self, cfg, commit, baseCommit, changesID)
	case "shim":
		return initShimMode(self, cfg, commit, baseCommit, changesID)
	case "gocache":
		return initGocacheMode(cfg, commit, baseCommit, changesID)
	default:
		return fmt.Errorf("github-actions-init: unsupported mode %q (expected direct, shim, or gocache)", cfg.mode)
	}
}

// GithubActionsDone finalizes caching started by -github-actions-init. It reads back the
// state -github-actions-init left in $GITHUB_ENV and is a no-op for direct mode.
func GithubActionsDone() error {
	mode := os.Getenv(envGHAMode)
	log.Printf("github-actions-done: mode=%q", mode)

	switch mode {
	case "direct":
		log.Printf("github-actions-done: direct mode has no background state to finalize")
		return nil
	case "shim":
		return doneShimMode()
	case "gocache":
		return doneGocacheMode()
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

func initDirectMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID string) error {
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
		"-max-file-bytes", strconv.FormatInt(cfg.maxFileBytes, 10),
	}
	args = append(args, commonScopeArgs(cfg, commit, baseCommit, changesID)...)

	env := map[string]string{
		"GOCACHEPROG": shellJoin(self, args),
		envGHAMode:    "direct",
	}
	addDefaultGOMAXPROCS(env)

	log.Printf("github-actions-init: direct mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

func initShimMode(self string, cfg githubActionsConfig, commit, baseCommit, changesID string) error {
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

	clientArgs := []string{"-remote-url", "unix://" + socket}
	if cfg.authToken != "" {
		clientArgs = append(clientArgs, "-auth-token", cfg.authToken)
	}

	env := map[string]string{
		"GOCACHEPROG": shellJoin(self, clientArgs),
		envGHAMode:    "shim",
		envGHASocket:  socket,
		envGHAAuth:    cfg.authToken,
		envGHAPIDFile: pidFile,
		envGHALogFile: logFile,
	}
	addDefaultGOMAXPROCS(env)

	log.Printf("github-actions-init: shim mode ready, GOCACHEPROG=%q", env["GOCACHEPROG"])

	return setGitHubEnv(env)
}

func initGocacheMode(cfg githubActionsConfig, commit, baseCommit, changesID string) error {
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
	if err := RestoreNativeCache(cacheDir, client, req, startedAt); err != nil {
		return fmt.Errorf("restore native cache: %w", err)
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
	}

	log.Printf("github-actions-init: gocache mode ready, GOCACHE=%q", cacheDir)

	return setGitHubEnv(env)
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
	lines, stopErr := StopShimServer("unix://"+socket, auth)
	for _, line := range lines {
		log.Print(line)
	}

	if stopErr != nil {
		log.Printf("github-actions-done: graceful stop failed: %s", stopErr.Error())
		if pidFile != "" {
			killByPIDFile(pidFile)
		}
	} else {
		log.Printf("github-actions-done: daemon stopped gracefully")
	}

	if logFile != "" {
		log.Printf("github-actions-done: daemon log tail (%s):\n%s", logFile, tailFile(logFile, githubActionsLogTailBytes))
	}

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
	return SaveNativeCache(cacheDir, client, req, maxFileBytes)
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

func addDefaultGOMAXPROCS(env map[string]string) {
	if os.Getenv("GOMAXPROCS") != "" {
		return
	}

	env["GOMAXPROCS"] = "100"
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
