package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bool64/dev/version"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
	"github.com/vearutop/gocacheprog/internal/gocache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func main() {
	if err := run(); err != nil {
		// Bypasses the log package deliberately: -quiet redirects it to io.Discard, and a
		// fatal exit must always be visible regardless of that.
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

//nolint:gocyclo,maintidx // CLI mode dispatch is intentionally centralized here.
func run() error {
	startedAt := time.Now().UTC()
	params := parseProxyParams()
	dir := flag.String("cache-dir", "", "cache directory; empty means automatic")
	httpListen := flag.String("http", "", "HTTP listen address or unix socket path")
	httpsListen := flag.String("https", "", "HTTPS listen address")
	httpsHost := flag.String("https-host", "", "public hostname for automatic Let's Encrypt certificates")
	stop := flag.String("stop", "", "stop a running local daemon listening on the given unix/tcp address")
	dumpLogs := flag.String("dump-log", "", "dump req/resp logs to file")
	remoteURL := flag.String("remote-url", "", "remote HTTP server cache source, e.g. https://example.com:8080")
	authToken := flag.String("auth-token", "", "optional bearer token for the remote HTTP cache server")
	maxDiskBytes := flag.Int64("max-disk-bytes", 0, "optional total on-disk cache size limit in bytes; 0 disables eviction")
	gocacheMaxDiskBytes := flag.Int64("gocache-max-disk-bytes", 0, "optional total on-disk native cache storage size limit in bytes on the remote server; 0 disables eviction")
	gocacheMaxAge := flag.Duration("gocache-max-age", 48*time.Hour, "maximum age for native GOCACHE objects on the remote server; 0 disables age-based retirement")
	preloadLimit := flag.Int("preload-limit", 2, "maximum number of concurrent preload preparations in server mode")
	preloadOnly := flag.Bool("preload-only", false, "preload cache into -cache-dir and exit without running as helper or uploading cache-used")
	restoreCache := flag.Bool("restore-cache", false, "restore native GOCACHE files into -cache-dir and exit")
	saveCache := flag.Bool("save-cache", false, "save freshly created native GOCACHE files from -cache-dir and exit")
	maxFileBytes := flag.Int64("max-file-bytes", 0, "maximum single file size in bytes for remote cache storage, preload item wire size, and native -restore-cache/-save-cache; 0 disables the limit except preload defaults to 1000000")
	restoreLimitBytes := flag.Int64("restore-limit-bytes", 0, "maximum total compressed bytes to download during native -restore-cache after -max-file-bytes filtering; 0 disables the limit")
	saveCacheMaxFileBytes := flag.Int64("save-cache-max-file-bytes", 0, "deprecated alias for -max-file-bytes")
	saveCacheChunkBytes := flag.Int64("save-cache-chunk-bytes", http.DefaultSaveCacheChunkBytes, "maximum size in bytes for a single native -save-cache HTTP chunk request body")
	jobStartUnix := flag.Int64("job-start-unix", 0, "job start Unix timestamp in nanoseconds for -save-cache; when empty, read the marker written by -restore-cache")
	canonicalize := flag.String("canonicalize-timestamps", "", "canonicalize file and directory timestamps under this repo root and exit")
	githubActionsInit := flag.String("github-actions-init", "", "set up caching for a GitHub Actions job from a single DSN; see internal/local/github_actions.go for the DSN format")
	githubActionsDone := flag.Bool("github-actions-done", false, "finalize caching started by -github-actions-init in an always() step")
	quiet := flag.Bool("quiet", false, "suppress informational logging, keeping only fatal errors; used for GOCACHEPROG helper instances started via -github-actions-init so they don't clutter go build/test output")
	ver := flag.Bool("version", false, "print version and exit")

	flag.Parse()

	if *quiet {
		log.SetOutput(io.Discard)
	}

	if *ver {
		fmt.Println(version.Module("github.com/vearutop/gocacheprog").Version)
		return nil
	}

	if *githubActionsInit != "" {
		return local.GithubActionsInit(*githubActionsInit)
	}

	if *githubActionsDone {
		return local.GithubActionsDone()
	}

	if *canonicalize != "" {
		return local.CanonicalizeTimestamps(*canonicalize)
	}

	if *stop != "" {
		resp, err := local.StopShimServer(*stop, *authToken)
		for _, line := range resp.Lines {
			log.Print(line)
		}
		log.Printf("cache summary: %s", resp.Stats.String())
		return err
	}

	if *restoreCache || *saveCache {
		if *maxFileBytes == 0 && *saveCacheMaxFileBytes != 0 {
			*maxFileBytes = *saveCacheMaxFileBytes
		}
		_ = *jobStartUnix
		return runNativeGOCACHEMode(*dir, *httpListen, *remoteURL, *authToken, *restoreCache, *saveCache, *maxFileBytes, *restoreLimitBytes, *saveCacheChunkBytes, startedAt, params)
	}

	params.MaxFileBytes = *maxFileBytes

	if err := normalizeServerFlags(httpListen, httpsListen, httpsHost); err != nil {
		return err
	}

	if isLocalRemoteURL(*remoteURL) && *httpListen == "" && !*preloadOnly {
		return runShim(*remoteURL, *authToken, *dumpLogs, *quiet)
	}

	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("user cache dir: %w", err)
		}
		d = filepath.Join(d, "gocacheprog")
		log.Printf("Defaulting to cache dir %s ...", d)
		*dir = d
	}

	if err := os.MkdirAll(*dir, 0o750); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	if *preloadOnly {
		if *httpListen != "" || *httpsListen != "" || *httpsHost != "" {
			return errors.New("-preload-only cannot be combined with -http, -https, or -https-host")
		}
		if *remoteURL == "" {
			return errors.New("-preload-only requires -remote-url")
		}
		params.DisableCacheUsed = true
	}

	if *httpListen != "" || *httpsListen != "" || *httpsHost != "" {
		if *remoteURL == "" {
			return runStoreServer(*httpListen, *httpsListen, *httpsHost, *dir, *authToken, *maxDiskBytes, *gocacheMaxDiskBytes, *maxFileBytes, *gocacheMaxAge, *preloadLimit)
		}

		if *httpsHost != "" || *httpsListen != "" {
			return errors.New("-https and -https-host are only supported in store server mode without -remote-url")
		}

		return runDaemon(*httpListen, *dir, *remoteURL, *authToken, *maxDiskBytes, *params)
	}

	if !*quiet {
		println("starting at dir", *dir)
	}

	var (
		logDump  io.Writer
		upstream cache.Store
		err      error
	)

	if *dumpLogs != "" {
		f, err := os.Create(*dumpLogs)
		if err != nil {
			return fmt.Errorf("create dump logs file: %w", err)
		}

		logDump = f

		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("close dump log file: %s", closeErr.Error())
			}
		}()
	}

	resps := make(chan cacheprog.Response, 100)

	if *remoteURL != "" {
		sessionStartedAt := time.Now().UTC()
		upstream, err = http.NewClientWithSession(*remoteURL, *authToken, &http.SessionInfo{
			SessionID: fmt.Sprintf("%d-%d", os.Getpid(), sessionStartedAt.UnixNano()),
			StartedAt: sessionStartedAt,
			PID:       os.Getpid(),
			CacheDir:  *dir,
			Params:    *params,
		})
		if err != nil {
			return fmt.Errorf("remote client: %w", err)
		}
	}

	store, err := local.NewStore(*dir, local.WithMaxDiskBytes(*maxDiskBytes))
	if err != nil {
		return fmt.Errorf("new cache store: %w", err)
	}

	dc := local.NewProxy(store, upstream, resps, *params)

	app := local.NewApp(os.Stdin, os.Stdout, dc, resps, logDump)
	if err := dc.MaybePreload(); err != nil {
		return err
	}

	if *preloadOnly {
		if err := dc.Close(); err != nil {
			return fmt.Errorf("close cache after preload-only: %w", err)
		}
		close(resps)
		dc.PrintStats()
		return nil
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)

			dc.PrintStats()
		}
	}()

	go app.IterateResponses()

	if err := app.IterateInput(); err != nil {
		return err
	}

	if err := dc.Close(); err != nil {
		return fmt.Errorf("close cache: %w", err)
	}
	close(resps)

	if *quiet {
		if err := local.AppendQuietRunStats(*dir, dc.StatsSummary()); err != nil {
			log.Printf("append run stats: %s", err.Error())
		}
	} else {
		dc.PrintStats()
	}

	return nil
}

func runServer(httpListen, httpsListen, httpsHost, certCacheDir string, store *local.Store, nativeStore *gocache.Store, authToken string, preloadLimit int) error {
	return local.RunServer(httpListen, httpsListen, httpsHost, certCacheDir, store, nativeStore, authToken, preloadLimit)
}

func parseProxyParams() *local.ProxyParams {
	params := &local.ProxyParams{}
	flag.BoolVar(&params.Preload, "preload", false, "preload cache from remote server")
	flag.BoolVar(&params.SkipPreload, "skip-preload", false, "skip preload even when preload scope flags are present")
	flag.DurationVar(&params.MaxRemoteGetTime, "max-remote-get-time", 0, "once cumulative remote get time exceeds this duration, local misses stop querying remote and return immediately")
	flag.StringVar(&params.Commit, "commit", "", "current commit SHA used to upload cache usage manifest")
	flag.StringVar(&params.ChangesID, "changes-id", "", "stable change stream label used to upload and preload latest cache usage manifest")
	flag.StringVar(&params.BuildType, "build-type", "", "optional build type label to isolate cache manifests, e.g. unit or race")
	flag.StringVar(&params.BaseCommit, "base-commit", "", "base commit SHA used to scope preload")
	flag.StringVar(&params.ParentCommit, "parent-commit", "", "parent commit SHA used to scope preload")
	flag.IntVar(&params.RemoteBatchConcurrency, "remote-batch-concurrency", 0, "maximum number of batched remote Get round trips in flight at once; 0 uses a sane default")
	return params
}

func runStoreServer(httpListen, httpsListen, httpsHost, dir, authToken string, maxDiskBytes int64, gocacheMaxDiskBytes int64, maxFileBytes int64, gocacheMaxAge time.Duration, preloadLimit int) error {
	store, err := local.NewStore(dir, local.WithCompression(), local.WithMaxDiskBytes(maxDiskBytes), local.WithMaxFileBytes(maxFileBytes))
	if err != nil {
		return fmt.Errorf("init local storage: %w", err)
	}
	nativeStore, err := gocache.NewStore(
		filepath.Join(dir, "native-gocache"),
		gocache.WithCompression(),
		gocache.WithMaxDiskBytes(gocacheMaxDiskBytes),
		gocache.WithMaxFileBytes(maxFileBytes),
		gocache.WithMaxAge(gocacheMaxAge),
	)
	if err != nil {
		return fmt.Errorf("init native GOCACHE storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close store: %s", err.Error())
		}
		if err := nativeStore.Close(); err != nil {
			log.Printf("close native GOCACHE store: %s", err.Error())
		}
	}()

	return runServer(httpListen, httpsListen, httpsHost, filepath.Join(dir, "autocert"), store, nativeStore, authToken, preloadLimit)
}

func runNativeGOCACHEMode(dir, httpListen, remoteURL, authToken string, restoreCache, saveCache bool, maxFileBytes, restoreLimitBytes, saveCacheChunkBytes int64, startedAt time.Time, params *local.ProxyParams) error {
	if restoreCache && saveCache {
		return errors.New("-restore-cache and -save-cache are mutually exclusive")
	}
	if httpListen != "" {
		return errors.New("native GOCACHE batch mode cannot be combined with -http")
	}
	if remoteURL == "" {
		return errors.New("native GOCACHE batch mode requires -remote-url")
	}

	cacheDir, err := local.ResolveNativeCacheDir(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return fmt.Errorf("ensure native cache dir: %w", err)
	}

	client, err := http.NewClientWithSession(remoteURL, authToken, &http.SessionInfo{
		SessionID: fmt.Sprintf("%d-%d", os.Getpid(), startedAt.UnixNano()),
		StartedAt: startedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		Params:    *params,
	})
	if err != nil {
		return fmt.Errorf("remote client: %w", err)
	}
	client.SetSaveCacheChunkBytes(saveCacheChunkBytes)

	req := gocache.Request{
		Commit:            params.Commit,
		ChangesID:         params.ChangesID,
		BuildType:         params.BuildType,
		BaseCommit:        params.BaseCommit,
		ParentCommit:      params.ParentCommit,
		MaxFileBytes:      maxFileBytes,
		RestoreLimitBytes: restoreLimitBytes,
	}

	if restoreCache {
		_, err := local.RestoreNativeCache(cacheDir, client, req, startedAt)
		return err
	}

	_, err = local.SaveNativeCache(cacheDir, client, req, maxFileBytes)
	return err
}

func runDaemon(listen, dir, remoteURL, authToken string, maxDiskBytes int64, params local.ProxyParams) error {
	upstream, err := newUpstreamClient(remoteURL, authToken, dir, params)
	if err != nil {
		return fmt.Errorf("remote client: %w", err)
	}

	store, err := local.NewStore(dir, local.WithMaxDiskBytes(maxDiskBytes))
	if err != nil {
		return fmt.Errorf("new cache store: %w", err)
	}

	resps := make(chan cacheprog.Response, 100)
	proxy := local.NewProxy(store, upstream, resps, params)
	recentLogf := newRecentLogf(20)
	proxy.Logf = recentLogf.Logf

	ready := make(chan struct{})
	var preloadMu sync.Mutex
	var preloadErr error
	preloadErrCh := make(chan error, 1)
	go func() {
		err := proxy.MaybePreload()
		preloadMu.Lock()
		preloadErr = err
		preloadMu.Unlock()
		preloadErrCh <- err
		close(ready)
	}()

	server := local.NewShimServer(proxy, resps, authToken, ready, func() error {
		preloadMu.Lock()
		defer preloadMu.Unlock()
		return preloadErr
	})
	err = server.Serve(listen, preloadErrCh)
	stopRequested := errors.Is(err, local.ErrStopRequested)
	if stopRequested {
		err = nil
	}
	proxy.PrintStats()
	closeErr := proxy.Close()
	close(resps)
	if stopRequested {
		stats := proxy.StatsSummary()
		stats.Invocations = server.SessionsSeen()
		resp := local.ShimStopResponse{Lines: recentLogf.Lines(), Stats: stats}
		if err != nil {
			resp.Err = err.Error()
		} else if closeErr != nil {
			resp.Err = "close daemon proxy: " + closeErr.Error()
		}
		server.ReplyStop(resp)
	}
	if err != nil {
		return err
	}
	if closeErr != nil {
		return fmt.Errorf("close daemon proxy: %w", closeErr)
	}

	return nil
}

type recentLogf struct {
	limit int
	mu    sync.Mutex
	lines []string
}

func newRecentLogf(limit int) *recentLogf {
	return &recentLogf{limit: limit}
}

func (r *recentLogf) Logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	log.Print(line)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.lines = append(r.lines, line)
	if len(r.lines) > r.limit {
		r.lines = append([]string(nil), r.lines[len(r.lines)-r.limit:]...)
	}
}

func (r *recentLogf) Lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.lines...)
}

func newUpstreamClient(remoteURL, authToken, cacheDir string, params local.ProxyParams) (cache.Store, error) {
	sessionStartedAt := time.Now().UTC()
	return http.NewClientWithSession(remoteURL, authToken, &http.SessionInfo{
		SessionID: fmt.Sprintf("%d-%d", os.Getpid(), sessionStartedAt.UnixNano()),
		StartedAt: sessionStartedAt,
		PID:       os.Getpid(),
		CacheDir:  cacheDir,
		Params:    params,
	})
}

func runShim(remoteURL string, authToken string, dumpLogs string, quiet bool) error {
	if !quiet {
		println("starting shim via", remoteURL)
	}

	var logDump io.Writer
	if dumpLogs != "" {
		f, err := os.Create(dumpLogs) //nolint:gosec // dump log path is an explicit local CLI parameter.
		if err != nil {
			return fmt.Errorf("create dump logs file: %w", err)
		}
		logDump = f
		defer func() {
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("close dump log file: %s", closeErr.Error())
			}
		}()
	}

	client, err := local.NewShimClient(remoteURL, authToken, fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UTC().UnixNano()))
	if err != nil {
		return fmt.Errorf("daemon client: %w", err)
	}

	err = local.ProcessShimSession(os.Stdin, os.Stdout, logDump, client)
	if errors.Is(err, local.ErrShimCloseTimeout) {
		local.RecordShimForcedClose(remoteURL)
	}

	return err
}

func isLocalRemoteURL(remoteURL string) bool {
	if remoteURL == "" {
		return false
	}

	if strings.HasPrefix(remoteURL, "unix://") {
		return true
	}

	u, err := url.Parse(remoteURL)
	if err != nil || u.Host == "" {
		return false
	}

	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1"
}

func normalizeServerFlags(httpListen, httpsListen, httpsHost *string) error {
	if *httpsHost == "" {
		if *httpsListen != "" {
			return errors.New("-https requires -https-host")
		}

		return nil
	}

	if isUnixListen(*httpListen) {
		return errors.New("-https-host cannot be combined with unix -http")
	}

	if *httpListen == "" {
		*httpListen = ":80"
	}

	if network, port, err := listenPort(*httpListen); err != nil {
		return fmt.Errorf("invalid -http for -https-host: %w", err)
	} else if network != "tcp" {
		return errors.New("-https-host requires TCP -http")
	} else if port != "80" {
		return fmt.Errorf("-https-host requires -http on port 80, got %q", *httpListen)
	}

	if *httpsListen == "" {
		*httpsListen = ":443"
	}

	if network, _, err := listenPort(*httpsListen); err != nil {
		return fmt.Errorf("invalid -https: %w", err)
	} else if network != "tcp" {
		return errors.New("-https must be a TCP address")
	}

	return nil
}

func isUnixListen(listen string) bool {
	return strings.HasPrefix(listen, "unix://")
}

func listenPort(listen string) (string, string, error) {
	if listen == "" {
		return "", "", nil
	}

	if isUnixListen(listen) {
		return "unix", "", nil
	}

	_, port, err := net.SplitHostPort(listen)
	if err == nil {
		return "tcp", port, nil
	}

	addrErr := &net.AddrError{}
	if errors.As(err, &addrErr) && strings.Contains(addrErr.Err, "missing port in address") {
		return "", "", err
	}

	if strings.Count(listen, ":") == 1 && strings.HasPrefix(listen, ":") {
		return "tcp", strings.TrimPrefix(listen, ":"), nil
	}

	if strings.Count(listen, ":") > 1 && !strings.HasPrefix(listen, "[") {
		return "", "", err
	}

	host, port, splitErr := net.SplitHostPort(net.JoinHostPort(listen, ""))
	if splitErr == nil {
		_ = host
		return "tcp", port, nil
	}

	return "", "", err
}
