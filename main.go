package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
	"github.com/vearutop/gocacheprog/internal/gocache"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

//nolint:gocyclo // CLI mode dispatch is intentionally centralized here.
func run() error {
	startedAt := time.Now().UTC()
	params := parseProxyParams()
	dir := flag.String("cache-dir", "", "cache directory; empty means automatic")
	listen := flag.String("listen", "", "listen address or unix socket path; when set, run as server instead of cache helper")
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
	maxFileBytes := flag.Int64("max-file-bytes", 0, "maximum single file size in bytes for remote cache storage and native -restore-cache/-save-cache; 0 disables the limit")
	saveCacheMaxFileBytes := flag.Int64("save-cache-max-file-bytes", 0, "deprecated alias for -max-file-bytes")
	maxFileSize := flag.Int64("max-file-size", 0, "deprecated alias for -max-file-bytes")
	saveCacheChunkBytes := flag.Int64("save-cache-chunk-bytes", http.DefaultSaveCacheChunkBytes, "maximum size in bytes for a single native -save-cache HTTP chunk request body")
	jobStartUnix := flag.Int64("job-start-unix", 0, "job start Unix timestamp in nanoseconds for -save-cache; when empty, read the marker written by -restore-cache")
	canonicalize := flag.String("canonicalize-timestamps", "", "canonicalize file and directory timestamps under this repo root and exit")

	flag.Parse()

	if *canonicalize != "" {
		return local.CanonicalizeTimestamps(*canonicalize)
	}

	if *stop != "" {
		lines, err := local.StopShimServer(*stop, *authToken)
		for _, line := range lines {
			log.Print(line)
		}
		return err
	}

	if *restoreCache || *saveCache {
		if *maxFileBytes == 0 {
			switch {
			case *saveCacheMaxFileBytes != 0:
				*maxFileBytes = *saveCacheMaxFileBytes
			case *maxFileSize != 0:
				*maxFileBytes = *maxFileSize
			}
		}
		_ = *jobStartUnix
		return runNativeGOCACHEMode(*dir, *listen, *remoteURL, *authToken, *restoreCache, *saveCache, *maxFileBytes, *saveCacheChunkBytes, startedAt, params)
	}

	params.MaxFileBytes = *maxFileBytes

	if isLocalRemoteURL(*remoteURL) && *listen == "" && !*preloadOnly {
		return runShim(*remoteURL, *authToken, *dumpLogs)
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
		if *listen != "" {
			return errors.New("-preload-only cannot be combined with -listen")
		}
		if *remoteURL == "" {
			return errors.New("-preload-only requires -remote-url")
		}
		params.DisableCacheUsed = true
	}

	if *listen != "" {
		if *remoteURL == "" {
			return runStoreServer(*listen, *dir, *authToken, *maxDiskBytes, *gocacheMaxDiskBytes, *maxFileBytes, *gocacheMaxAge, *preloadLimit)
		}

		return runDaemon(*listen, *dir, *remoteURL, *authToken, *maxDiskBytes, *params)
	}

	println("starting at dir", *dir)

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

	dc.PrintStats()

	return nil
}

func runServer(listen string, store *local.Store, nativeStore *gocache.Store, authToken string, preloadLimit int) error {
	return local.RunServer(listen, store, nativeStore, authToken, preloadLimit)
}

func parseProxyParams() *local.ProxyParams {
	params := &local.ProxyParams{}
	flag.BoolVar(&params.Preload, "preload", false, "preload cache from remote server")
	flag.BoolVar(&params.SkipPreload, "skip-preload", false, "skip preload even when preload scope flags are present")
	flag.DurationVar(&params.MaxRemoteGetTime, "max-remote-get-time", 0, "once cumulative remote get time exceeds this duration, local misses stop querying remote and return immediately")
	flag.Int64Var(&params.PreloadSize, "preload-size", 1000000, "preload cache from remote server fo items up to this size")
	flag.StringVar(&params.Commit, "commit", "", "current commit SHA used to upload cache usage manifest")
	flag.StringVar(&params.ChangesID, "changes-id", "", "stable change stream label used to upload and preload latest cache usage manifest")
	flag.StringVar(&params.BuildType, "build-type", "", "optional build type label to isolate cache manifests, e.g. unit or race")
	flag.StringVar(&params.BaseCommit, "base-commit", "", "base commit SHA used to scope preload")
	flag.StringVar(&params.ParentCommit, "parent-commit", "", "parent commit SHA used to scope preload")
	return params
}

func runStoreServer(listen, dir, authToken string, maxDiskBytes int64, gocacheMaxDiskBytes int64, maxFileBytes int64, gocacheMaxAge time.Duration, preloadLimit int) error {
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

	return runServer(listen, store, nativeStore, authToken, preloadLimit)
}

func resolveNativeCacheDir(dir string) (string, error) {
	if dir != "" {
		return resolveAbsPath(dir)
	}

	if envDir := strings.TrimSpace(os.Getenv("GOCACHE")); envDir != "" {
		return resolveAbsPath(envDir)
	}

	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("user cache dir: %w", err)
	}

	return filepath.Join(userCacheDir, "go-build"), nil
}

func resolveAbsPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("user home dir: %w", err)
		}
		if path == "~" {
			path = homeDir
		} else {
			path = filepath.Join(homeDir, path[2:])
		}
	}

	return filepath.Abs(path)
}

func runNativeGOCACHEMode(dir, listen, remoteURL, authToken string, restoreCache, saveCache bool, maxFileBytes, saveCacheChunkBytes int64, startedAt time.Time, params *local.ProxyParams) error {
	if restoreCache && saveCache {
		return errors.New("-restore-cache and -save-cache are mutually exclusive")
	}
	if listen != "" {
		return errors.New("native GOCACHE batch mode cannot be combined with -listen")
	}
	if remoteURL == "" {
		return errors.New("native GOCACHE batch mode requires -remote-url")
	}

	cacheDir, err := resolveNativeCacheDir(dir)
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
		Commit:       params.Commit,
		ChangesID:    params.ChangesID,
		BuildType:    params.BuildType,
		BaseCommit:   params.BaseCommit,
		ParentCommit: params.ParentCommit,
		MaxFileBytes: maxFileBytes,
	}

	if restoreCache {
		return runRestoreCache(cacheDir, client, req, startedAt)
	}

	return runSaveCache(cacheDir, client, req, maxFileBytes)
}

func runRestoreCache(cacheDir string, client *http.Client, req gocache.Request, startedAt time.Time) error {
	restoredPaths := make([]string, 0)
	stats, err := client.RestoreCache(req, func(item gocache.FileItem, body io.Reader) error {
		restoredPaths = append(restoredPaths, item.Path)
		return gocache.RestoreToDir(cacheDir, item, body)
	})
	if err != nil {
		return err
	}
	restorePrepareTime, restoreTotalTime := client.LastRestoreTimings()
	log.Printf(
		"restore-cache completed: files=%d download_time=%s compressed=%s compressed_rate=%s uncompressed=%s uncompressed_rate=%s server_prepare_time=%q server_total_time=%q; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q sources=%q",
		stats.Files,
		stats.Duration,
		humanBytes(stats.CompressedBytes),
		humanBytesPerSecond(stats.CompressedBytes, stats.Duration),
		humanBytes(stats.UncompressedBytes),
		humanBytesPerSecond(stats.UncompressedBytes, stats.Duration),
		restorePrepareTime,
		restoreTotalTime,
		req.Commit,
		req.ChangesID,
		req.BuildType,
		req.BaseCommit,
		req.ParentCommit,
		client.LastRestoreSources(),
	)

	if err := gocache.WriteRestoredPaths(cacheDir, restoredPaths); err != nil {
		return err
	}

	return gocache.WriteJobStartMarker(cacheDir, startedAt)
}

func runSaveCache(cacheDir string, client *http.Client, req gocache.Request, maxFileBytes int64) error {
	batch, err := gocache.CollectFreshFiles(cacheDir, maxFileBytes)
	if err != nil {
		return err
	}
	if len(batch.Items) == 0 {
		log.Printf(
			"save-cache completed: files=0 upload_time=0s compressed=0 B uncompressed=0 B; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q",
			req.Commit,
			req.ChangesID,
			req.BuildType,
			req.BaseCommit,
			req.ParentCommit,
		)
		return nil
	}

	stats, err := client.SaveCache(req, batch)
	if err != nil {
		return err
	}
	saveTotalTime := client.LastSaveTiming()
	log.Printf(
		"save-cache completed: files=%d upload_time=%s compressed=%s uncompressed=%s server_total_time=%q; commit=%q changes_id=%q build_type=%q base_commit=%q parent_commit=%q",
		stats.Files,
		stats.Duration,
		humanBytes(stats.CompressedBytes),
		humanBytes(stats.UncompressedBytes),
		saveTotalTime,
		req.Commit,
		req.ChangesID,
		req.BuildType,
		req.BaseCommit,
		req.ParentCommit,
	)
	return nil
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
		resp := local.ShimStopResponse{Lines: recentLogf.Lines()}
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

func humanBytes(v int64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%d B", v)
	}

	div, exp := int64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(v)/float64(div), "KMGTPE"[exp])
}

func humanBytesPerSecond(bytes int64, d time.Duration) string {
	if bytes <= 0 || d <= 0 {
		return "0 B/s"
	}

	return humanBytes(int64(float64(bytes)/d.Seconds())) + "/s"
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

func runShim(remoteURL string, authToken string, dumpLogs string) error {
	println("starting shim via", remoteURL)

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

	return local.ProcessShimSession(os.Stdin, os.Stdout, logDump, client)
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
