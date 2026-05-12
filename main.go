package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/cacheprog"
	"github.com/vearutop/gocacheprog/internal/http"
	"github.com/vearutop/gocacheprog/internal/local"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	params := parseProxyParams()
	dir := flag.String("cache-dir", "", "cache directory; empty means automatic")
	listen := flag.String("listen", "", "listen address or unix socket path; when set, run as server instead of cache helper")
	dumpLogs := flag.String("dump-log", "", "dump req/resp logs to file")
	remoteURL := flag.String("remote-url", "", "remote HTTP server cache source, e.g. https://example.com:8080")
	authToken := flag.String("auth-token", "", "optional bearer token for the remote HTTP cache server")
	maxDiskBytes := flag.Int64("max-disk-bytes", 0, "optional total on-disk cache size limit in bytes; 0 disables eviction")
	preloadLimit := flag.Int("preload-limit", 2, "maximum number of concurrent preload preparations in server mode")
	canonicalize := flag.String("canonicalize-timestamps", "", "canonicalize file and directory timestamps under this repo root and exit")

	flag.Parse()

	if *canonicalize != "" {
		return local.CanonicalizeTimestamps(*canonicalize)
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

	if *listen != "" {
		if *remoteURL == "" {
			return runStoreServer(*listen, *dir, *authToken, *maxDiskBytes, *preloadLimit)
		}

		return runDaemon(*listen, *dir, *remoteURL, *authToken, *maxDiskBytes, params)
	}

	if isLocalRemoteURL(*remoteURL) {
		return runShim(*remoteURL, *authToken, *dumpLogs)
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
			Params:    params,
		})
		if err != nil {
			return fmt.Errorf("remote client: %w", err)
		}
	}

	store, err := local.NewStore(*dir, local.WithMaxDiskBytes(*maxDiskBytes))
	if err != nil {
		return fmt.Errorf("new cache store: %w", err)
	}

	dc := local.NewProxy(store, upstream, resps, params)

	app := local.NewApp(os.Stdin, os.Stdout, dc, resps, logDump)
	if err := dc.MaybePreload(); err != nil {
		return err
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

func runServer(listen string, store *local.Store, authToken string, preloadLimit int) error {
	return local.RunServer(listen, store, authToken, preloadLimit)
}

func parseProxyParams() local.ProxyParams {
	params := local.ProxyParams{}
	flag.BoolVar(&params.Preload, "preload", false, "preload cache from remote server")
	flag.Int64Var(&params.PreloadSize, "preload-size", 1000000, "preload cache from remote server fo items up to this size")
	flag.StringVar(&params.Commit, "commit", "", "current commit SHA used to upload cache usage manifest")
	flag.StringVar(&params.ChangesID, "changes-id", "", "stable change stream label used to upload and preload latest cache usage manifest")
	flag.StringVar(&params.BuildType, "build-type", "", "optional build type label to isolate cache manifests, e.g. unit or race")
	flag.StringVar(&params.BaseCommit, "base-commit", "", "base commit SHA used to scope preload")
	flag.StringVar(&params.ParentCommit, "parent-commit", "", "parent commit SHA used to scope preload")
	return params
}

func runStoreServer(listen, dir, authToken string, maxDiskBytes int64, preloadLimit int) error {
	store, err := local.NewStore(dir, local.WithCompression(), local.WithMaxDiskBytes(maxDiskBytes))
	if err != nil {
		return fmt.Errorf("init local storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("close store: %s", err.Error())
		}
	}()

	return runServer(listen, store, authToken, preloadLimit)
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
	proxy.Verbose = true

	ready := make(chan struct{})
	preloadErrCh := make(chan error, 1)
	go func() {
		preloadErrCh <- proxy.MaybePreload()
		close(ready)
	}()

	server := local.NewShimServer(proxy, resps, authToken, ready)
	err = server.Serve(listen, preloadErrCh)
	closeErr := proxy.Close()
	close(resps)
	if err != nil {
		return err
	}
	if closeErr != nil {
		return fmt.Errorf("close daemon proxy: %w", closeErr)
	}

	return nil
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
