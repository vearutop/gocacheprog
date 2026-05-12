package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	nethttp "net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"github.com/vearutop/gocacheprogd/internal/http"
	"github.com/vearutop/gocacheprogd/internal/local"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	dir := flag.String("cache-dir", "", "cache directory; empty means automatic")
	listen := flag.String("listen", "", "listen address or unix socket path; when set, run as server instead of cache helper")
	dumpLogs := flag.String("dump-log", "", "dump req/resp logs to file")
	remoteURL := flag.String("remote-url", "", "remote HTTP server cache source, e.g. https://example.com:8080")
	authToken := flag.String("auth-token", "", "optional bearer token for the remote HTTP cache server")
	maxDiskBytes := flag.Int64("max-disk-bytes", 0, "optional total on-disk cache size limit in bytes; 0 disables eviction")
	preloadLimit := flag.Int("preload-limit", 2, "maximum number of concurrent preload preparations in server mode")
	preload := flag.Bool("preload", false, "preload cache from remote server")
	preloadSize := flag.Int64("preload-size", 1000000, "preload cache from remote server fo items up to this size")
	commit := flag.String("commit", "", "current commit SHA used to upload cache usage manifest")
	changesID := flag.String("changes-id", "", "stable change stream label used to upload and preload latest cache usage manifest")
	buildType := flag.String("build-type", "", "optional build type label to isolate cache manifests, e.g. unit or race")
	baseCommit := flag.String("base-commit", "", "base commit SHA used to scope preload")
	parentCommit := flag.String("parent-commit", "", "parent commit SHA used to scope preload")
	canonicalize := flag.String("canonicalize-timestamps", "", "canonicalize file and directory timestamps under this repo root and exit")

	flag.Parse()

	if *canonicalize != "" {
		return canonicalizeTimestamps(*canonicalize)
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

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	if *listen != "" {
		return runServer(*listen, *dir, *authToken, *maxDiskBytes, *preloadLimit)
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

		defer f.Close()
	}

	resps := make(chan cacheprog.Response, 100)

	if *remoteURL != "" {
		sessionStartedAt := time.Now().UTC()
		upstream, err = http.NewClientWithSession(*remoteURL, *authToken, &http.SessionInfo{
			SessionID:  fmt.Sprintf("%d-%d", os.Getpid(), sessionStartedAt.UnixNano()),
			StartedAt:  sessionStartedAt,
			PID:        os.Getpid(),
			CacheDir:   *dir,
			Commit:     *commit,
			Parent:     *parentCommit,
			ChangesID:  *changesID,
			BuildType:  *buildType,
			BaseCommit: *baseCommit,
		})
		if err != nil {
			return fmt.Errorf("remote client: %w", err)
		}
	}

	dc, err := local.NewProxy(*dir, upstream, resps)
	if err != nil {
		return fmt.Errorf("new cache: %w", err)
	}

	dc.Verbose = true
	initialLocalEntries := dc.HasLocalEntries()
	app := local.NewApp(os.Stdin, os.Stdout, dc, resps, logDump)

	if *preload || *commit != "" || *changesID != "" || *buildType != "" || *baseCommit != "" || *parentCommit != "" {
		if dc.HasLocalEntries() {
			println("skipping preload because local cache dir is already populated")
		} else {
			st := time.Now()
			println("preloading cache up to", *preloadSize, "bytes per item from remote server ...")
			if err := dc.Preload(cache.PreloadRequest{
				MaxSize:      *preloadSize,
				Commit:       *commit,
				ChangesID:    *changesID,
				BuildType:    *buildType,
				BaseCommit:   *baseCommit,
				ParentCommit: *parentCommit,
			}); err != nil {
				return fmt.Errorf("preload cache: %w", err)
			}

			if s, ok := upstream.(interface{ LastPreloadSources() string }); ok {
				if sources := s.LastPreloadSources(); sources != "" {
					println("preload sources:", sources)
				}
			}

			println("preload done in", time.Since(st).String())
		}
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

	if err := dc.PostCacheUsed(*commit, *changesID, *buildType, !initialLocalEntries); err != nil {
		return fmt.Errorf("post cache-used: %w", err)
	}
	close(resps)

	dc.PrintStats()

	return nil
}

func runServer(listen string, dir string, authToken string, maxDiskBytes int64, preloadLimit int) error {
	store, err := local.NewStore(dir, true, local.WithMaxDiskBytes(maxDiskBytes))
	if err != nil {
		return fmt.Errorf("init local storage: %w", err)
	}
	defer store.Close()

	h := http.NewHandlerWithPreloadLimit(store, authToken, preloadLimit)

	go func() {
		for {
			time.Sleep(5 * time.Second)
			store.PrintStats()
		}
	}()

	network, addr := listenNetworkAndAddr(listen)
	if network == "unix" {
		if err := os.RemoveAll(addr); err != nil {
			return fmt.Errorf("remove old unix socket %s: %w", addr, err)
		}
		if err := os.MkdirAll(filepath.Dir(addr), 0o755); err != nil {
			return fmt.Errorf("create unix socket dir: %w", err)
		}
	}

	ln, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("listen %s %s: %w", network, addr, err)
	}
	if network == "unix" {
		defer os.Remove(addr)
	}
	defer ln.Close()

	server := &nethttp.Server{Handler: h}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(ln)
	}()

	log.Printf("Listening on %s://%s ...", network, addr)

	select {
	case sig := <-stop:
		log.Printf("Shutting down on %s ...", sig)
		if err := server.Close(); err != nil {
			log.Printf("server close: %s", err.Error())
		}
	case err := <-errCh:
		if err != nil && err != nethttp.ErrServerClosed {
			return fmt.Errorf("serve %s %s: %w", network, addr, err)
		}
	}

	return nil
}

func listenNetworkAndAddr(listen string) (network string, addr string) {
	if strings.HasPrefix(listen, "unix://") {
		return "unix", strings.TrimPrefix(listen, "unix://")
	}

	return "tcp", listen
}
