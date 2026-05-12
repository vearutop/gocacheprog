package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
	canonicalize := flag.String("canonicalize-timestamps", "", "canonicalize file and directory timestamps under this repo root and exit")
	params := local.ProxyParams{}

	flag.BoolVar(&params.Preload, "preload", false, "preload cache from remote server")
	flag.Int64Var(&params.PreloadSize, "preload-size", 1000000, "preload cache from remote server fo items up to this size")
	flag.StringVar(&params.Commit, "commit", "", "current commit SHA used to upload cache usage manifest")
	flag.StringVar(&params.ChangesID, "changes-id", "", "stable change stream label used to upload and preload latest cache usage manifest")
	flag.StringVar(&params.BuildType, "build-type", "", "optional build type label to isolate cache manifests, e.g. unit or race")
	flag.StringVar(&params.BaseCommit, "base-commit", "", "base commit SHA used to scope preload")
	flag.StringVar(&params.ParentCommit, "parent-commit", "", "parent commit SHA used to scope preload")

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
		store, err := local.NewStore(*dir, local.WithCompression(), local.WithMaxDiskBytes(*maxDiskBytes))
		if err != nil {
			return fmt.Errorf("init local storage: %w", err)
		}
		defer store.Close()

		return runServer(*listen, store, *authToken, *preloadLimit)
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

	dc, err := local.NewProxy(store, upstream, resps, params)
	if err != nil {
		return fmt.Errorf("new cache: %w", err)
	}

	dc.Verbose = true
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
