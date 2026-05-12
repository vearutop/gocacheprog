package local

import (
	"errors"
	"fmt"
	"log"
	"net"
	nethttp "net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	cachehttp "github.com/vearutop/gocacheprog/internal/http"
)

func RunServer(listen string, store *Store, authToken string, preloadLimit int) error {
	h := cachehttp.NewHandlerWithPreloadLimit(store, authToken, preloadLimit)
	return serveHTTP(listen, h, store.PrintStats)
}

func RunProxyServer(listen string, h nethttp.Handler, printStats func()) error {
	return serveHTTP(listen, h, printStats)
}

func serveHTTP(listen string, h nethttp.Handler, printStats func()) error {
	if printStats != nil {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				printStats()
			}
		}()
	}

	network, addr := listenNetworkAndAddr(listen)
	if network == "unix" {
		if err := os.RemoveAll(addr); err != nil {
			return fmt.Errorf("remove old unix socket %s: %w", addr, err)
		}
		if err := os.MkdirAll(filepath.Dir(addr), 0o750); err != nil {
			return fmt.Errorf("create unix socket dir: %w", err)
		}
	}

	ln, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("listen %s %s: %w", network, addr, err)
	}
	if network == "unix" {
		defer func() {
			if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
				log.Printf("remove unix socket %s: %s", addr, err.Error())
			}
		}()
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close listener: %s", err.Error())
		}
	}()

	server := &nethttp.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
	}

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
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
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
