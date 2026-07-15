package local

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	nethttp "net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vearutop/gocacheprog/internal/gocache"
	cachehttp "github.com/vearutop/gocacheprog/internal/http"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

func RunServer(httpListen, httpsListen, httpsHost, certCacheDir string, store *Store, nativeStore *gocache.Store, authToken string, preloadLimit int) error {
	h := cachehttp.NewHandlerWithPreloadLimit(store, nativeStore, authToken, preloadLimit)
	if httpsHost == "" {
		return serveHTTP(httpListen, h, store.PrintStats)
	}

	return serveHTTPAndHTTPS(httpListen, httpsListen, httpsHost, certCacheDir, h, store.PrintStats)
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

func serveHTTPAndHTTPS(httpListen, httpsListen, httpsHost, certCacheDir string, h nethttp.Handler, printStats func()) error {
	if printStats != nil {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				printStats()
			}
		}()
	}

	manager := &autocert.Manager{
		Cache:      autocert.DirCache(certCacheDir),
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(httpsHost),
	}

	httpServer, httpLn, httpLogAddr, httpCleanup, err := prepareServer(httpListen, autocertHTTPHandler(httpsHost, manager), false)
	if err != nil {
		return err
	}
	defer httpCleanup()

	httpsServer, httpsLn, httpsLogAddr, httpsCleanup, err := prepareServer(httpsListen, h, true)
	if err != nil {
		return err
	}
	defer httpsCleanup()

	httpsServer.TLSConfig = manager.TLSConfig()
	httpsServer.TLSConfig.MinVersion = tls.VersionTLS12
	if httpsServer.TLSConfig.NextProtos == nil {
		httpsServer.TLSConfig.NextProtos = []string{acme.ALPNProto, "h2", "http/1.1"}
	}

	return runServers(
		[]serverRunner{
			{name: "http", addr: httpLogAddr, serve: func() error { return httpServer.Serve(httpLn) }, close: func() error { return httpServer.Close() }},
			{name: "https", addr: httpsLogAddr, serve: func() error { return httpsServer.Serve(tls.NewListener(httpsLn, httpsServer.TLSConfig)) }, close: func() error { return httpsServer.Close() }},
		},
		func() {
			if err := warmAutocertCertificate(manager, httpsHost); err != nil {
				log.Printf("warm autocert certificate for %s: %s", httpsHost, err.Error())
			}
		},
	)
}

func warmAutocertCertificate(manager *autocert.Manager, httpsHost string) error {
	cert, err := manager.GetCertificate(&tls.ClientHelloInfo{
		ServerName: httpsHost,
		SupportedProtos: []string{
			"h2",
			"http/1.1",
		},
		SignatureSchemes: []tls.SignatureScheme{
			tls.ECDSAWithP256AndSHA256,
			tls.PSSWithSHA256,
		},
		SupportedCurves: []tls.CurveID{
			tls.CurveP256,
		},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
	})
	if err != nil {
		return err
	}
	if cert.Leaf != nil {
		log.Printf("warmed autocert certificate for %s, expires at %s", httpsHost, cert.Leaf.NotAfter.Format(time.RFC3339))
		return nil
	}
	log.Printf("warmed autocert certificate for %s", httpsHost)
	return nil
}

func autocertHTTPHandler(httpsHost string, manager *autocert.Manager) nethttp.Handler {
	return manager.HTTPHandler(nethttp.HandlerFunc(func(rw nethttp.ResponseWriter, r *nethttp.Request) {
		// Built from struct fields, not string concatenation of the raw request URI, so
		// request-controlled input can only ever populate Path/RawQuery, never Scheme/Host.
		target := url.URL{
			Scheme:   "https",
			Host:     httpsHost,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
		}
		nethttp.Redirect(rw, r, target.String(), nethttp.StatusMovedPermanently)
	}))
}

type serverRunner struct {
	name  string
	addr  string
	serve func() error
	close func() error
}

func runServers(servers []serverRunner, onStarted func()) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)

	errCh := make(chan error, len(servers))
	for _, srv := range servers {
		srv := srv
		go func() {
			errCh <- srv.serve()
		}()
		log.Printf("Listening on %s://%s ...", srv.name, srv.addr)
	}
	if onStarted != nil {
		onStarted()
	}

	select {
	case sig := <-stop:
		log.Printf("Shutting down on %s ...", sig)
		for _, srv := range servers {
			if err := srv.close(); err != nil {
				log.Printf("%s server close: %s", srv.name, err.Error())
			}
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			for _, srv := range servers {
				if closeErr := srv.close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) && !errors.Is(closeErr, nethttp.ErrServerClosed) {
					log.Printf("%s server close after error: %s", srv.name, closeErr.Error())
				}
			}
			return err
		}
	}

	return nil
}

func prepareServer(listen string, h nethttp.Handler, tlsEnabled bool) (*nethttp.Server, net.Listener, string, func(), error) {
	network, addr := listenNetworkAndAddr(listen)
	if network == "unix" {
		if err := os.RemoveAll(addr); err != nil {
			return nil, nil, "", nil, fmt.Errorf("remove old unix socket %s: %w", addr, err)
		}
		if err := os.MkdirAll(filepath.Dir(addr), 0o750); err != nil {
			return nil, nil, "", nil, fmt.Errorf("create unix socket dir: %w", err)
		}
	}

	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("listen %s %s: %w", network, addr, err)
	}

	cleanup := func() {
		if network == "unix" {
			if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
				log.Printf("remove unix socket %s: %s", addr, err.Error())
			}
		}
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close listener: %s", err.Error())
		}
	}

	server := &nethttp.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}

	if tlsEnabled {
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	return server, ln, addr, cleanup, nil
}

func listenNetworkAndAddr(listen string) (network string, addr string) {
	if strings.HasPrefix(listen, "unix://") {
		return "unix", strings.TrimPrefix(listen, "unix://")
	}

	return "tcp", listen
}
