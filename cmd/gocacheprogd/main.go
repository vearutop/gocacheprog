package main

import (
	"flag"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/http"
	"github.com/vearutop/gocacheprogd/internal/local"
	"log"
	http2 "net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	var (
		listen = flag.String("listen", ":8080", "Listen address")
		dir    = flag.String("dir", "", "Cache store")
	)

	flag.Parse()

	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("user cache dir: %w", err)
		}
		d = filepath.Join(d, "gocacheprog")
		log.Printf("Defaulting to cache dir %s ...", d)
		*dir = d
	}

	if err := os.MkdirAll(*dir, 0755); err != nil {
		return fmt.Errorf("ensure cache dir: %w", err)
	}

	store, err := local.NewStore(*dir, true)
	if err != nil {
		return fmt.Errorf("init local storage: %w", err)
	}
	defer store.Close()

	h := http.NewHandler(store)

	go func() {
		for {
			time.Sleep(5 * time.Second)
			store.PrintStats()
		}
	}()

	// Channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		println("Shutting down ...")
		store.Close()

		os.Exit(0)
	}()

	log.Printf("Listening on %s ...", *listen)
	if err := http2.ListenAndServe(*listen, h); err != nil {
		if clErr := store.Close(); clErr != nil {
			log.Println(clErr.Error())
		}

		return fmt.Errorf("listen %s: %w", *listen, err)
	}

	return nil
}
