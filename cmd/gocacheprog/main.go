package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"github.com/vearutop/gocacheprogd/internal/http"
	"github.com/vearutop/gocacheprogd/internal/local"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	dir := flag.String("cache-dir", "", "cache directory; empty means automatic")
	dumpLogs := flag.String("dump-log", "", "dump req/resp logs to file")
	remoteURL := flag.String("remote-url", "", "remote HTTP server cache source, e.g. https://example.com:8080")

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

	println("starting at dir", *dir)

	var (
		mu       sync.Mutex
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
		upstream, err = http.NewClient(*remoteURL)
		if err != nil {
			return fmt.Errorf("remote client: %w", err)
		}
	}

	dc, err := local.NewProxy(*dir, upstream, resps)
	if err != nil {
		return fmt.Errorf("new cache: %w", err)
	}

	dc.Verbose = true

	br := bufio.NewReader(os.Stdin)
	jd := json.NewDecoder(br)

	je := json.NewEncoder(os.Stdout)

	if err := je.Encode(&cacheprog.Response{KnownCommands: []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}}); err != nil {
		return fmt.Errorf("encode known commands: %w", err)
	}

	go func() {
		for {
			time.Sleep(5 * time.Second)

			dc.PrintStats()
		}
	}()

	go func() {
		for res := range resps {
			if err := je.Encode(res); err != nil {
				println(err.Error())
			}

			if logDump != nil {
				res.TS = time.Now().UTC().Unix()

				j, _ := json.Marshal(res)

				mu.Lock()
				logDump.Write(append(j, '\n'))
				mu.Unlock()
			}
		}
	}()

	for {
		var req cacheprog.Request
		if err := jd.Decode(&req); err != nil {
			return fmt.Errorf("decode request: %w", err)
		}

		if logDump != nil {
			req.TS = time.Now().UTC().Unix()

			j, _ := json.Marshal(req)

			mu.Lock()
			logDump.Write(append(j, '\n'))
			mu.Unlock()
		}

		if req.Command == cacheprog.CmdClose {
			break
		}

		if req.Command == cacheprog.CmdGet {
			dc.Lookup(req)
			continue
		}

		if req.Command == cacheprog.CmdPut {
			var body []byte

			if req.BodySize > 0 {
				if err := jd.Decode(&body); err != nil {
					return fmt.Errorf("decode base64 cache body: %w", err)
				}

				if int64(len(body)) != req.BodySize {
					return fmt.Errorf("only got %d bytes of declared %d", len(body), req.BodySize)
				}
			}

			resps <- dc.Put(req, body)
		}
	}

	if err := dc.Close(); err != nil {
		return fmt.Errorf("close cache: %w", err)
	}
	close(resps)

	dc.PrintStats()

	return nil
}
