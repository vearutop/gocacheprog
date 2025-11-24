package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/bool64/progress"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
	"github.com/vearutop/gocacheprogd/internal/disk"
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

	var mu sync.Mutex
	var logDump io.Writer

	if *dumpLogs != "" {
		f, err := os.Create(*dumpLogs)
		if err != nil {
			return fmt.Errorf("create dump logs file: %w", err)
		}

		logDump = f

		defer f.Close()
	}

	resps := make(chan cacheprog.Response, 100)

	dc, err := disk.NewCache(*dir, resps)
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

	//pr := &progress.Progress{
	//	Interval: 5 * time.Second,
	//	Print: func(status progress.Status) {
	//		println(progress.DefaultStatus(status))
	//	},
	//}
	//
	//pr.AddMetrics()
	//pr.Start()

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

			diskPath, err := dc.Put(req.ActionID, req.OutputID, req.BodySize, body)
			if err != nil {
				return fmt.Errorf("put: %w", err)
			}

			res := cacheprog.Response{ID: req.ID}
			res.DiskPath = diskPath
			res.OutputID = req.OutputID
			res.Size = req.BodySize

			now := time.Now().UTC()
			res.Time = &now

			resps <- res
		}
	}

	if err := dc.Close(); err != nil {
		return fmt.Errorf("close cache: %w", err)
	}
	close(resps)

	return nil
}
