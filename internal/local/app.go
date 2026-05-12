package local

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/vearutop/gocacheprog/internal/cacheprog"
)

type App struct {
	in      io.Reader
	out     io.Writer
	proxy   *Proxy
	resps   chan cacheprog.Response
	logDump io.Writer

	mu sync.Mutex
	jd *json.Decoder
	je *json.Encoder
}

func NewApp(in io.Reader, out io.Writer, proxy *Proxy, resps chan cacheprog.Response, logDump io.Writer) *App {
	return &App{
		in:      in,
		out:     out,
		proxy:   proxy,
		resps:   resps,
		logDump: logDump,
		jd:      json.NewDecoder(bufio.NewReader(in)),
		je:      json.NewEncoder(out),
	}
}

func (a *App) IterateResponses() {
	if err := a.je.Encode(&cacheprog.Response{KnownCommands: []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}}); err != nil {
		log.Printf("encode known commands: %s", err.Error())
		return
	}

	for res := range a.resps {
		if err := a.je.Encode(res); err != nil {
			log.Printf("encode response: %s", err.Error())
		}

		a.dumpResponse(res)
	}
}

func (a *App) IterateInput() error {
	for {
		var req cacheprog.Request
		if err := a.jd.Decode(&req); err != nil {
			return fmt.Errorf("decode request: %w", err)
		}

		a.dumpRequest(req)

		if req.Command == cacheprog.CmdClose {
			return nil
		}

		switch req.Command {
		case cacheprog.CmdGet:
			a.proxy.Lookup(req)
		case cacheprog.CmdPut:
			var body []byte

			if req.BodySize > 0 {
				if err := a.jd.Decode(&body); err != nil {
					return fmt.Errorf("decode base64 cache body: %w", err)
				}

				if int64(len(body)) != req.BodySize {
					return fmt.Errorf("only got %d bytes of declared %d", len(body), req.BodySize)
				}
			}

			a.resps <- a.proxy.Put(req, body)
		default:
			return fmt.Errorf("unsupported command: %s", req.Command)
		}
	}
}

func (a *App) dumpRequest(req cacheprog.Request) {
	if a.logDump == nil {
		return
	}

	req.TS = time.Now().UTC().Unix()
	j, _ := json.Marshal(req)

	a.mu.Lock()
	_, _ = a.logDump.Write(append(j, '\n'))
	a.mu.Unlock()
}

func (a *App) dumpResponse(res cacheprog.Response) {
	if a.logDump == nil {
		return
	}

	res.TS = time.Now().UTC().Unix()
	j, _ := json.Marshal(res)

	a.mu.Lock()
	_, _ = a.logDump.Write(append(j, '\n'))
	a.mu.Unlock()
}
