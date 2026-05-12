package local

import (
	"bufio"
	"encoding/json"
	"errors"
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

var errBodySizeMismatch = errors.New("cache body size mismatch")

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
	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	if err := a.je.Encode(&cacheprog.Response{KnownCommands: []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}}); err != nil {
		log.Printf("encode known commands: %s", err.Error())
		return
	}

	for res := range a.resps {
		//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
		if err := a.je.Encode(res); err != nil {
			log.Printf("encode response: %s", err.Error())
		}

		a.dumpResponse(res)
	}
}

func (a *App) IterateInput() error {
	for {
		var req cacheprog.Request
		//nolint:musttag // cacheprog.Request is defined by the Go cache protocol.
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
					return fmt.Errorf("%w: got %d bytes of declared %d", errBodySizeMismatch, len(body), req.BodySize)
				}
			}

			a.resps <- a.proxy.Put(req, body)
		default:
			return fmt.Errorf("unsupported command %q", req.Command)
		}
	}
}

func (a *App) dumpRequest(req cacheprog.Request) {
	if a.logDump == nil {
		return
	}

	req.TS = time.Now().UTC().Unix()
	//nolint:musttag // cacheprog.Request is defined by the Go cache protocol.
	j, err := json.Marshal(req)
	if err != nil {
		log.Printf("marshal request dump: %s", err.Error())
		return
	}

	a.dumpLogLine("request", j)
}

func (a *App) dumpResponse(res cacheprog.Response) {
	if a.logDump == nil {
		return
	}

	res.TS = time.Now().UTC().Unix()
	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	j, err := json.Marshal(res)
	if err != nil {
		log.Printf("marshal response dump: %s", err.Error())
		return
	}

	a.dumpLogLine("response", j)
}

func (a *App) dumpLogLine(kind string, payload []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, err := a.logDump.Write(append(payload, '\n')); err != nil {
		log.Printf("write %s dump: %s", kind, err.Error())
	}
}
