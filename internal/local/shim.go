package local

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vearutop/gocacheprog/internal/cacheprog"
)

type shimHello struct {
	SessionID string `json:"session_id,omitempty"`
	AuthToken string `json:"auth_token,omitempty"`
}

type shimEnvelope struct {
	Request cacheprog.Request `json:"request"`
	Body    []byte            `json:"body,omitempty"`
}

type shimPending struct {
	session    *shimSession
	originalID int64
}

type shimSession struct {
	sessionID string
	conn      net.Conn
	enc       *json.Encoder
	mu        sync.Mutex
}

func (s *shimSession) writeResponse(resp cacheprog.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.enc.Encode(resp)
}

type ShimServer struct {
	authToken string
	proxy     *Proxy
	resps     <-chan cacheprog.Response
	ready     <-chan struct{}

	nextID  int64
	waiters map[int64]shimPending
	mu      sync.Mutex
}

func NewShimServer(proxy *Proxy, resps <-chan cacheprog.Response, authToken string, ready <-chan struct{}) *ShimServer {
	s := &ShimServer{
		authToken: authToken,
		proxy:     proxy,
		resps:     resps,
		ready:     ready,
		waiters:   map[int64]shimPending{},
	}

	go s.dispatchResponses()

	return s
}

func (s *ShimServer) Serve(listen string, preloadErrCh <-chan error) error {
	if s.proxy != nil {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				s.proxy.PrintStats()
			}
		}()
	}

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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)

	errCh := make(chan error, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				errCh <- err
				return
			}

			go s.serveConn(conn)
		}
	}()

	s.proxy.logf("Listening on %s://%s ...", network, addr)

	for {
		select {
		case sig := <-stop:
			s.proxy.logf("Shutting down on %s ...", sig)
			return nil
		case err := <-preloadErrCh:
			if err != nil {
				return fmt.Errorf("daemon preload: %w", err)
			}
			preloadErrCh = nil
		case err := <-errCh:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("accept %s %s: %w", network, addr, err)
			}
			return nil
		}
	}
}

func (s *ShimServer) serveConn(conn net.Conn) {
	defer conn.Close()

	jd := json.NewDecoder(bufio.NewReader(conn))
	je := json.NewEncoder(conn)

	var hello shimHello
	if err := jd.Decode(&hello); err != nil {
		s.proxy.logf("decode shim hello: %s", err.Error())
		return
	}

	if s.authToken != "" && strings.TrimSpace(hello.AuthToken) != s.authToken {
		s.proxy.logf("reject shim session %q: invalid auth", hello.SessionID)
		return
	}

	session := &shimSession{
		sessionID: hello.SessionID,
		conn:      conn,
		enc:       je,
	}

	if s.ready != nil {
		select {
		case <-s.ready:
		case <-time.After(30 * time.Second):
			s.proxy.logf("shim session %q timed out waiting for daemon readiness", hello.SessionID)
			s.dropSessionWaiters(session)
			return
		}
	}

	for {
		var env shimEnvelope
		if err := jd.Decode(&env); err != nil {
			if err != io.EOF {
				s.proxy.logf("decode shim request: %s", err.Error())
			}
			s.dropSessionWaiters(session)
			return
		}

		switch env.Request.Command {
		case cacheprog.CmdGet:
			internalID := atomic.AddInt64(&s.nextID, 1)

			s.mu.Lock()
			s.waiters[internalID] = shimPending{
				session:    session,
				originalID: env.Request.ID,
			}
			s.mu.Unlock()

			req := env.Request
			req.ID = internalID
			s.proxy.Lookup(req)

		case cacheprog.CmdPut:
			req := env.Request
			req.ID = atomic.AddInt64(&s.nextID, 1)
			resp := s.proxy.Put(req, env.Body)
			resp.ID = env.Request.ID
			if err := session.writeResponse(resp); err != nil {
				s.proxy.logf("write shim put response: %s", err.Error())
				s.dropSessionWaiters(session)
				return
			}

		case cacheprog.CmdClose:
			s.dropSessionWaiters(session)
			return

		default:
			s.proxy.logf("unsupported shim command: %s", env.Request.Command)
			s.dropSessionWaiters(session)
			return
		}
	}
}

func (s *ShimServer) dropSessionWaiters(session *shimSession) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, pending := range s.waiters {
		if pending.session == session {
			delete(s.waiters, id)
		}
	}
}

func (s *ShimServer) dispatchResponses() {
	for resp := range s.resps {
		s.mu.Lock()
		pending, ok := s.waiters[resp.ID]
		if ok {
			delete(s.waiters, resp.ID)
		}
		s.mu.Unlock()

		if !ok {
			s.proxy.logf("orphan shim response id=%d", resp.ID)
			continue
		}

		resp.ID = pending.originalID
		if err := pending.session.writeResponse(resp); err != nil {
			s.proxy.logf("write shim get response: %s", err.Error())
		}
	}
}

type ShimClient struct {
	sessionID string
	authToken string
	conn      net.Conn
	enc       *json.Encoder
	jd        *json.Decoder
	resps     chan cacheprog.Response
	writeMu   sync.Mutex
}

func NewShimClient(remoteURL string, authToken string, sessionID string) (*ShimClient, error) {
	network, addr, err := shimNetworkAndAddr(remoteURL)
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(5 * time.Second)
	var conn net.Conn
	for {
		conn, err = net.DialTimeout(network, addr, 2*time.Second)
		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			return nil, err
		}

		time.Sleep(100 * time.Millisecond)
	}

	c := &ShimClient{
		sessionID: sessionID,
		authToken: authToken,
		conn:      conn,
		enc:       json.NewEncoder(conn),
		jd:        json.NewDecoder(bufio.NewReader(conn)),
		resps:     make(chan cacheprog.Response, 100),
	}

	if err := c.enc.Encode(shimHello{SessionID: sessionID, AuthToken: authToken}); err != nil {
		conn.Close()
		return nil, err
	}

	go c.readResponses()

	return c, nil
}

func (c *ShimClient) readResponses() {
	defer close(c.resps)

	for {
		var resp cacheprog.Response
		if err := c.jd.Decode(&resp); err != nil {
			return
		}

		c.resps <- resp
	}
}

func (c *ShimClient) Responses() <-chan cacheprog.Response {
	return c.resps
}

func (c *ShimClient) Do(req cacheprog.Request, body []byte) (cacheprog.Response, error) {
	if err := c.Send(req, body); err != nil {
		return cacheprog.Response{}, err
	}

	for resp := range c.resps {
		if resp.ID == req.ID {
			return resp, nil
		}
	}

	return cacheprog.Response{}, io.EOF
}

func (c *ShimClient) Send(req cacheprog.Request, body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.enc.Encode(shimEnvelope{
		Request: req,
		Body:    body,
	})
}

func (c *ShimClient) Close() error {
	return c.conn.Close()
}

func shimNetworkAndAddr(remoteURL string) (string, string, error) {
	if strings.HasPrefix(remoteURL, "unix://") {
		return "unix", strings.TrimPrefix(remoteURL, "unix://"), nil
	}

	if strings.Contains(remoteURL, "://") {
		u, err := url.Parse(remoteURL)
		if err != nil {
			return "", "", err
		}

		return "tcp", u.Host, nil
	}

	return "tcp", remoteURL, nil
}

func ProcessShimSession(in io.Reader, out io.Writer, logDump io.Writer, client *ShimClient) error {
	br := bufio.NewReader(in)
	jd := json.NewDecoder(br)
	je := json.NewEncoder(out)

	if err := je.Encode(&cacheprog.Response{KnownCommands: []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}}); err != nil {
		return fmt.Errorf("encode known commands: %w", err)
	}

	var mu sync.Mutex
	errCh := make(chan error, 1)
	var pending sync.WaitGroup

	go func() {
		for resp := range client.Responses() {
			if err := je.Encode(resp); err != nil {
				errCh <- fmt.Errorf("encode response: %w", err)
				return
			}
			dumpShimResponse(logDump, &mu, resp)
			pending.Done()
		}

		errCh <- nil
	}()

	for {
		var req cacheprog.Request
		if err := jd.Decode(&req); err != nil {
			_ = client.Close()
			return fmt.Errorf("decode request: %w", err)
		}

		dumpShimRequest(logDump, &mu, req)

		var body []byte
		if req.Command == cacheprog.CmdPut && req.BodySize > 0 {
			if err := jd.Decode(&body); err != nil {
				_ = client.Close()
				return fmt.Errorf("decode base64 cache body: %w", err)
			}
			if int64(len(body)) != req.BodySize {
				_ = client.Close()
				return fmt.Errorf("only got %d bytes of declared %d", len(body), req.BodySize)
			}
		}

		if req.Command == cacheprog.CmdClose {
			pending.Wait()
			_ = client.Close()
			return nil
		}

		if req.Command != cacheprog.CmdClose {
			pending.Add(1)
		}

		if err := client.Send(req, body); err != nil {
			if req.Command != cacheprog.CmdClose {
				pending.Done()
			}
			_ = client.Close()
			return err
		}

		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
			return io.EOF
		default:
		}
	}
}

func dumpShimRequest(logDump io.Writer, mu *sync.Mutex, req cacheprog.Request) {
	if logDump == nil {
		return
	}

	req.TS = time.Now().UTC().Unix()
	j, _ := json.Marshal(req)
	mu.Lock()
	_, _ = logDump.Write(append(j, '\n'))
	mu.Unlock()
}

func dumpShimResponse(logDump io.Writer, mu *sync.Mutex, resp cacheprog.Response) {
	if logDump == nil {
		return
	}

	resp.TS = time.Now().UTC().Unix()
	j, _ := json.Marshal(resp)
	mu.Lock()
	_, _ = logDump.Write(append(j, '\n'))
	mu.Unlock()
}
