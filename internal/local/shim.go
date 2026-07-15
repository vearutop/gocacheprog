package local

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vearutop/gocacheprog/internal/cacheprog"
)

var ErrStopRequested = errors.New("shim stop requested")

type shimHello struct {
	SessionID         string `json:"session_id,omitempty"`
	AuthToken         string `json:"auth_token,omitempty"`
	Stop              bool   `json:"stop,omitempty"`
	StartedAtUnixNano int64  `json:"started_at_unix_nano,omitempty"`
}

type shimStopRequest struct {
	replyCh chan ShimStopResponse
	doneCh  chan struct{}
}

type ShimStopResponse struct {
	Lines []string `json:"lines,omitempty"`
	Err   string   `json:"err,omitempty"`
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
	sessionID      string
	conn           net.Conn
	enc            *json.Encoder
	startedAt      time.Time
	firstGetLogged atomic.Bool
	mu             sync.Mutex
}

func (s *shimSession) writeResponse(resp cacheprog.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	return s.enc.Encode(resp)
}

type ShimServer struct {
	authToken string
	proxy     *Proxy
	resps     <-chan cacheprog.Response
	ready     <-chan struct{}
	readyErr  func() error

	nextID  int64
	waiters map[int64]shimPending
	mu      sync.Mutex

	connMu          sync.Mutex
	conns           map[net.Conn]struct{}
	connWg          sync.WaitGroup
	shutdownOnce    sync.Once
	stopReqCh       chan shimStopRequest
	stopReplyMu     sync.Mutex
	stopReplyCh     chan ShimStopResponse
	stopReplyDoneCh chan struct{}
	sessionsSeen    int64
	activeSessions  int64
	firstGetsServed int64
	firstGetTotalNs int64
	firstGetMaxNs   int64
}

func NewShimServer(proxy *Proxy, resps <-chan cacheprog.Response, authToken string, ready <-chan struct{}, readyErr func() error) *ShimServer {
	s := &ShimServer{
		authToken: authToken,
		proxy:     proxy,
		resps:     resps,
		ready:     ready,
		readyErr:  readyErr,
		waiters:   map[int64]shimPending{},
		conns:     map[net.Conn]struct{}{},
		stopReqCh: make(chan shimStopRequest, 1),
	}

	go s.dispatchResponses()

	return s
}

//nolint:gocyclo // shim server startup/shutdown coordination is intentionally centralized here.
func (s *ShimServer) Serve(listen string, preloadErrCh <-chan error) error {
	if s.proxy != nil {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				s.PrintStats()
			}
		}()
	}

	network, addr := listenNetworkAndAddr(listen)
	ln, err := prepareShimListener(network, addr)
	if err != nil {
		return err
	}
	if network == "unix" {
		defer func() {
			if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
				s.proxy.logf("remove unix socket %s: %s", addr, err.Error())
			}
		}()
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.proxy.logf("close shim listener: %s", err.Error())
		}
	}()

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

			s.startConn(conn)
		}
	}()

	s.proxy.logf("Listening on %s://%s ...", network, addr)

	ready := s.ready
	for {
		select {
		case sig := <-stop:
			s.proxy.logf("Shutting down on %s ...", sig)
			s.shutdown(ln)
			return nil
		case req := <-s.stopReqCh:
			s.setStopReply(req.replyCh, req.doneCh)
			s.proxy.logf("Shutting down on remote stop request ...")
			s.shutdown(ln)
			return ErrStopRequested
		case <-ready:
			if s.readyErr != nil {
				if err := s.readyErr(); err != nil {
					return fmt.Errorf("daemon preload: %w", err)
				}
			}
			ready = nil
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

func (s *ShimServer) shutdown(ln net.Listener) {
	s.shutdownOnce.Do(func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.proxy.logf("close shim listener during shutdown: %s", err.Error())
		}

		s.connMu.Lock()
		conns := make([]net.Conn, 0, len(s.conns))
		for conn := range s.conns {
			conns = append(conns, conn)
		}
		s.connMu.Unlock()

		for _, conn := range conns {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.proxy.logf("close shim conn during shutdown: %s", err.Error())
			}
		}

		s.connWg.Wait()
	})
}

func (s *ShimServer) startConn(conn net.Conn) {
	s.connWg.Add(1)
	go s.serveConn(conn)
}

func (s *ShimServer) Stats() map[string]string {
	firstGetsServed := atomic.LoadInt64(&s.firstGetsServed)
	firstGetAvg := "0s"
	if firstGetsServed > 0 {
		firstGetAvg = (time.Duration(atomic.LoadInt64(&s.firstGetTotalNs)) / time.Duration(firstGetsServed)).String()
	}

	return map[string]string{
		"sessions_seen":    strconv.FormatInt(atomic.LoadInt64(&s.sessionsSeen), 10),
		"active_sessions":  strconv.FormatInt(atomic.LoadInt64(&s.activeSessions), 10),
		"first_get_served": strconv.FormatInt(firstGetsServed, 10),
		"first_get_avg":    firstGetAvg,
		"first_get_max":    time.Duration(atomic.LoadInt64(&s.firstGetMaxNs)).String(),
	}
}

func (s *ShimServer) PrintStats() {
	if s.proxy != nil {
		s.proxy.PrintStats()
	}

	st := s.Stats()
	var sb strings.Builder
	for _, k := range slices.Sorted(maps.Keys(st)) {
		fmt.Fprintf(&sb, " %s: %s", k, st[k])
	}

	if s.proxy != nil {
		s.proxy.logf("shim:%s", sb.String())
	}
}

func (s *ShimServer) setStopReply(replyCh chan ShimStopResponse, doneCh chan struct{}) {
	s.stopReplyMu.Lock()
	defer s.stopReplyMu.Unlock()
	s.stopReplyCh = replyCh
	s.stopReplyDoneCh = doneCh
}

func (s *ShimServer) ReplyStop(resp ShimStopResponse) {
	s.stopReplyMu.Lock()
	replyCh := s.stopReplyCh
	doneCh := s.stopReplyDoneCh
	s.stopReplyCh = nil
	s.stopReplyDoneCh = nil
	s.stopReplyMu.Unlock()

	if replyCh == nil {
		return
	}

	replyCh <- resp
	close(replyCh)
	if doneCh != nil {
		<-doneCh
	}
}

func prepareShimListener(network string, addr string) (net.Listener, error) {
	if network == "unix" {
		if err := os.RemoveAll(addr); err != nil {
			return nil, fmt.Errorf("remove old unix socket %s: %w", addr, err)
		}
		if err := os.MkdirAll(filepath.Dir(addr), 0o750); err != nil {
			return nil, fmt.Errorf("create unix socket dir: %w", err)
		}
	}

	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s %s: %w", network, addr, err)
	}

	return ln, nil
}

//nolint:gocyclo // shim session protocol handling is intentionally centralized.
func (s *ShimServer) serveConn(conn net.Conn) {
	s.connMu.Lock()
	s.conns[conn] = struct{}{}
	s.connMu.Unlock()
	tracked := true

	defer func() {
		if tracked {
			s.connMu.Lock()
			delete(s.conns, conn)
			s.connMu.Unlock()
			s.connWg.Done()
		}

		if err := conn.Close(); err != nil {
			s.proxy.logf("close shim conn: %s", err.Error())
		}
	}()

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

	if hello.Stop {
		s.connMu.Lock()
		delete(s.conns, conn)
		s.connMu.Unlock()
		s.connWg.Done()
		tracked = false

		replyCh := make(chan ShimStopResponse, 1)
		doneCh := make(chan struct{})
		select {
		case s.stopReqCh <- shimStopRequest{replyCh: replyCh, doneCh: doneCh}:
		default:
			replyCh <- ShimStopResponse{Err: "stop already in progress"}
			close(replyCh)
			close(doneCh)
		}

		resp, ok := <-replyCh
		if !ok {
			return
		}

		if err := je.Encode(resp); err != nil {
			s.proxy.logf("write shim stop response: %s", err.Error())
		}
		close(doneCh)
		return
	}

	session := &shimSession{
		sessionID: hello.SessionID,
		conn:      conn,
		enc:       je,
		startedAt: time.Now(),
	}
	if hello.StartedAtUnixNano > 0 {
		session.startedAt = time.Unix(0, hello.StartedAtUnixNano)
	}
	atomic.AddInt64(&s.sessionsSeen, 1)
	atomic.AddInt64(&s.activeSessions, 1)
	defer atomic.AddInt64(&s.activeSessions, -1)

	if s.ready != nil {
		select {
		case <-s.ready:
			if s.readyErr != nil {
				if err := s.readyErr(); err != nil {
					s.proxy.logf("shim session %q blocked by daemon preload failure: %s", hello.SessionID, err.Error())
					s.dropSessionWaiters(session)
					return
				}
			}
		case <-time.After(30 * time.Second):
			s.proxy.logf("shim session %q timed out waiting for daemon readiness", hello.SessionID)
			s.dropSessionWaiters(session)
			return
		}
	}

	for {
		var env shimEnvelope
		//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
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

		if pending.session.firstGetLogged.CompareAndSwap(false, true) {
			latency := time.Since(pending.session.startedAt)
			atomic.AddInt64(&s.firstGetsServed, 1)
			atomic.AddInt64(&s.firstGetTotalNs, latency.Nanoseconds())
			for {
				prev := atomic.LoadInt64(&s.firstGetMaxNs)
				if latency.Nanoseconds() <= prev {
					break
				}
				if atomic.CompareAndSwapInt64(&s.firstGetMaxNs, prev, latency.Nanoseconds()) {
					break
				}
			}
			s.proxy.logf("shim session %q first_get_latency=%s", pending.session.sessionID, latency.String())
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
	startedAt time.Time
	conn      net.Conn
	enc       *json.Encoder
	jd        *json.Decoder
	resps     chan cacheprog.Response
	writeMu   sync.Mutex
	waitersMu sync.Mutex
	waiters   map[int64]chan cacheprog.Response
}

func NewShimClient(remoteURL string, authToken string, sessionID string) (*ShimClient, error) {
	startedAt := time.Now()
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
		startedAt: startedAt,
		conn:      conn,
		enc:       json.NewEncoder(conn),
		jd:        json.NewDecoder(bufio.NewReader(conn)),
		resps:     make(chan cacheprog.Response, 100),
		waiters:   make(map[int64]chan cacheprog.Response),
	}

	//nolint:gosec // local shim auth token is an expected protocol field.
	if err := c.enc.Encode(shimHello{
		SessionID:         sessionID,
		AuthToken:         authToken,
		StartedAtUnixNano: startedAt.UnixNano(),
	}); err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			return nil, fmt.Errorf("close shim conn after hello failure: %w", closeErr)
		}
		return nil, err
	}

	go c.readResponses()

	return c, nil
}

func (c *ShimClient) readResponses() {
	defer func() {
		c.waitersMu.Lock()
		for id, ch := range c.waiters {
			close(ch)
			delete(c.waiters, id)
		}
		c.waitersMu.Unlock()
		close(c.resps)
	}()

	for {
		var resp cacheprog.Response
		//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
		if err := c.jd.Decode(&resp); err != nil {
			return
		}

		c.waitersMu.Lock()
		waiter, ok := c.waiters[resp.ID]
		if ok {
			delete(c.waiters, resp.ID)
		}
		c.waitersMu.Unlock()
		if ok {
			waiter <- resp
			close(waiter)
			continue
		}

		c.resps <- resp
	}
}

func (c *ShimClient) Responses() <-chan cacheprog.Response {
	return c.resps
}

func (c *ShimClient) Do(req cacheprog.Request, body []byte) (cacheprog.Response, error) {
	respCh := make(chan cacheprog.Response, 1)
	c.waitersMu.Lock()
	c.waiters[req.ID] = respCh
	c.waitersMu.Unlock()

	if err := c.Send(req, body); err != nil {
		c.waitersMu.Lock()
		delete(c.waiters, req.ID)
		c.waitersMu.Unlock()
		return cacheprog.Response{}, err
	}

	resp, ok := <-respCh
	if !ok {
		return cacheprog.Response{}, io.EOF
	}

	return resp, nil
}

func (c *ShimClient) Send(req cacheprog.Request, body []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	//nolint:musttag // shimEnvelope is the streaming shim protocol payload.
	return c.enc.Encode(shimEnvelope{
		Request: req,
		Body:    body,
	})
}

func (c *ShimClient) Close() error {
	return c.conn.Close()
}

func StopShimServer(remoteURL string, authToken string) ([]string, error) {
	network, addr, err := shimNetworkAndAddr(remoteURL)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout(network, addr, 2*time.Second) //nolint:gosec // addr is the operator-supplied -stop target, a local daemon address, not attacker-controlled remote input.
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			log.Printf("close stop shim conn: %s", closeErr.Error())
		}
	}()

	enc := json.NewEncoder(conn)
	//nolint:gosec // local shim auth token is an expected protocol field.
	if err := enc.Encode(shimHello{AuthToken: authToken, Stop: true}); err != nil {
		return nil, err
	}

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, err
	}

	var resp ShimStopResponse
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, err
	}

	if resp.Err != "" {
		return resp.Lines, errors.New(resp.Err)
	}

	return resp.Lines, nil
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

//nolint:gocyclo // the stdio session loop is inherently stateful and clearer inline.
func ProcessShimSession(in io.Reader, out io.Writer, logDump io.Writer, client *ShimClient) error {
	br := bufio.NewReader(in)
	jd := json.NewDecoder(br)
	je := json.NewEncoder(out)

	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	if err := je.Encode(&cacheprog.Response{KnownCommands: []cacheprog.Cmd{cacheprog.CmdPut, cacheprog.CmdGet, cacheprog.CmdClose}}); err != nil {
		return fmt.Errorf("encode known commands: %w", err)
	}

	var mu sync.Mutex
	errCh := make(chan error, 1)
	var pending sync.WaitGroup
	var inFlight atomic.Int64
	var firstGetLogged atomic.Bool

	go func() {
		for resp := range client.Responses() {
			if firstGetLogged.CompareAndSwap(false, true) {
				log.Printf("shim first_get_latency=%s", time.Since(client.startedAt).String())
			}
			//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
			if err := je.Encode(resp); err != nil {
				errCh <- fmt.Errorf("encode response: %w", err)
				return
			}
			dumpShimResponse(logDump, &mu, resp)
			inFlight.Add(-1)
			pending.Done()
		}

		if inFlight.Load() > 0 {
			errCh <- io.ErrUnexpectedEOF
			return
		}

		errCh <- nil
	}()

	for {
		req, err := decodeShimRequest(jd)
		if err != nil {
			if closeErr := client.Close(); closeErr != nil {
				return errors.Join(fmt.Errorf("decode request: %w", err), fmt.Errorf("close client: %w", closeErr))
			}
			return fmt.Errorf("decode request: %w", err)
		}

		dumpShimRequest(logDump, &mu, req)

		body, err := decodeShimBody(jd, req)
		if err != nil {
			if closeErr := client.Close(); closeErr != nil {
				return errors.Join(err, fmt.Errorf("close client: %w", closeErr))
			}
			return err
		}

		if req.Command == cacheprog.CmdClose {
			waitDone := make(chan struct{})
			go func() {
				pending.Wait()
				close(waitDone)
			}()
			select {
			case <-waitDone:
			case err := <-errCh:
				if err == nil {
					break
				}
				if closeErr := client.Close(); closeErr != nil {
					return errors.Join(fmt.Errorf("wait for pending shim responses: %w", err), fmt.Errorf("close shim client: %w", closeErr))
				}
				return fmt.Errorf("wait for pending shim responses: %w", err)
			}
			if err := client.Close(); err != nil {
				return fmt.Errorf("close shim client: %w", err)
			}
			return nil
		}

		if req.Command != cacheprog.CmdClose {
			inFlight.Add(1)
			pending.Add(1)
		}

		if err := client.Send(req, body); err != nil {
			if req.Command != cacheprog.CmdClose {
				inFlight.Add(-1)
				pending.Done()
			}
			if closeErr := client.Close(); closeErr != nil {
				return errors.Join(err, fmt.Errorf("close client: %w", closeErr))
			}
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

func decodeShimRequest(jd *json.Decoder) (cacheprog.Request, error) {
	var req cacheprog.Request
	//nolint:musttag // cacheprog.Request is defined by the Go cache protocol.
	if err := jd.Decode(&req); err != nil {
		return cacheprog.Request{}, err
	}

	return req, nil
}

func decodeShimBody(jd *json.Decoder, req cacheprog.Request) ([]byte, error) {
	if req.Command != cacheprog.CmdPut || req.BodySize <= 0 {
		return nil, nil
	}

	var body []byte
	if err := jd.Decode(&body); err != nil {
		return nil, fmt.Errorf("decode base64 cache body: %w", err)
	}
	if int64(len(body)) != req.BodySize {
		return nil, fmt.Errorf("only got %d bytes of declared %d", len(body), req.BodySize)
	}

	return body, nil
}

func dumpShimRequest(logDump io.Writer, mu *sync.Mutex, req cacheprog.Request) {
	if logDump == nil {
		return
	}

	req.TS = time.Now().UTC().Unix()
	//nolint:musttag // cacheprog.Request is defined by the Go cache protocol.
	j, err := json.Marshal(req)
	if err != nil {
		return
	}
	mu.Lock()
	if _, err := logDump.Write(append(j, '\n')); err != nil {
		mu.Unlock()
		return
	}
	mu.Unlock()
}

func dumpShimResponse(logDump io.Writer, mu *sync.Mutex, resp cacheprog.Response) {
	if logDump == nil {
		return
	}

	resp.TS = time.Now().UTC().Unix()
	//nolint:musttag // cacheprog.Response is defined by the Go cache protocol.
	j, err := json.Marshal(resp)
	if err != nil {
		return
	}
	mu.Lock()
	if _, err := logDump.Write(append(j, '\n')); err != nil {
		mu.Unlock()
		return
	}
	mu.Unlock()
}
