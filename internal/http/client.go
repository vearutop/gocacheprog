package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/dynhist-go"
	"github.com/vearutop/gocacheprogd/internal/cache"
)

var gatewayRetryDelay = 5 * time.Second

type SessionInfo struct {
	SessionID string
	StartedAt time.Time
	PID       int
	CacheDir  string
	Params    SessionParams
}

type SessionParams interface {
	SessionCommit() string
	SessionParentCommit() string
	SessionChangesID() string
	SessionBuildType() string
	SessionBaseCommit() string
}

type Client struct {
	baseURL   string
	authToken string

	tr *http.Transport

	latencyGet *dynhist.Collector
	latencyPut *dynhist.Collector

	bytesRead    int64
	bytesWritten int64
	preloadBytes int64
	preloadItems int64
	getCnt       int64
	getReqCnt    int64
	putCnt       int64

	mu                 sync.Mutex
	lastPreloadSources string
}

func NewClient(baseURL string, authToken string) (*Client, error) {
	return NewClientWithSession(baseURL, authToken, nil)
}

func NewClientWithSession(baseURL string, authToken string, sessionInfo *SessionInfo) (*Client, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")

	req, err := http.NewRequest(http.MethodGet, baseURL+"/version", nil)
	if err != nil {
		return nil, err
	}
	setAuthHeader(req, authToken)
	setSessionHeaders(req, sessionInfo)

	client := &Client{baseURL: baseURL, authToken: authToken}
	client.latencyGet = &dynhist.Collector{WeightFunc: dynhist.LatencyWidth, BucketsLimit: 50}
	client.latencyPut = &dynhist.Collector{WeightFunc: dynhist.LatencyWidth, BucketsLimit: 50}

	client.tr = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	d := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	client.tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return c, err
		}

		return &countingConn{
			c:    client,
			Conn: c,
		}, nil
	}

	resp, err := client.roundTrip(req, "version")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := checkStatus(resp, http.StatusOK, "version"); err != nil {
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, fmt.Errorf("authentication failed: -auth-token <value> is missing or incorrect")
		}
		return nil, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(string(b), "gocacheprogd ") {
		return nil, fmt.Errorf("unexpected version: %s", string(b))
	}

	return client, nil
}

func setSessionHeaders(req *http.Request, sessionInfo *SessionInfo) {
	if sessionInfo == nil {
		return
	}

	if sessionInfo.SessionID != "" {
		req.Header.Set("X-GoCacheProg-Session-Id", sessionInfo.SessionID)
	}
	if !sessionInfo.StartedAt.IsZero() {
		req.Header.Set("X-GoCacheProg-Started-At", sessionInfo.StartedAt.UTC().Format(time.RFC3339Nano))
	}
	if sessionInfo.PID != 0 {
		req.Header.Set("X-GoCacheProg-Pid", strconv.Itoa(sessionInfo.PID))
	}
	if sessionInfo.CacheDir != "" {
		req.Header.Set("X-GoCacheProg-Cache-Dir", sessionInfo.CacheDir)
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionCommit() != "" {
		req.Header.Set("X-GoCacheProg-Commit", sessionInfo.Params.SessionCommit())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionParentCommit() != "" {
		req.Header.Set("X-GoCacheProg-Parent", sessionInfo.Params.SessionParentCommit())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionChangesID() != "" {
		req.Header.Set("X-GoCacheProg-Changes", sessionInfo.Params.SessionChangesID())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionBuildType() != "" {
		req.Header.Set("X-GoCacheProg-Build-Type", sessionInfo.Params.SessionBuildType())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionBaseCommit() != "" {
		req.Header.Set("X-GoCacheProg-Base", sessionInfo.Params.SessionBaseCommit())
	}
}

type countingConn struct {
	net.Conn
	c *Client
}

// Read reads data from the connection.
// Read can be made to time out and return an Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetReadDeadline.
func (c *countingConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	atomic.AddInt64(&c.c.bytesRead, int64(n))

	return n, err
}

// Write writes data to the connection.
// Write can be made to time out and return an Error with Timeout() == true
// after a fixed time limit; see SetDeadline and SetWriteDeadline.
func (c *countingConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	atomic.AddInt64(&c.c.bytesWritten, int64(n))

	return n, err
}

func (c *Client) Preload(req cache.PreloadRequest, cb func(resp cache.ResponseItem)) error {
	j, err := json.Marshal(req)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, c.baseURL+"/preload", bytes.NewReader(j))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r, "preload")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if err := checkStatus(res, http.StatusOK, "preload"); err != nil {
		return err
	}

	c.mu.Lock()
	c.lastPreloadSources = strings.TrimSpace(res.Header.Get("X-GoCacheProgD-Preload-Sources"))
	c.mu.Unlock()

	var resp cache.Response

	preloadItems := 0
	_, err = resp.ReaderFrom(res.Body, func(item cache.ResponseItem, body io.Reader) error {
		preloadItems++

		if item.Size != 0 {
			if body == nil {
				return fmt.Errorf("empty body, item: %v", item)
			}

			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(body), nil
			})
		}

		cb(item)

		return nil
	})

	atomic.AddInt64(&c.preloadItems, int64(preloadItems))
	atomic.AddInt64(&c.preloadBytes, atomic.LoadInt64(&c.bytesRead))
	atomic.StoreInt64(&c.bytesRead, 0)
	atomic.StoreInt64(&c.bytesWritten, 0)

	return err
}

func (c *Client) PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error {
	if commit == "" && changesID == "" {
		return nil
	}

	body := strings.NewReader(strings.Join(actionIDs, "\n"))
	v := url.Values{}
	if commit != "" {
		v.Set("commit", commit)
	}
	if changesID != "" {
		v.Set("changes-id", changesID)
	}
	if buildType != "" {
		v.Set("build-type", buildType)
	}
	if replaceChanges {
		v.Set("replace-changes", "1")
	}
	endpoint := c.baseURL + "/cache-used?" + v.Encode()

	r, err := http.NewRequest(http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "text/plain")
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r, "cache-used")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("cache-used status %d: %s", res.StatusCode, strings.TrimSpace(string(b)))
	}

	return nil
}

func (c *Client) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	atomic.AddInt64(&c.getCnt, 1)
	atomic.AddInt64(&c.getReqCnt, 1)

	j, err := json.Marshal(req)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, c.baseURL+"/get", bytes.NewReader(j))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	setAuthHeader(r, c.authToken)

	st := time.Now()

	res, err := c.roundTrip(r, "get")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if err := checkStatus(res, http.StatusOK, "get"); err != nil {
		return err
	}

	var resp cache.Response

	_, err = resp.ReaderFrom(res.Body, func(item cache.ResponseItem, body io.Reader) error {
		if item.Size != 0 {
			if body == nil {
				return fmt.Errorf("empty body, item: %v", item)
			}

			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(body), nil
			})
		}

		cb(item)

		return nil
	})

	c.latencyGet.Add(1000 * time.Since(st).Seconds())

	return err
}

func (c *Client) head(req cache.Request) (cache.Response, error) {
	atomic.AddInt64(&c.getCnt, 1)
	var resp cache.Response

	j, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}

	r, err := http.NewRequest(http.MethodPost, c.baseURL+"/head", bytes.NewReader(j))
	if err != nil {
		return resp, err
	}
	r.Header.Set("Content-Type", "application/json")
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r, "head")
	if err != nil {
		return resp, err
	}
	defer res.Body.Close()

	if err := checkStatus(res, http.StatusOK, "head"); err != nil {
		return resp, err
	}

	j, err = io.ReadAll(res.Body)
	if err != nil {
		return resp, err
	}

	err = json.Unmarshal(j, &resp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (c *Client) filterPutItems(values cache.Response) (cache.Response, error) {
	var headReq cache.Request
	for _, item := range values.Items {
		headReq.ActionIDs = append(headReq.ActionIDs, item.ActionID)
	}
	exists, err := c.head(headReq)
	if err != nil {
		return values, fmt.Errorf("head: %w", err)
	}

	actionIDs := make(map[string]struct{})
	for _, item := range exists.Items {
		if !item.Miss && item.ActionID != "" {
			actionIDs[item.ActionID] = struct{}{}
		}
	}

	filteredItems := make([]cache.ResponseItem, 0, len(values.Items))
	for _, item := range values.Items {
		if _, ok := actionIDs[item.ActionID]; ok {
			continue
		}

		if item.Size >= cache.MinCompressionSize {
			rd, err := item.CompressedBodyReader()

			item.SetBodyReader(func() (io.ReadCloser, error) {
				return rd, err
			})
		}

		filteredItems = append(filteredItems, item)
	}

	values.Items = filteredItems

	return values, nil
}

func (c *Client) Put(values cache.Response) error {
	atomic.AddInt64(&c.putCnt, 1)

	values, err := c.filterPutItems(values)
	if err != nil {
		return err
	}

	cl, err := values.ContentLength()
	if err != nil {
		return err
	}

	rd, err := values.ReaderNaive()
	if err != nil {
		return fmt.Errorf("buffering request body: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.baseURL+"/put", rd)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.ContentLength = cl
	setAuthHeader(req, c.authToken)

	st := time.Now()

	res, err := c.roundTrip(req, "put")
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer res.Body.Close()

	c.latencyPut.Add(1000 * time.Since(st).Seconds())

	if err := checkStatus(res, http.StatusNoContent, "put"); err != nil {
		return err
	}

	return nil
}

func checkStatus(res *http.Response, expected int, op string) error {
	if res.StatusCode == expected {
		return nil
	}

	b, _ := io.ReadAll(res.Body)
	msg := strings.TrimSpace(string(b))
	if msg == "" {
		msg = http.StatusText(res.StatusCode)
	}

	return fmt.Errorf("%s status %d: %s", op, res.StatusCode, msg)
}

func (c *Client) Stats() map[string]string {
	c.mu.Lock()
	lastPreloadSources := c.lastPreloadSources
	c.mu.Unlock()

	return map[string]string{
		"bytes_read":      byteSize(atomic.LoadInt64(&c.bytesRead)),
		"bytes_written":   byteSize(atomic.LoadInt64(&c.bytesWritten)),
		"preload_bytes":   byteSize(atomic.LoadInt64(&c.preloadBytes)),
		"preload_sources": lastPreloadSources,
		"preloaded":       fmt.Sprintf("%d", atomic.LoadInt64(&c.preloadItems)),
		"get_95%":         fmt.Sprintf("%.2fms", c.latencyGet.Percentile(95)),
		"get_cnt":         fmt.Sprintf("%d", atomic.LoadInt64(&c.getCnt)),
		"get_req_cnt":     fmt.Sprintf("%d", atomic.LoadInt64(&c.getReqCnt)),
		"put_95%":         fmt.Sprintf("%.2fms", c.latencyPut.Percentile(95)),
		"put_cnt":         fmt.Sprintf("%d", atomic.LoadInt64(&c.putCnt)),
	}
}

func (c *Client) LastPreloadSources() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastPreloadSources
}

func setAuthHeader(r *http.Request, authToken string) {
	if strings.TrimSpace(authToken) == "" {
		return
	}

	r.Header.Set("Authorization", "Bearer "+authToken)
}

func (c *Client) roundTrip(req *http.Request, op string) (*http.Response, error) {
	res, err := c.tr.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusBadGateway && res.StatusCode != http.StatusGatewayTimeout {
		return res, nil
	}

	if req.GetBody == nil && req.Body != nil && req.Body != http.NoBody {
		return res, nil
	}

	b, readErr := io.ReadAll(res.Body)
	res.Body.Close()
	if readErr != nil {
		return nil, readErr
	}

	log.Printf("%s got status %d, retrying once in %s", op, res.StatusCode, gatewayRetryDelay)
	time.Sleep(gatewayRetryDelay)

	var body io.ReadCloser
	if req.GetBody != nil {
		body, err = req.GetBody()
		if err != nil {
			return nil, err
		}
	} else {
		body = http.NoBody
	}

	retryReq := req.Clone(req.Context())
	retryReq.Body = body
	retryReq.GetBody = req.GetBody
	retryReq.ContentLength = req.ContentLength

	retryRes, err := c.tr.RoundTrip(retryReq)
	if err != nil {
		return nil, err
	}

	if retryRes.StatusCode == http.StatusBadGateway || retryRes.StatusCode == http.StatusGatewayTimeout {
		retryRes.Body.Close()
		return &http.Response{
			StatusCode: res.StatusCode,
			Status:     res.Status,
			Header:     res.Header.Clone(),
			Body:       io.NopCloser(bytes.NewReader(b)),
		}, nil
	}

	return retryRes, nil
}

// Bytes.
const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
	PETABYTE
	EXABYTE
)

// byteSize returns a human-readable byte string of the form 10M, 12.5K, and so forth.
func byteSize(bytes int64) string {
	var (
		unit  string
		value = float64(bytes)
	)

	switch {
	case bytes >= EXABYTE:
		unit = "EB"
		value /= EXABYTE
	case bytes >= PETABYTE:
		unit = "PB"
		value /= PETABYTE
	case bytes >= TERABYTE:
		unit = "TB"
		value /= TERABYTE
	case bytes >= GIGABYTE:
		unit = "GB"
		value /= GIGABYTE
	case bytes >= MEGABYTE:
		unit = "MB"
		value /= MEGABYTE
	case bytes >= KILOBYTE:
		unit = "KB"
		value /= KILOBYTE
	default:
		unit = "B"
	}

	result := strconv.FormatFloat(value, 'f', 1, 64)
	result = strings.TrimSuffix(result, ".0")

	return result + unit
}
