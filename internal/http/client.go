package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearutop/dynhist-go"
	"github.com/vearutop/gocacheprog/internal/cache"
	"github.com/vearutop/gocacheprog/internal/gocache"
)

var (
	gatewayRetryDelay   = 5 * time.Second
	saveCacheRetryDelay = 5 * time.Second
)

const (
	DefaultSaveCacheChunkBytes int64 = 900 * 1024
	saveCacheMaxRetries              = 3
)

const (
	headerSessionID          = "X-Gocacheprog-Session-Id"
	headerStartedAt          = "X-Gocacheprog-Started-At"
	headerPID                = "X-Gocacheprog-Pid"
	headerCacheDir           = "X-Gocacheprog-Cache-Dir"
	headerCommit             = "X-Gocacheprog-Commit"
	headerParent             = "X-Gocacheprog-Parent"
	headerChanges            = "X-Gocacheprog-Changes"
	headerBuildType          = "X-Gocacheprog-Build-Type"
	headerBase               = "X-Gocacheprog-Base"
	headerPreloadSources     = "X-Gocacheprog-Preload-Sources"
	headerPreloadQueueWait   = "X-Gocacheprog-Preload-Queue-Wait"
	headerPreloadPrepareTime = "X-Gocacheprog-Preload-Prepare-Time"
	headerPreloadTotalTime   = "X-Gocacheprog-Preload-Total-Time"
	headerRestorePrepareTime = "X-Gocacheprog-Restore-Prepare-Time"
	headerRestoreTotalTime   = "X-Gocacheprog-Restore-Total-Time"
	headerSaveTotalTime      = "X-Gocacheprog-Save-Total-Time"
)

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
	getTotalNs   int64
	putCnt       int64

	saveCacheMaxRequestBytes int64

	mu                     sync.Mutex
	lastPreloadSources     string
	lastPreloadQueueWait   string
	lastPreloadPrepareTime string
	lastPreloadTotalTime   string
	lastRestoreSources     string
	lastRestorePrepareTime string
	lastRestoreTotalTime   string
	lastSaveTotalTime      string
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

	client.saveCacheMaxRequestBytes = DefaultSaveCacheChunkBytes

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

	resp, err := client.roundTrip(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("close version body: %s", err.Error())
		}
	}()

	if err := checkStatus(resp, http.StatusOK, "version"); err != nil {
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, errors.New("authentication failed: -auth-token <value> is missing or incorrect")
		}
		return nil, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(string(b), "gocacheprog ") {
		return nil, fmt.Errorf("unexpected version: %s", string(b))
	}

	return client, nil
}

func setSessionHeaders(req *http.Request, sessionInfo *SessionInfo) {
	if sessionInfo == nil {
		return
	}

	if sessionInfo.SessionID != "" {
		req.Header.Set(headerSessionID, sessionInfo.SessionID)
	}
	if !sessionInfo.StartedAt.IsZero() {
		req.Header.Set(headerStartedAt, sessionInfo.StartedAt.UTC().Format(time.RFC3339Nano))
	}
	if sessionInfo.PID != 0 {
		req.Header.Set(headerPID, strconv.Itoa(sessionInfo.PID))
	}
	if sessionInfo.CacheDir != "" {
		req.Header.Set(headerCacheDir, sessionInfo.CacheDir)
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionCommit() != "" {
		req.Header.Set(headerCommit, sessionInfo.Params.SessionCommit())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionParentCommit() != "" {
		req.Header.Set(headerParent, sessionInfo.Params.SessionParentCommit())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionChangesID() != "" {
		req.Header.Set(headerChanges, sessionInfo.Params.SessionChangesID())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionBuildType() != "" {
		req.Header.Set(headerBuildType, sessionInfo.Params.SessionBuildType())
	}
	if sessionInfo.Params != nil && sessionInfo.Params.SessionBaseCommit() != "" {
		req.Header.Set(headerBase, sessionInfo.Params.SessionBaseCommit())
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

	res, err := c.roundTrip(r)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close preload body: %s", err.Error())
		}
	}()

	if err := checkStatus(res, http.StatusOK, "preload"); err != nil {
		return err
	}

	c.mu.Lock()
	c.lastPreloadSources = strings.TrimSpace(res.Header.Get(headerPreloadSources))
	c.lastPreloadQueueWait = strings.TrimSpace(res.Header.Get(headerPreloadQueueWait))
	c.lastPreloadPrepareTime = strings.TrimSpace(res.Header.Get(headerPreloadPrepareTime))
	c.lastPreloadTotalTime = strings.TrimSpace(res.Header.Get(headerPreloadTotalTime))
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

func (c *Client) RestoreCache(req gocache.Request, cb func(item gocache.FileItem, body io.Reader) error) (gocache.TransferStats, error) {
	startedAt := time.Now()
	r, err := http.NewRequest(http.MethodGet, c.baseURL+"/restore-cache?"+gocacheQuery(req).Encode(), nil)
	if err != nil {
		return gocache.TransferStats{}, err
	}
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r)
	if err != nil {
		return gocache.TransferStats{}, err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close restore-cache body: %s", err.Error())
		}
	}()

	if err := checkStatus(res, http.StatusOK, "restore-cache"); err != nil {
		return gocache.TransferStats{}, err
	}

	c.mu.Lock()
	c.lastRestoreSources = strings.TrimSpace(res.Header.Get(headerRestoreSources))
	c.lastRestorePrepareTime = strings.TrimSpace(res.Header.Get(headerRestorePrepareTime))
	c.lastRestoreTotalTime = strings.TrimSpace(res.Header.Get(headerRestoreTotalTime))
	c.mu.Unlock()

	var stats gocache.TransferStats
	_, err = gocache.ReadStream(res.Body, func(item gocache.FileItem, body io.Reader) error {
		stats.Files++
		stats.UncompressedBytes += item.Size
		if item.WireSize != 0 {
			stats.CompressedBytes += item.WireSize
		} else {
			stats.CompressedBytes += item.Size
		}

		if body != nil {
			data, err := io.ReadAll(body)
			if err != nil {
				return err
			}
			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data)), nil
			})

			rd, err := item.UncompressedBodyReader()
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := rd.Close(); closeErr != nil {
					log.Printf("close restore-cache decoded reader: %s", closeErr.Error())
				}
			}()

			return cb(item, rd)
		}

		return cb(item, nil)
	})
	c.mu.Lock()
	if totalTime := strings.TrimSpace(res.Trailer.Get(headerRestoreTotalTime)); totalTime != "" {
		c.lastRestoreTotalTime = totalTime
	}
	c.mu.Unlock()
	stats.Duration = time.Since(startedAt)
	return stats, err
}

func (c *Client) SaveCache(req gocache.Request, batch gocache.Batch) (gocache.TransferStats, error) {
	atomic.AddInt64(&c.putCnt, 1)
	startedAt := time.Now()
	if len(batch.Items) == 0 {
		return gocache.TransferStats{Duration: time.Since(startedAt)}, nil
	}

	var lastErr error
	for attempt := 1; attempt <= saveCacheMaxRetries+1; attempt++ {
		stats, err := c.saveCacheOnce(req, batch, startedAt)
		if err == nil {
			stats.Duration = time.Since(startedAt)
			return stats, nil
		}

		lastErr = err
		if attempt > saveCacheMaxRetries {
			break
		}
		log.Printf("save-cache upload attempt %d/%d failed, retrying in %s: %s", attempt, saveCacheMaxRetries+1, saveCacheRetryDelay, err.Error())
		time.Sleep(saveCacheRetryDelay)
	}

	return gocache.TransferStats{}, fmt.Errorf("save-cache upload failed after %d attempts: %w", saveCacheMaxRetries+1, lastErr)
}

func (c *Client) saveCacheOnce(req gocache.Request, batch gocache.Batch, startedAt time.Time) (gocache.TransferStats, error) {
	uploadID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10) + "-" + strconv.Itoa(os.Getpid())
	if err := c.startSaveCache(req, uploadID); err != nil {
		return gocache.TransferStats{}, err
	}

	maxRequestBytes := c.saveCacheMaxRequestBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = DefaultSaveCacheChunkBytes
	}

	var (
		stats             gocache.TransferStats
		writerErrCh       = make(chan error, 1)
		writerFiles       int64
		writerWireBytes   int64
		writerSourceBytes int64
	)

	pr, pw := io.Pipe()
	go func() {
		sw := gocache.NewStreamWriter(pw)
		for _, item := range batch.Items {
			atomic.AddInt64(&writerFiles, 1)
			atomic.AddInt64(&writerSourceBytes, item.Size)

			item, err := prepareSaveCacheItem(item)
			if err != nil {
				_ = pw.CloseWithError(err)
				writerErrCh <- err
				return
			}
			atomic.AddInt64(&writerWireBytes, wireBodySize(item))

			if err := sw.WriteItem(item); err != nil {
				_ = pw.CloseWithError(err)
				writerErrCh <- err
				return
			}
		}

		if err := sw.Close(); err != nil {
			_ = pw.CloseWithError(err)
			writerErrCh <- err
			return
		}

		if err := pw.Close(); err != nil {
			log.Printf("close save-cache stream pipe writer: %s", err.Error())
		}
		writerErrCh <- nil
	}()

	buf := make([]byte, maxRequestBytes)
	for {
		n, err := io.ReadFull(pr, buf)
		if n > 0 {
			if chunkErr := c.saveCacheChunk(req, uploadID, buf[:n]); chunkErr != nil {
				logAbortSaveCacheError(c.abortSaveCache(req, uploadID))
				return gocache.TransferStats{}, fmt.Errorf("save-cache chunk upload failed upload_id=%s chunk_bytes=%d sent_files=%d sent_wire_bytes=%d sent_source_bytes=%d: %w", uploadID, n, atomic.LoadInt64(&writerFiles), atomic.LoadInt64(&writerWireBytes), atomic.LoadInt64(&writerSourceBytes), chunkErr)
			}
		}

		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}

		logAbortSaveCacheError(c.abortSaveCache(req, uploadID))
		return gocache.TransferStats{}, fmt.Errorf("read save-cache stream failed upload_id=%s sent_files=%d sent_wire_bytes=%d sent_source_bytes=%d: %w", uploadID, atomic.LoadInt64(&writerFiles), atomic.LoadInt64(&writerWireBytes), atomic.LoadInt64(&writerSourceBytes), err)
	}

	if writerErr := <-writerErrCh; writerErr != nil {
		logAbortSaveCacheError(c.abortSaveCache(req, uploadID))
		return gocache.TransferStats{}, fmt.Errorf("build save-cache stream failed upload_id=%s sent_files=%d sent_wire_bytes=%d sent_source_bytes=%d: %w", uploadID, atomic.LoadInt64(&writerFiles), atomic.LoadInt64(&writerWireBytes), atomic.LoadInt64(&writerSourceBytes), writerErr)
	}

	if err := c.finalizeSaveCache(req, uploadID); err != nil {
		logAbortSaveCacheError(c.abortSaveCache(req, uploadID))
		return gocache.TransferStats{}, fmt.Errorf("save-cache finalize failed upload_id=%s sent_files=%d sent_wire_bytes=%d sent_source_bytes=%d duration=%s: %w", uploadID, atomic.LoadInt64(&writerFiles), atomic.LoadInt64(&writerWireBytes), atomic.LoadInt64(&writerSourceBytes), time.Since(startedAt), err)
	}

	stats.Files = int(atomic.LoadInt64(&writerFiles))
	stats.CompressedBytes = atomic.LoadInt64(&writerWireBytes)
	stats.UncompressedBytes = atomic.LoadInt64(&writerSourceBytes)
	stats.Duration = time.Since(startedAt)
	return stats, nil
}

func prepareSaveCacheItem(item gocache.FileItem) (gocache.FileItem, error) {
	if item.Size < cache.MinCompressionSize {
		if item.WireSize == 0 {
			item.WireSize = item.Size
		}
		return item, nil
	}

	rd, err := item.CompressedBodyReader()
	if err != nil {
		return item, err
	}

	data, err := io.ReadAll(rd)
	if closeErr := rd.Close(); closeErr != nil {
		log.Printf("close save-cache compressed reader: %s", closeErr.Error())
	}
	if err != nil {
		return item, err
	}

	item.IsCompressed = true
	item.WireSize = int64(len(data))
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})

	return item, nil
}

func wireBodySize(item gocache.FileItem) int64 {
	if item.WireSize > 0 {
		return item.WireSize
	}

	return item.Size
}

func logAbortSaveCacheError(err error) {
	if err != nil {
		log.Printf("abort save-cache upload failed: %s", err.Error())
	}
}

func (c *Client) startSaveCache(req gocache.Request, uploadID string) error {
	return c.postSaveCacheControl(req, uploadID, "/save-cache-start", "save-cache-start")
}

func (c *Client) saveCacheChunk(req gocache.Request, uploadID string, chunk []byte) error {
	r, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.baseURL+"/save-cache-chunk?"+saveCacheQuery(req, uploadID).Encode(), bytes.NewReader(chunk))
	if err != nil {
		return err
	}
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close save-cache chunk body: %s", err.Error())
		}
	}()

	return checkStatus(res, http.StatusNoContent, "save-cache-chunk")
}

func (c *Client) finalizeSaveCache(req gocache.Request, uploadID string) error {
	return c.postSaveCacheControl(req, uploadID, "/save-cache-finalize", "save-cache-finalize")
}

func (c *Client) abortSaveCache(req gocache.Request, uploadID string) error {
	return c.postSaveCacheControl(req, uploadID, "/save-cache-abort", "save-cache-abort")
}

func (c *Client) postSaveCacheControl(req gocache.Request, uploadID, path, op string) error {
	r, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.baseURL+path+"?"+saveCacheQuery(req, uploadID).Encode(), nil)
	if err != nil {
		return err
	}
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close %s body: %s", op, err.Error())
		}
	}()

	c.mu.Lock()
	c.lastSaveTotalTime = strings.TrimSpace(res.Header.Get(headerSaveTotalTime))
	c.mu.Unlock()

	return checkStatus(res, http.StatusNoContent, op)
}

func gocacheQuery(req gocache.Request) url.Values {
	v := url.Values{}
	if req.Commit != "" {
		v.Set("commit", req.Commit)
	}
	if req.ChangesID != "" {
		v.Set("changes-id", req.ChangesID)
	}
	if req.BuildType != "" {
		v.Set("build-type", req.BuildType)
	}
	if req.BaseCommit != "" {
		v.Set("base-commit", req.BaseCommit)
	}
	if req.ParentCommit != "" {
		v.Set("parent-commit", req.ParentCommit)
	}
	if req.MaxFileBytes > 0 {
		v.Set("max-file-bytes", strconv.FormatInt(req.MaxFileBytes, 10))
	}
	if req.RestoreLimitBytes > 0 {
		v.Set("restore-limit-bytes", strconv.FormatInt(req.RestoreLimitBytes, 10))
	}
	return v
}

func saveCacheQuery(req gocache.Request, uploadID string) url.Values {
	v := gocacheQuery(req)
	if uploadID != "" {
		v.Set("upload-id", uploadID)
	}
	return v
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

	r, err := http.NewRequest(http.MethodPost, endpoint, body) //nolint:gosec // endpoint is an explicit user-configured remote cache URL.
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "text/plain")
	setAuthHeader(r, c.authToken)

	res, err := c.roundTrip(r)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Print("close cache-used body")
		}
	}()

	if res.StatusCode != http.StatusNoContent {
		b, readErr := io.ReadAll(res.Body)
		if readErr != nil {
			return fmt.Errorf("cache-used status %d and response read failed: %w", res.StatusCode, readErr)
		}
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

	res, err := c.roundTrip(r)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close get body: %s", err.Error())
		}
	}()

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

	elapsed := time.Since(st)
	c.latencyGet.Add(1000 * elapsed.Seconds())
	atomic.AddInt64(&c.getTotalNs, elapsed.Nanoseconds())

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

	res, err := c.roundTrip(r)
	if err != nil {
		return resp, err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close head body: %s", err.Error())
		}
	}()

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

	res, err := c.roundTrip(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("close put body: %s", err.Error())
		}
	}()

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

	b, readErr := io.ReadAll(res.Body)
	if readErr != nil {
		return fmt.Errorf("%s status %d: response read failed: %w", op, res.StatusCode, readErr)
	}
	msg := strings.TrimSpace(string(b))
	if msg == "" {
		msg = http.StatusText(res.StatusCode)
	}

	return fmt.Errorf("%s status %d: %s", op, res.StatusCode, msg)
}

func (c *Client) Stats() map[string]string {
	c.mu.Lock()
	lastPreloadSources := c.lastPreloadSources
	lastPreloadQueueWait := c.lastPreloadQueueWait
	lastPreloadPrepareTime := c.lastPreloadPrepareTime
	lastPreloadTotalTime := c.lastPreloadTotalTime
	c.mu.Unlock()

	return map[string]string{
		"bytes_read":           byteSize(atomic.LoadInt64(&c.bytesRead)),
		"bytes_written":        byteSize(atomic.LoadInt64(&c.bytesWritten)),
		"preload_bytes":        byteSize(atomic.LoadInt64(&c.preloadBytes)),
		"preload_sources":      lastPreloadSources,
		"preload_queue_wait":   lastPreloadQueueWait,
		"preload_prepare_time": lastPreloadPrepareTime,
		"preload_total_time":   lastPreloadTotalTime,
		"preloaded":            strconv.FormatInt(atomic.LoadInt64(&c.preloadItems), 10),
		"get_95%":              fmt.Sprintf("%.2fms", c.latencyGet.Percentile(95)),
		"get_cnt":              strconv.FormatInt(atomic.LoadInt64(&c.getCnt), 10),
		"get_req_cnt":          strconv.FormatInt(atomic.LoadInt64(&c.getReqCnt), 10),
		"get_total_time":       time.Duration(atomic.LoadInt64(&c.getTotalNs)).String(),
		"put_95%":              fmt.Sprintf("%.2fms", c.latencyPut.Percentile(95)),
		"put_cnt":              strconv.FormatInt(atomic.LoadInt64(&c.putCnt), 10),
	}
}

func (c *Client) LastPreloadSources() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastPreloadSources
}

func (c *Client) SetSaveCacheChunkBytes(maxBytes int64) {
	if maxBytes <= 0 {
		maxBytes = DefaultSaveCacheChunkBytes
	}

	c.saveCacheMaxRequestBytes = maxBytes
}

func (c *Client) LastRestoreSources() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastRestoreSources
}

func (c *Client) LastRestoreTimings() (prepareTime string, totalTime string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastRestorePrepareTime, c.lastRestoreTotalTime
}

func (c *Client) LastSaveTiming() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastSaveTotalTime
}

func (c *Client) LastPreloadTimings() (queueWait string, prepareTime string, totalTime string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastPreloadQueueWait, c.lastPreloadPrepareTime, c.lastPreloadTotalTime
}

func (c *Client) GetTotalTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&c.getTotalNs))
}

func setAuthHeader(r *http.Request, authToken string) {
	if strings.TrimSpace(authToken) == "" {
		return
	}

	r.Header.Set("Authorization", "Bearer "+authToken)
}

func (c *Client) roundTrip(req *http.Request) (*http.Response, error) {
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
	if err := res.Body.Close(); err != nil {
		log.Print("close gateway retry body")
	}
	if readErr != nil {
		return nil, readErr
	}

	log.Printf("gateway timeout, retrying once in %s", gatewayRetryDelay)
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
		if err := retryRes.Body.Close(); err != nil {
			log.Print("close retry response body")
		}
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
