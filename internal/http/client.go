package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vearutop/dynhist-go"
	"github.com/vearutop/gocacheprogd/internal/cache"
)

type Client struct {
	baseURL string

	tr *http.Transport

	latencyGet *dynhist.Collector
	latencyPut *dynhist.Collector

	bytesRead    int64
	bytesWritten int64
	preloadBytes int64
	preloadItems int64
	getCnt       int64
	putCnt       int64
}

func NewClient(baseURL string) (*Client, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")

	req, err := http.NewRequest(http.MethodGet, baseURL+"/version", nil)
	if err != nil {
		return nil, err
	}

	client := &Client{baseURL: baseURL}
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

	resp, err := client.tr.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(string(b), "gocacheprogd ") {
		return nil, fmt.Errorf("unexpected version: %s", string(b))
	}

	return client, nil
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

	res, err := c.tr.RoundTrip(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()

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

func (c *Client) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	atomic.AddInt64(&c.getCnt, 1)

	j, err := json.Marshal(req)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, c.baseURL+"/get", bytes.NewReader(j))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")

	st := time.Now()

	res, err := c.tr.RoundTrip(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()

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

	res, err := c.tr.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	defer res.Body.Close()

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

	st := time.Now()

	res, err := c.tr.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	c.latencyPut.Add(1000 * time.Since(st).Seconds())

	return res.Body.Close()
}

func (c *Client) Stats() map[string]string {
	return map[string]string{
		"bytes_read":    byteSize(atomic.LoadInt64(&c.bytesRead)),
		"bytes_written": byteSize(atomic.LoadInt64(&c.bytesWritten)),
		"preloaded":     fmt.Sprintf("%d", atomic.LoadInt64(&c.preloadItems)),
		"get_95%":       fmt.Sprintf("%.2fms", c.latencyGet.Percentile(95)),
		"get_cnt":       fmt.Sprintf("%d", atomic.LoadInt64(&c.getCnt)),
		"put_95%":       fmt.Sprintf("%.2fms", c.latencyPut.Percentile(95)),
		"put_cnt":       fmt.Sprintf("%d", atomic.LoadInt64(&c.putCnt)),
	}
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
