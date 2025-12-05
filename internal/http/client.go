package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	baseURL string
}

func NewClient(baseURL string) (*Client, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")

	req, err := http.NewRequest(http.MethodGet, baseURL+"/version", nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
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

	return &Client{baseURL: baseURL}, nil
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

	res, err := http.DefaultTransport.RoundTrip(r)
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

	return err
}

func (c *Client) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	j, err := json.Marshal(req)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPost, c.baseURL+"/get", bytes.NewReader(j))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultTransport.RoundTrip(r)
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

	return err
}

func (c *Client) Put(values cache.Response) error {
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

	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	return res.Body.Close()
}
