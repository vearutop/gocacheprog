package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

const MinCompressionSize = 200 // 200 bytes brings ratio of ~1.5x.

type Store interface {
	Get(req Request, cb func(resp ResponseItem)) error
	Put(values Response) error
}

type Preloader interface {
	Preload(req PreloadRequest, cb func(resp ResponseItem)) error
}

type UsageRecorder interface {
	PostCacheUsed(commit string, changesID string, buildType string, actionIDs []string, replaceChanges bool) error
}

type PreloadSourceProvider interface {
	PreloadSources(req PreloadRequest) ([]string, error)
}

// IntegrityChecker is implemented by stores that can walk their entries verifying stored bytes
// against declared metadata, reporting (and, unless dryRun, removing) any that don't match.
type IntegrityChecker interface {
	IntegrityCheck(dryRun bool) IntegrityReport
}

// IntegrityReportEntry describes one entry whose stored bytes didn't match its declared size.
type IntegrityReportEntry struct {
	ActionID string `json:"action_id"`
	OutputID string `json:"output_id"`
	Size     int64  `json:"size"`
	WireSize int64  `json:"wire_size,omitempty"`
	Error    string `json:"error"`
	Removed  bool   `json:"removed"`
}

// IntegrityReport is the result of an IntegrityCheck run.
type IntegrityReport struct {
	Checked int64                  `json:"checked"`
	Broken  []IntegrityReportEntry `json:"broken,omitempty"`
	DryRun  bool                   `json:"dry_run"`
}

type ResponseItem struct {
	ActionID     string     `json:",omitempty"`
	Miss         bool       `json:",omitempty"` // cache miss
	OutputID     string     `json:",omitempty"` // the OutputID stored with the body
	Size         int64      `json:",omitempty"` // body size in bytes
	Time         *time.Time `json:",omitempty"` // when the object was put in the cache (optional; used for cache expiration)
	IsCompressed bool       `json:",omitempty"`
	WireSize     int64      `json:",omitempty"` // file size over the wire, can be smaller if compressed
	DiskPath     string     `json:",omitempty"` // optional file path to uncompressed cache item, for local store

	bodyReader func() (io.ReadCloser, error)
}

type Response struct {
	Items []ResponseItem `json:",omitempty"`
}

type Request struct {
	ActionIDs []string `json:",omitempty"`
}

type PreloadRequest struct {
	MaxSize      int64  `json:",omitempty"`
	Commit       string `json:",omitempty"`
	ChangesID    string `json:",omitempty"`
	BuildType    string `json:",omitempty"`
	BaseCommit   string `json:",omitempty"`
	ParentCommit string `json:",omitempty"`
}

func (ri *ResponseItem) SetBodyReader(bodyReader func() (io.ReadCloser, error)) {
	ri.bodyReader = bodyReader
}

func (ri *ResponseItem) PrepareBodyReader() {
	if ri.bodyReader != nil {
		return
	}

	if ri.DiskPath != "" {
		diskPath := ri.DiskPath

		ri.bodyReader = func() (io.ReadCloser, error) {
			if ri.WireSize < 1e6 {
				data, err := os.ReadFile(diskPath) //nolint:gosec // diskPath comes from the local cache index.
				if err != nil {
					return nil, err
				}

				return io.NopCloser(bytes.NewReader(data)), nil
			}

			f, err := os.Open(diskPath) //nolint:gosec // diskPath comes from the local cache index.
			if err != nil {
				return nil, err
			}
			return f, nil
		}
	}
}

func (ri *ResponseItem) UncompressedBodyReader() (io.ReadCloser, error) {
	ri.PrepareBodyReader()

	if ri.Size == 0 && ri.WireSize == 0 {
		return nil, nil
	}

	if ri.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", ri)
	}

	// Dynamically decompress the body.
	if ri.IsCompressed {
		rd, err := ri.bodyReader()
		if err != nil {
			return nil, err
		}

		zrd, err := zstd.NewReader(rd)
		if err != nil {
			if closeErr := rd.Close(); closeErr != nil {
				log.Printf("close compressed body reader after zstd init failure: %s", closeErr.Error())
			}

			return nil, err
		}

		// rd must stay open for as long as the returned reader is read from - the zstd
		// decoder pulls compressed bytes from it lazily, not all at once - so closing it
		// here (rather than deferring to whoever closes the returned reader) would race
		// with, and eventually break, any read past whatever happened to already be
		// buffered. This bit small/in-memory bodies (see PrepareBodyReader's ReadFile path)
		// but broke any real *os.File-backed body once reads outlived this function's own
		// scope, surfacing as "file already closed".
		return &zstdReadCloser{ReadCloser: zrd.IOReadCloser(), inner: rd}, nil
	}

	return ri.bodyReader()
}

// zstdReadCloser closes both the zstd decoder and the underlying compressed-body reader it
// pulls from, so callers only need to close the one reader they were handed.
type zstdReadCloser struct {
	io.ReadCloser
	inner io.Closer
}

func (z *zstdReadCloser) Close() error {
	err1 := z.ReadCloser.Close()
	err2 := z.inner.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

// CompressedBodyReader updates item with compression.
func (ri *ResponseItem) CompressedBodyReader() (io.ReadCloser, error) {
	ri.PrepareBodyReader()

	if ri.Size == 0 && ri.WireSize == 0 {
		return nil, nil
	}

	if ri.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", ri)
	}

	if ri.IsCompressed {
		return ri.bodyReader()
	}

	rd, err := ri.bodyReader()
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rd.Close(); closeErr != nil {
			log.Printf("close body reader before compression: %s", closeErr.Error())
		}
	}()

	data, readErr := io.ReadAll(rd)
	if readErr != nil {
		return nil, readErr
	}

	buf := make([]byte, 0, len(data)/2)
	buf = zstd.EncodeTo(buf, data)

	ri.WireSize = int64(len(buf))
	ri.IsCompressed = true

	return io.NopCloser(bytes.NewReader(buf)), nil
}

// WireBodyReader can be encoded or compressed.
func (ri *ResponseItem) WireBodyReader() (io.ReadCloser, error) {
	ri.PrepareBodyReader()

	if ri.Size == 0 && ri.WireSize == 0 {
		return nil, nil
	}

	if ri.bodyReader == nil {
		return nil, fmt.Errorf("no body reader for item: %+v", ri)
	}

	return ri.bodyReader()
}

func (ri *ResponseItem) WriteTo(w io.Writer) (int64, error) {
	if ri.Size == 0 && ri.WireSize == 0 {
		return 0, nil
	}

	bodyReader, err := ri.WireBodyReader()
	if err != nil {
		return 0, err
	}

	if bodyReader == nil {
		return 0, nil
	}

	defer func() {
		if closeErr := bodyReader.Close(); closeErr != nil {
			log.Printf("close write body reader: %s", closeErr.Error())
		}
	}()

	buf := make([]byte, ri.WireSize)
	n, err := io.ReadFull(bodyReader, buf)
	if err != nil {
		return int64(n), fmt.Errorf("read item %T (read %d bytes) %+v: %w", bodyReader, n, ri, err)
	}

	n, err = w.Write(buf)
	if err != nil {
		return int64(n), fmt.Errorf("write item %T (written %d bytes) %+v: %w", bodyReader, n, ri, err)
	}

	if int64(n) != ri.WireSize {
		return int64(n), fmt.Errorf("unexpected item write: %d bytes, expected %d, %T, %+v", n, ri.WireSize, bodyReader, ri)
	}

	return int64(n), err
}

type pagesReader struct {
	// next returns contents of the next page,
	// io.EOF error indicates last page.
	next func() ([]byte, error)

	// buf keeps the data to be read.
	buf []byte
}

func (r *pagesReader) Read(p []byte) (n int, err error) {
	// Fill the reader buffer with pages
	// until it exceeds the incoming buffer
	// or reaches the end of pages.
	for len(r.buf) < len(p) {
		if r.next == nil {
			break
		}

		page, err := r.next()

		r.buf = append(r.buf, page...)
		if err != nil && errors.Is(err, io.EOF) {
			r.next = nil
			break
		}

		if err != nil {
			copy(p, r.buf)
			return len(r.buf), err
		}
	}

	// Put head of reader buffer in the incoming buffer.
	n = copy(p, r.buf)

	// Move remaining tail into the head of reader buffer.
	remaining := r.buf[n:]
	r.buf = r.buf[:len(remaining)]
	copy(r.buf, remaining)

	if len(r.buf) == 0 && r.next == nil {
		return n, io.EOF
	}

	return n, nil
}

func (r *Response) ReaderNaive() (io.Reader, error) {
	buf := bytes.NewBuffer(nil)

	cl, err := r.ContentLength()
	if err != nil {
		return nil, fmt.Errorf("content length: %w", err)
	}

	n, err := r.WriteTo(buf)
	if err != nil {
		return nil, fmt.Errorf("write to buffer: %w", err)
	}

	if n != cl {
		return nil, fmt.Errorf("unexpected content length: %d bytes, expected %d", n, cl)
	}

	return buf, nil
}

func (r *Response) Reader() (io.Reader, error) {
	rd := &pagesReader{}

	// Encode the Response as JSON
	jsonData, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)

	// Write the length of the JSON data as a 4-byte integer in binary format
	jsonLength, err := checkedJSONLength(jsonData)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, jsonLength)
	if err != nil {
		return nil, fmt.Errorf("write head: %w", err)
	}

	// Write the JSON data itself
	_, err = buf.Write(jsonData)
	if err != nil {
		return nil, err
	}

	rd.buf = buf.Bytes()
	idx := 0

	rd.next = func() ([]byte, error) {
		if idx >= len(r.Items) {
			return nil, io.EOF
		}

		// Iterate through each ResponseItem and write its body
		item := r.Items[idx]
		idx++

		buf.Reset()
		if _, err := item.WriteTo(buf); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	return rd, nil
}

func (r *Response) WriteTo(w io.Writer) (int64, error) {
	var totalBytesWritten int64

	// Encode the Response as JSON
	jsonData, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}

	// Write the length of the JSON data as a 4-byte integer in binary format
	jsonLength, err := checkedJSONLength(jsonData)
	if err != nil {
		return totalBytesWritten, err
	}
	err = binary.Write(w, binary.BigEndian, jsonLength)
	if err != nil {
		return totalBytesWritten, fmt.Errorf("write head: %w", err)
	}
	totalBytesWritten += 4 // Length of the encoded int32

	// Write the JSON data itself
	n, err := w.Write(jsonData)
	if err != nil {
		return totalBytesWritten + int64(n), err
	}
	totalBytesWritten += int64(n)

	// Iterate through each ResponseItem and write its body
	for i, item := range r.Items {
		n, err := item.WriteTo(w)
		totalBytesWritten += n
		if err != nil {
			return totalBytesWritten, fmt.Errorf("write item %d/%d: %w", i, len(r.Items), err)
		}
	}

	return totalBytesWritten, nil
}

func checkedJSONLength(jsonData []byte) (int32, error) {
	if len(jsonData) > math.MaxInt32 {
		return 0, fmt.Errorf("response header too large: %d bytes", len(jsonData))
	}

	return int32(len(jsonData)), nil //nolint:gosec // range is checked above.
}

func (r *Response) ReaderFrom(rd io.Reader, read func(item ResponseItem, body io.Reader) error) (int64, error) {
	var totalBytesRead int64

	// Read the length of the JSON data as a 4-byte integer
	var jsonLength int32
	err := binary.Read(rd, binary.BigEndian, &jsonLength)
	if err != nil {
		return totalBytesRead, err
	}
	totalBytesRead += 4 // Length of the encoded int32

	// Read the JSON data itself
	jsonData := make([]byte, jsonLength)
	n, err := io.ReadFull(rd, jsonData)
	totalBytesRead += int64(n)
	if err != nil {
		return totalBytesRead, err
	}

	// Unmarshal JSON data into the Response struct
	err = json.Unmarshal(jsonData, r)
	if err != nil {
		return totalBytesRead, err
	}

	// Read the body of each ResponseItem if necessary
	for _, item := range r.Items {
		if item.WireSize == 0 {
			if err := read(item, nil); err != nil {
				return totalBytesRead, err
			}

			continue
		}

		consumed, err := readItemBody(rd, item.WireSize, func(cr io.Reader) error { return read(item, cr) })
		totalBytesRead += consumed

		if err != nil {
			return totalBytesRead, fmt.Errorf("item %+v: %w", item, err)
		}
	}

	return totalBytesRead, nil
}

// ErrShortRead marks a ReaderFrom item whose actual bytes on the wire fell short of its
// declared WireSize - most likely a server-side index entry that doesn't match the object it
// actually streamed. Callers can check for this via errors.Is to tell "this response desynced
// mid-stream" apart from other failures (network errors, a decode error inside read itself),
// and to treat it as a data-integrity anomaly worth surfacing rather than a fatal failure.
var ErrShortRead = errors.New("short read")

// readItemBody hands read a wireSize-bounded view of rd, then drains any part of it read left
// unconsumed, so the caller's stream offset stays aligned to this item's declared boundary
// regardless of what read did with the body. It returns an error if the actual byte count on
// the wire fell short of wireSize (e.g. the server's index entry didn't match the object it
// actually streamed): failing loudly here, instead of silently continuing, matters because past
// a short item every later item in the same stream would be read from the wrong offset,
// corrupting each of them in turn rather than just this one.
func readItemBody(rd io.Reader, wireSize int64, read func(io.Reader) error) (int64, error) {
	cr := &countingReader{r: io.LimitReader(rd, wireSize)}

	if err := read(cr); err != nil {
		return 0, err
	}

	if _, err := io.Copy(io.Discard, cr); err != nil {
		return cr.n, fmt.Errorf("drain remaining body: %w", err)
	}

	if cr.n != wireSize {
		return cr.n, fmt.Errorf("%w: got %d bytes, want %d", ErrShortRead, cr.n, wireSize)
	}

	return cr.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

func (r *Response) ContentLength() (int64, error) {
	// Encode the Response as JSON
	jsonData, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}

	total := 4 + int64(len(jsonData))
	for _, item := range r.Items {
		size := item.Size
		if item.WireSize != 0 {
			size = item.WireSize
		}

		total += size
	}

	return total, nil
}
