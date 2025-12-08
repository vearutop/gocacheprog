package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"
)

type Store interface {
	Get(req Request, cb func(resp ResponseItem)) error
	Put(values Response) error
}

type Preloader interface {
	Preload(req PreloadRequest, cb func(resp ResponseItem)) error
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
	MaxSize int64 `json:",omitempty"`
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
				data, err := os.ReadFile(diskPath)
				if err != nil {
					return nil, err
				}

				return io.NopCloser(bytes.NewReader(data)), nil
			}

			f, err := os.Open(diskPath)
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

	return ri.bodyReader()
}

// WireBodyReader can be encoded or compressed.
func (ri *ResponseItem) WireBodyReader() (io.ReadCloser, error) {
	return ri.UncompressedBodyReader()
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
		if err := bodyReader.Close(); err != nil {
			log.Println("close item writer:", err.Error())
		}
	}()

	buf := make([]byte, ri.WireSize)
	n, err := io.ReadFull(bodyReader, buf)
	if err != nil {
		if f, ok := bodyReader.(*os.File); ok {
			fi, err := f.Stat()
			if err != nil {
				log.Println("stat file:", err.Error())
			}
			offset, _ := f.Seek(0, io.SeekCurrent)
			sys := fi.Sys().(*syscall.Stat_t)
			log.Printf("file: %s\n", f.Name())
			log.Printf("inode: %d, nlink: %d\n", sys.Ino, sys.Nlink)
			log.Printf("file size: %d, current offset: %d\n", fi.Size(), offset)
			time.Sleep(1 * time.Second)

			fi, err = os.Stat(f.Name()) // <-- note: os.Stat, not f.Stat()
			if err != nil {
				log.Println("stat 2 file:", err.Error())
			}
			sys = fi.Sys().(*syscall.Stat_t)
			log.Printf("after sleep inode: %d, nlink: %d\n", sys.Ino, sys.Nlink)
			log.Printf("after sleep file size: %d, current offset: %d\n", fi.Size(), offset)

		}

		return int64(n), fmt.Errorf("read item %T (read %d bytes) %+v: %w", bodyReader, n, ri, err)
	}

	n, err = w.Write(buf)
	if err != nil {
		return int64(n), fmt.Errorf("write item %T (written %d bytes) %+v: %w", bodyReader, n, ri, err)
	}

	//n, err := io.Copy(w, bodyReader)
	//if err != nil {
	//	return n, fmt.Errorf("write item %+v: %w", ri, err)
	//}

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
	jsonLength := int32(len(jsonData))
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
	jsonLength := int32(len(jsonData))
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
		if item.WireSize > 0 {
			bodyReader := io.LimitReader(rd, item.WireSize)

			if err := read(item, bodyReader); err != nil {
				return totalBytesRead, err
			}

			totalBytesRead += item.WireSize
		} else {
			if err := read(item, nil); err != nil {
				return totalBytesRead, err
			}
		}
	}

	return totalBytesRead, nil
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
