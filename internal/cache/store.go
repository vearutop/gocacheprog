package cache

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"time"
)

type Store interface {
	Get(req Request) (Response, error)
	Put(values Response) error
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

	bodyReader func() io.ReadCloser
}

type Response struct {
	Items []ResponseItem `json:",omitempty"`
}

type Request struct {
	ActionIDs []string `json:",omitempty"`
}

func (ri *ResponseItem) SetBodyReader(bodyReader func() io.ReadCloser) {
	ri.bodyReader = bodyReader
}

func (ri *ResponseItem) UncompressedBodyReader() io.ReadCloser {
	return ri.bodyReader()
}

func (ri *ResponseItem) WriteTo(w io.Writer) (int64, error) {
	if ri.Size == 0 {
		return 0, nil
	}

	bodyReader := ri.bodyReader()
	defer func() {
		if err := bodyReader.Close(); err != nil {
			log.Println(err.Error())
		}
	}()

	n, err := io.Copy(w, bodyReader)

	return n, err
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
		return totalBytesWritten, err
	}
	totalBytesWritten += 4 // Length of the encoded int32

	// Write the JSON data itself
	n, err := w.Write(jsonData)
	if err != nil {
		return totalBytesWritten + int64(n), err
	}
	totalBytesWritten += int64(n)

	// Iterate through each ResponseItem and write its body
	for _, item := range r.Items {
		n, err := item.WriteTo(w)
		totalBytesWritten += n
		if err != nil {
			return totalBytesWritten, err
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
		total += item.WireSize
	}

	return total, nil
}
