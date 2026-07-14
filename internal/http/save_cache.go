package http

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/vearutop/gocacheprog/internal/gocache"
)

func (h *Handler) SaveCache(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "save-cache is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	req := parseGOCACHERequest(r)
	if err := h.processSaveCacheStream(req, r.Body, "single-request"); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
}

func (h *Handler) StartSaveCache(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "save-cache is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	req := parseGOCACHERequest(r)
	uploadID, err := saveUploadID(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)

	h.saveSessionsMu.Lock()
	if _, exists := h.saveSessions[uploadID]; exists {
		h.saveSessionsMu.Unlock()
		closeWithLog(pr, "close save-cache start reader")
		closeWithLog(pw, "close save-cache start writer")
		http.Error(rw, "upload session already exists", http.StatusConflict)
		return
	}
	now := time.Now()
	h.saveSessions[uploadID] = &saveCacheSession{writer: pw, done: done, startedAt: now, lastLogUnixNano: now.UnixNano()}
	h.saveSessionsMu.Unlock()

	log.Printf("save-cache upload_id=%q start received: commit=%q changes=%q build_type=%q", uploadID, req.Commit, req.ChangesID, req.BuildType)

	go func() {
		done <- h.processSaveCacheStream(req, pr, uploadID)
	}()

	if maxFileBytes := h.gocacheStore.MaxFileBytes(); maxFileBytes > 0 {
		rw.Header().Set(headerSaveMaxFileBytes, strconv.FormatInt(maxFileBytes, 10))
	}
	rw.WriteHeader(http.StatusNoContent)
}

func (h *Handler) SaveCacheChunk(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "save-cache is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	session, uploadID, err := h.lookupSaveSession(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	n, err := io.Copy(session.writer, r.Body)
	atomic.AddInt64(&session.chunks, 1)
	atomic.AddInt64(&session.bytes, n)
	if err != nil {
		closeWithLog(session.writer, "close save-cache session writer after chunk failure")
		h.deleteSaveSession(uploadID)
		http.Error(rw, fmt.Sprintf("save-cache upload %s chunk failed after %d chunks, %d bytes: %s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), err.Error()), http.StatusInternalServerError)
		return
	}

	logSaveCacheProgress(uploadID, session)

	rw.WriteHeader(http.StatusNoContent)
}

func logSaveCacheProgress(uploadID string, session *saveCacheSession) {
	last := atomic.LoadInt64(&session.lastLogUnixNano)
	now := time.Now()
	if now.Sub(time.Unix(0, last)) < saveCacheProgressLogInterval {
		return
	}
	if !atomic.CompareAndSwapInt64(&session.lastLogUnixNano, last, now.UnixNano()) {
		return
	}

	log.Printf("save-cache upload_id=%q progress: received_chunks=%d received_bytes=%d elapsed=%s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), now.Sub(session.startedAt))
}

func (h *Handler) FinalizeSaveCache(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "save-cache is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	session, uploadID, err := h.lookupSaveSession(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("save-cache upload_id=%q finalize received: received_chunks=%d received_bytes=%d elapsed=%s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), time.Since(session.startedAt))

	if err := session.writer.Close(); err != nil {
		h.deleteSaveSession(uploadID)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	err = <-session.done
	h.deleteSaveSession(uploadID)
	if err != nil {
		http.Error(rw, fmt.Sprintf("save-cache upload %s finalize failed after %d chunks, %d bytes, duration %s: %s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), time.Since(session.startedAt), err.Error()), http.StatusInternalServerError)
		return
	}

	log.Printf("save-cache upload_id=%q finalize succeeded: received_chunks=%d received_bytes=%d duration=%s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), time.Since(session.startedAt))

	rw.Header().Set(headerSaveTotalTime, time.Since(session.startedAt).String())
	rw.WriteHeader(http.StatusNoContent)
}

func (h *Handler) AbortSaveCache(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "save-cache is not supported", http.StatusNotImplemented)
		return
	}

	defer closeRequestBody(r)

	session, uploadID, err := h.lookupSaveSession(r)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("save-cache upload_id=%q abort received: received_chunks=%d received_bytes=%d elapsed=%s", uploadID, atomic.LoadInt64(&session.chunks), atomic.LoadInt64(&session.bytes), time.Since(session.startedAt))

	closeWithLog(session.writer, "close save-cache session writer on abort")
	h.deleteSaveSession(uploadID)
	<-session.done

	rw.WriteHeader(http.StatusNoContent)
}

func (h *Handler) processSaveCacheStream(req gocache.Request, body io.Reader, uploadID string) error {
	paths := make([]string, 0)
	progress := saveCacheProgress{uploadID: uploadID}
	streamBytes, err := gocache.ReadStream(body, func(item gocache.FileItem, itemBody io.Reader) error {
		if item.Size != 0 && itemBody == nil {
			return fmt.Errorf("upload_id=%s item=%d path=%q size=%d wire_size=%d: empty body", uploadID, progress.items+1, item.Path, item.Size, item.WireSize)
		}

		progress.items++
		progress.path = item.Path
		progress.sourceBytes += item.Size

		if maxFileBytes := h.gocacheStore.MaxFileBytes(); maxFileBytes > 0 && item.Size > maxFileBytes {
			// Store.SaveItem would skip this item without reading its body (it
			// exceeds -max-file-bytes). Recognize that here too, before the
			// truncation check below, so an intentional skip isn't mistaken for a
			// truncated upload: ReadStream still drains the unread body bytes on
			// its own regardless of what this callback does with itemBody.
			return nil
		}

		expectedWireSize := item.WireSize
		if expectedWireSize == 0 {
			expectedWireSize = item.Size
		}

		var counted *countingReader
		if itemBody != nil {
			counted = &countingReader{rd: itemBody}
			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(counted), nil
			})
		}
		if err := h.gocacheStore.SaveItem(item); err != nil {
			readBytes := int64(0)
			if counted != nil {
				readBytes = counted.n
			}
			return fmt.Errorf("upload_id=%s item=%d path=%q size=%d wire_size=%d read_wire_bytes=%d: save item: %w", uploadID, progress.items, item.Path, item.Size, expectedWireSize, readBytes, err)
		}
		if counted != nil && counted.n != expectedWireSize {
			return fmt.Errorf("upload_id=%s item=%d path=%q size=%d wire_size=%d read_wire_bytes=%d: truncated item body", uploadID, progress.items, item.Path, item.Size, expectedWireSize, counted.n)
		}
		progress.wireBytes += expectedWireSize
		paths = append(paths, item.Path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("read save-cache stream upload_id=%s items=%d wire_bytes=%d source_bytes=%d stream_bytes=%d last_path=%q: %w", uploadID, progress.items, progress.wireBytes, progress.sourceBytes, streamBytes, progress.path, err)
	}

	if err := h.gocacheStore.MergeSavedPaths(req, paths); err != nil {
		return fmt.Errorf("merge save-cache manifests upload_id=%s items=%d wire_bytes=%d source_bytes=%d: %w", uploadID, progress.items, progress.wireBytes, progress.sourceBytes, err)
	}

	return nil
}

type saveCacheProgress struct {
	uploadID    string
	items       int
	wireBytes   int64
	sourceBytes int64
	path        string
}

type countingReader struct {
	rd io.Reader
	n  int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.rd.Read(p)
	r.n += int64(n)
	return n, err
}

func (h *Handler) lookupSaveSession(r *http.Request) (*saveCacheSession, string, error) {
	uploadID, err := saveUploadID(r)
	if err != nil {
		return nil, "", err
	}

	h.saveSessionsMu.Lock()
	defer h.saveSessionsMu.Unlock()

	session, ok := h.saveSessions[uploadID]
	if !ok {
		return nil, "", errors.New("unknown upload-id")
	}

	return session, uploadID, nil
}

func (h *Handler) deleteSaveSession(uploadID string) {
	h.saveSessionsMu.Lock()
	defer h.saveSessionsMu.Unlock()

	delete(h.saveSessions, uploadID)
}

func saveUploadID(r *http.Request) (string, error) {
	uploadID := r.URL.Query().Get("upload-id")
	if uploadID == "" {
		return "", errors.New("missing upload-id")
	}

	return uploadID, nil
}

func closeRequestBody(r *http.Request) {
	if err := r.Body.Close(); err != nil {
		log.Print("close save-cache request body failed")
	}
}

func closeWithLog(c io.Closer, msg string) {
	if err := c.Close(); err != nil {
		log.Print(msg)
	}
}
