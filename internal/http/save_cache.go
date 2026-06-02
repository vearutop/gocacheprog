package http

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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
	if err := h.processSaveCacheStream(req, r.Body); err != nil {
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
	h.saveSessions[uploadID] = &saveCacheSession{writer: pw, done: done, startedAt: time.Now()}
	h.saveSessionsMu.Unlock()

	go func() {
		done <- h.processSaveCacheStream(req, pr)
	}()

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

	if _, err := io.Copy(session.writer, r.Body); err != nil {
		closeWithLog(session.writer, "close save-cache session writer after chunk failure")
		h.deleteSaveSession(uploadID)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
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

	if err := session.writer.Close(); err != nil {
		h.deleteSaveSession(uploadID)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	err = <-session.done
	h.deleteSaveSession(uploadID)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

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

	closeWithLog(session.writer, "close save-cache session writer on abort")
	h.deleteSaveSession(uploadID)
	<-session.done

	rw.WriteHeader(http.StatusNoContent)
}

func (h *Handler) processSaveCacheStream(req gocache.Request, body io.Reader) error {
	paths := make([]string, 0)
	_, err := gocache.ReadStream(body, func(item gocache.FileItem, itemBody io.Reader) error {
		if item.Size != 0 && itemBody == nil {
			return fmt.Errorf("empty body for %s", item.Path)
		}

		if itemBody != nil {
			item.SetBodyReader(func() (io.ReadCloser, error) {
				return io.NopCloser(itemBody), nil
			})
		}
		if err := h.gocacheStore.SaveItem(item); err != nil {
			return err
		}
		paths = append(paths, item.Path)
		return nil
	})
	if err != nil {
		return err
	}

	return h.gocacheStore.MergeSavedPaths(req, paths)
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
