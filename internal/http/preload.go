package http

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vearutop/gocacheprog/internal/cache"
)

func (h *Handler) Preload(rw http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.preloadStarted, 1)
	waitStartedAt := time.Now()
	h.preloadSem <- struct{}{}
	queueWait := time.Since(waitStartedAt)
	prepareStartedAt := time.Now()
	atomic.AddInt64(&h.preloadInFlight, 1)
	defer func() {
		atomic.AddInt64(&h.preloadInFlight, -1)
		atomic.AddInt64(&h.preloadCompleted, 1)
		<-h.preloadSem
	}()

	bb, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	if err := r.Body.Close(); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	var req cache.PreloadRequest
	if err := json.Unmarshal(bb, &req); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var resp cache.Response

	p, ok := h.store.(cache.Preloader)
	if !ok {
		http.Error(rw, "preload is not supported", http.StatusNotImplemented)
		return
	}

	preloadSources := ""
	if s, ok := h.store.(cache.PreloadSourceProvider); ok {
		sources, err := s.PreloadSources(req)
		if err != nil {
			if isManifestValidationError(err) {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(sources) > 0 {
			preloadSources = strings.Join(sources, ",")
			rw.Header().Set(headerPreloadSources, preloadSources)
		}
	}

	err = p.Preload(req, func(item cache.ResponseItem) {
		if item.WireSize == 0 {
			item.WireSize = item.Size
		}
		if item.DiskPath != "" {
			diskPath := item.DiskPath
			item.SetBodyReader(func() (io.ReadCloser, error) {
				f, err := os.Open(diskPath)
				if err != nil {
					return nil, err
				}
				return f, nil
			})

			item.DiskPath = ""
		}

		resp.Items = append(resp.Items, item)
	})
	if err != nil {
		if isManifestValidationError(err) {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	cl, err := resp.ContentLength()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	prepareTime := time.Since(prepareStartedAt)
	totalTime := queueWait + prepareTime

	log.Printf(
		"preload queue_wait=%s prepare_time=%s total_time=%s; remote=%s; commit=%q; parent=%q; changes=%q; build_type=%q; sources=%s; items=%d; content_length=%d",
		queueWait,
		prepareTime,
		totalTime,
		r.RemoteAddr,
		req.Commit,
		req.ParentCommit,
		req.ChangesID,
		req.BuildType,
		preloadSources,
		len(resp.Items),
		cl,
	)

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.Itoa(int(cl)))
	rw.WriteHeader(http.StatusOK)

	n, err := resp.WriteTo(rw)
	if err != nil {
		log.Println("get error:", err.Error(), "; bytes written:", n, "; content length:", cl)
	}
}
