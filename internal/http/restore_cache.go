package http

import (
	"encoding/binary"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vearutop/gocacheprog/internal/gocache"
)

const (
	headerRestoreSources = "X-Gocacheprog-Restore-Sources"
)

func (h *Handler) RestoreCache(rw http.ResponseWriter, r *http.Request) {
	if h.gocacheStore == nil {
		http.Error(rw, "restore-cache is not supported", http.StatusNotImplemented)
		return
	}

	startedAt := time.Now()
	req := parseGOCACHERequest(r)
	sources, err := h.gocacheStore.RestoreSources(req)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	prepareTime := time.Since(startedAt)
	if len(sources) == 0 {
		totalTime := time.Since(startedAt)
		rw.Header().Set("Content-Type", "application/octet-stream")
		rw.Header().Set("Content-Length", "4")
		rw.Header().Set(headerRestoreSources, "")
		rw.Header().Set(headerRestorePrepareTime, prepareTime.String())
		rw.Header().Set(headerRestoreTotalTime, totalTime.String())
		rw.WriteHeader(http.StatusOK)
		if err := binary.Write(rw, binary.BigEndian, int32(0)); err != nil {
			log.Printf("restore-cache empty write error: %s", err.Error())
		}
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Add("Trailer", headerRestoreTotalTime)
	rw.Header().Set(headerRestoreSources, strings.Join(sources, ","))
	rw.Header().Set(headerRestorePrepareTime, prepareTime.String())
	rw.WriteHeader(http.StatusOK)

	sw := gocache.NewStreamWriter(rw)
	var writeErrLogged bool
	_, err = h.gocacheStore.Restore(req, func(item gocache.FileItem) {
		// Once one item's header is flushed without a matching body, the stream is
		// desynced for the rest of the response; stop attempting further writes so this
		// doesn't log once per remaining item for what is really a single failure.
		if writeErrLogged {
			return
		}
		if err := sw.WriteItem(item); err != nil {
			log.Printf("restore-cache write item error: path=%q: %s", item.Path, err.Error())
			writeErrLogged = true
		}
	})
	if err != nil {
		log.Printf("restore-cache prepare error: %s", err.Error())
		return
	}
	if err := sw.Close(); err != nil {
		log.Printf("restore-cache close error: %s", err.Error())
	}
	rw.Header().Set(headerRestoreTotalTime, time.Since(startedAt).String())
}

func parseGOCACHERequest(r *http.Request) gocache.Request {
	maxFileBytes := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("max-file-bytes")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			maxFileBytes = parsed
		}
	}
	restoreLimitBytes := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("restore-limit-bytes")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			restoreLimitBytes = parsed
		}
	}

	return gocache.Request{
		Commit:            strings.TrimSpace(r.URL.Query().Get("commit")),
		ChangesID:         strings.TrimSpace(r.URL.Query().Get("changes-id")),
		BuildType:         strings.TrimSpace(r.URL.Query().Get("build-type")),
		BaseCommit:        strings.TrimSpace(r.URL.Query().Get("base-commit")),
		ParentCommit:      strings.TrimSpace(r.URL.Query().Get("parent-commit")),
		MaxFileBytes:      maxFileBytes,
		RestoreLimitBytes: restoreLimitBytes,
	}
}
