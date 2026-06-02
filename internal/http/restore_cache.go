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
	_, err = h.gocacheStore.Restore(req, func(item gocache.FileItem) {
		if err := sw.WriteItem(item); err != nil {
			log.Print("restore-cache write item error")
		}
	})
	if err != nil {
		log.Print("restore-cache prepare error")
		return
	}
	if err := sw.Close(); err != nil {
		log.Print("restore-cache close error")
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

	return gocache.Request{
		Commit:       strings.TrimSpace(r.URL.Query().Get("commit")),
		ChangesID:    strings.TrimSpace(r.URL.Query().Get("changes-id")),
		BuildType:    strings.TrimSpace(r.URL.Query().Get("build-type")),
		BaseCommit:   strings.TrimSpace(r.URL.Query().Get("base-commit")),
		ParentCommit: strings.TrimSpace(r.URL.Query().Get("parent-commit")),
		MaxFileBytes: maxFileBytes,
	}
}
