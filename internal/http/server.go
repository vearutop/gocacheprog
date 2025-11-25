package http

import (
	"encoding/binary"
	"encoding/json"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"io"
	"log"
	"time"
)

type Repo interface {
	Get(req Request) (Response, error)
	Put(req Request) (Response, error)
}

type Handler struct {
}

func (h *Handler) Serve(req cache.Request) {
}
