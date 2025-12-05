package http_test

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/http"
	"github.com/vearutop/gocacheprogd/internal/local"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	localStore, err := local.NewStore("./testdata")
	require.NoError(t, err)

	now := time.Now()
	var items []cache.ResponseItem
	for i := 0; i < 10; i++ {
		item := cache.ResponseItem{}

		item.ActionID = "actionId" + strconv.Itoa(i)
		body := "body" + strconv.Itoa(i)
		item.Size = int64(len(body))
		item.OutputID = "outputId" + strconv.Itoa(i)
		item.Time = &now
		item.WireSize = item.Size
		item.SetBodyReader(func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBufferString(body)), nil
		})
		items = append(items, item)
	}

	h := http.NewHandler(localStore)

	srv := httptest.NewServer(h)

	client, err := http.NewClient(srv.URL)
	require.NoError(t, err)

	require.NoError(t, client.Put(cache.Response{Items: items}))

	req := cache.Request{}
	for i := 0; i < 5; i++ {
		req.ActionIDs = append(req.ActionIDs, "actionId"+strconv.Itoa(i))
	}

	require.NoError(t, client.Get(req, func(item cache.ResponseItem) {
		rd, err := item.UncompressedBodyReader()
		require.NoError(t, err)

		defer rd.Close()

		b, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, strings.ReplaceAll(item.ActionID, "actionId", "body"), string(b))
	}))
}
