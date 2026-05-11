package local

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vearutop/gocacheprogd/internal/cache"
	"github.com/vearutop/gocacheprogd/internal/cacheprog"
)

func TestProxyPostCacheUsed_ReportsDedupedSortedActionIDs(t *testing.T) {
	upstream := &usageRecorderStub{}

	proxy, err := NewProxy(t.TempDir(), upstream, make(chan cacheprog.Response, 1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.recordUsedActionID("actionId2")
	proxy.recordUsedActionID("actionId1")
	proxy.recordUsedActionID("actionId2")

	require.NoError(t, proxy.PostCacheUsed("commit123"))
	require.True(t, upstream.called)
	require.Equal(t, "commit123", upstream.commit)
	require.Equal(t, []string{"actionId1", "actionId2"}, upstream.actionIDs)
}

func TestProxyPostCacheUsed_NoOpWithoutUsageRecorder(t *testing.T) {
	proxy, err := NewProxy(t.TempDir(), noopStore{}, make(chan cacheprog.Response, 1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
	})

	proxy.recordUsedActionID("actionId1")

	require.NoError(t, proxy.PostCacheUsed("commit123"))
	require.NoError(t, proxy.PostCacheUsed(""))
}

type usageRecorderStub struct {
	called    bool
	commit    string
	actionIDs []string
}

func (u *usageRecorderStub) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (u *usageRecorderStub) Put(values cache.Response) error {
	return nil
}

func (u *usageRecorderStub) PostCacheUsed(commit string, actionIDs []string) error {
	u.called = true
	u.commit = commit
	u.actionIDs = append([]string(nil), actionIDs...)

	return nil
}

type noopStore struct{}

func (noopStore) Get(req cache.Request, cb func(resp cache.ResponseItem)) error {
	return nil
}

func (noopStore) Put(values cache.Response) error {
	return nil
}
