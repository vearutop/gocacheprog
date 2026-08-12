package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClientSessionsSnapshot_PrunesDoneSessions exercises doneSessionRetention directly, since
// waiting out the real 5 minutes in a test would be impractical.
func TestClientSessionsSnapshot_PrunesDoneSessions(t *testing.T) {
	h := &Handler{
		clientSessions: map[string]*clientSession{
			"done-old": {
				Done:      true,
				DoneAt:    time.Now().Add(-doneSessionRetention - time.Second),
				FirstSeen: time.Now().Add(-time.Hour),
			},
			"done-recent": {
				Done:      true,
				DoneAt:    time.Now().Add(-time.Second),
				FirstSeen: time.Now().Add(-time.Minute),
			},
		},
	}

	views := h.clientSessionsSnapshot()
	require.Len(t, views, 1)
	require.Equal(t, "done", views[0].Status)

	h.clientSessionsMu.Lock()
	_, stillTracked := h.clientSessions["done-old"]
	h.clientSessionsMu.Unlock()
	require.False(t, stillTracked, "session done more than doneSessionRetention ago should be pruned from the map")
}

// TestClientSessionsSnapshot_PrunesIdleSessions covers a session that never called
// -github-actions-done (e.g. it crashed, or its mode has no done hook): it should still show as
// idle for a while, then get pruned too, same as a done session eventually does.
func TestClientSessionsSnapshot_PrunesIdleSessions(t *testing.T) {
	h := &Handler{
		clientSessions: map[string]*clientSession{
			"idle-fresh": {
				LastSeen:  time.Now().Add(-sessionIdleTimeout - time.Second),
				FirstSeen: time.Now().Add(-time.Hour),
			},
			"idle-stale": {
				LastSeen:  time.Now().Add(-sessionRetention - time.Second),
				FirstSeen: time.Now().Add(-time.Hour),
			},
		},
	}

	views := h.clientSessionsSnapshot()
	require.Len(t, views, 1)
	require.Equal(t, "idle", views[0].Status)

	h.clientSessionsMu.Lock()
	_, stillTracked := h.clientSessions["idle-stale"]
	h.clientSessionsMu.Unlock()
	require.False(t, stillTracked, "a session idle for more than sessionRetention should be pruned from the map")
}
