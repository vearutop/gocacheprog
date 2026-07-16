package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestResponseItem_UncompressedBodyReader_SurvivesReadAfterReturnForLargeFile guards against a
// real production incident: for compressed items backed by a real *os.File (WireSize >= 1e6,
// see PrepareBodyReader), UncompressedBodyReader used to close that file via its own defer
// before returning, even though the zstd decoder it hands back still needs to keep pulling
// bytes from it. Reads happening after the call returns - exactly what a caller like
// Store.verifyOutput does - failed with "file already closed". Files under the 1e6 threshold
// never hit this, since PrepareBodyReader reads those fully into memory up front, which is why
// this went unnoticed until a caller decompressed a genuinely large stored object.
func TestResponseItem_UncompressedBodyReader_SurvivesReadAfterReturnForLargeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "object")

	original := []byte(strings.Repeat("hello-world-integrity-check ", 100))
	compressed := zstd.EncodeTo(nil, original)
	require.NoError(t, os.WriteFile(path, compressed, 0o600))

	item := ResponseItem{
		Size:         int64(len(original)),
		WireSize:     1_000_001, // forces PrepareBodyReader's real *os.File path, not its in-memory ReadFile shortcut.
		IsCompressed: true,
		DiskPath:     path,
	}

	rd, err := item.UncompressedBodyReader()
	require.NoError(t, err)
	require.NotNil(t, rd)

	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, original, got)
	require.NoError(t, rd.Close())
}

func makeRawItem(actionID, outputID, body string) ResponseItem {
	item := ResponseItem{ActionID: actionID, OutputID: outputID, Size: int64(len(body)), WireSize: int64(len(body))}
	item.SetBodyReader(func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	})

	return item
}

func TestReaderFrom_RoundTripsWhenReadDrainsBodyItself(t *testing.T) {
	resp := Response{Items: []ResponseItem{
		makeRawItem("a1", "o1", "hello"),
		makeRawItem("a2", "o2", "world!!"),
	}}

	var wire bytes.Buffer
	_, err := resp.WriteTo(&wire)
	require.NoError(t, err)

	buf := wire.Bytes()

	var got Response
	var bodies []string
	n, err := got.ReaderFrom(bytes.NewReader(buf), func(_ ResponseItem, body io.Reader) error {
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}

		bodies = append(bodies, string(b))

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(buf)), n)
	require.Equal(t, []string{"hello", "world!!"}, bodies)
}

func TestReaderFrom_RoundTripsWhenReadLeavesBodyUndrained(t *testing.T) {
	resp := Response{Items: []ResponseItem{
		makeRawItem("a1", "o1", "hello"),
		makeRawItem("a2", "o2", "world!!"),
	}}

	var wire bytes.Buffer
	_, err := resp.WriteTo(&wire)
	require.NoError(t, err)

	buf := wire.Bytes()

	var got Response
	var seen []string
	// This callback deliberately never touches body, mirroring a caller (e.g. Client.Preload's
	// ReaderFrom callback) that only stashes a lazy body reader and returns immediately.
	n, err := got.ReaderFrom(bytes.NewReader(buf), func(item ResponseItem, _ io.Reader) error {
		seen = append(seen, item.ActionID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(buf)), n, "undrained bytes must still be counted so later items stay aligned")
	require.Equal(t, []string{"a1", "a2"}, seen)
}

func TestReaderFrom_AbortsOnShortItemInsteadOfCorruptingLaterItems(t *testing.T) {
	resp := Response{Items: []ResponseItem{
		{ActionID: "a1", OutputID: "o1", Size: 5, WireSize: 5},
		{ActionID: "a2", OutputID: "o2", Size: 10, WireSize: 10},
		{ActionID: "a3", OutputID: "o3", Size: 7, WireSize: 7},
	}}

	jsonData, err := json.Marshal(&resp)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.BigEndian, int32(len(jsonData))))
	buf.Write(jsonData)
	buf.WriteString("hello") // item a1: exactly its declared 5 bytes.
	buf.WriteString("shrt")  // item a2: only 4 of its declared 10 bytes, then the stream ends.
	// No bytes at all for a3: the server's index entry for a2 didn't match what it actually had.

	var got Response
	var seen []string
	_, err = got.ReaderFrom(&buf, func(item ResponseItem, body io.Reader) error {
		seen = append(seen, item.ActionID)
		_, _ = io.ReadAll(body) //nolint:errcheck // best-effort, like a real decompressing consumer; only ReaderFrom's own error matters here.
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrShortRead)
	require.Equal(t, []string{"a1", "a2"}, seen, "a3 must never be handed to read() once the stream has desynced")
}
