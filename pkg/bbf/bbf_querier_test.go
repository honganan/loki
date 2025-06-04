package bbf

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func TestIntersectChunks(t *testing.T) {
	chunk1 := &logproto.ChunkRef{Fingerprint: 1, UserID: "fake", From: 0, Through: 1, Checksum: 1}
	chunk2 := &logproto.ChunkRef{Fingerprint: 2, UserID: "fake", From: 0, Through: 1, Checksum: 1}
	chunk3 := &logproto.ChunkRef{Fingerprint: 3, UserID: "fake", From: 0, Through: 1, Checksum: 1}
	chunk4 := &logproto.ChunkRef{Fingerprint: 4, UserID: "fake", From: 0, Through: 1, Checksum: 1}

	results := [][]*logproto.ChunkRef{
		{chunk1, chunk2, chunk4},
		{chunk2, chunk3, chunk4},
		{chunk2, chunk4},
	}

	expected := []*logproto.ChunkRef{chunk2, chunk4}
	actual := intersectChunks(results)

	require.Equal(t, expected, actual, "intersectChunks did not return the expected result")
}

func TestUnionChunks(t *testing.T) {
	chunk1 := &logproto.ChunkRef{Fingerprint: 1, UserID: "fake", From: 0, Through: 1, Checksum: 1}
	chunk2 := &logproto.ChunkRef{Fingerprint: 2, UserID: "fake", From: 0, Through: 1, Checksum: 1}
	chunk3 := &logproto.ChunkRef{Fingerprint: 3, UserID: "fake", From: 0, Through: 1, Checksum: 1}

	results := [][]*logproto.ChunkRef{
		{chunk1, chunk2},
		{chunk2, chunk3},
		{chunk1, chunk3},
	}

	expected := []*logproto.ChunkRef{chunk1, chunk2, chunk3}
	actual := unionChunks(results)

	require.ElementsMatch(t, expected, actual, "unionChunks did not return the expected result")
}
