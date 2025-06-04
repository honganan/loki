package bbf

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomFilterFalsePositive(t *testing.T) {
	tokens := make([]string, 2000_000)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token_%d", i)
	}
	data := bloomFilterMarshal(nil, tokens)
	bf := getBloomFilter()
	defer putBloomFilter(bf)
	if err := bf.unmarshal(data); err != nil {
		t.Fatalf("unexpected error when unmarshaling bloom filter: %s", err)
	}

	// count the number of false positives on 20K non-existing tokens
	falsePositives := 0
	for i := range tokens {
		token := fmt.Sprintf("non-existing-token_%d", i)
		if bf.containsAll([]string{token}) {
			falsePositives++
		}
	}
	p := float64(falsePositives) / float64(len(tokens))
	maxFalsePositive := 0.0011
	if p > maxFalsePositive {
		t.Fatalf("too high false positive rate; got %.4f; want %.4f max", p, maxFalsePositive)
	}
}

func TestBloomFilterUnmarshal(t *testing.T) {
	// Initialize a bloom filter with some tokens
	tokens := []string{"token1", "token2", "token3", "ay1678162880420jogo7fake/5b503fffc65d8139/191d4991bff:191d4991bff:442d0320"}
	bf := NewBloomFilter(100, 0)
	bf.Add(tokens)

	// Marshal the bloom filter
	var dst []byte
	dst = bf.marshal(dst)

	// Create a new bloom filter and unmarshal the data into it
	newBf := &bloomFilter{}
	err := newBf.unmarshal(dst)
	require.NoError(t, err)

	// Verify that the unmarshaled bloom filter contains the same tokens
	require.True(t, newBf.containsAll(tokens))
	require.Equal(t, bf.idx, newBf.idx)
	require.Equal(t, bf.appended, newBf.appended)
	require.Equal(t, bf.bits, newBf.bits)
}

func BenchmarkBloomFilterUnmarshal(b *testing.B) {
	tokens := []string{"token1", "token2", "token3", "ay1678162880420jogo7fake/5b503fffc65d8139/191d4991bff:191d4991bff:442d0320"}
	bf := NewBloomFilter(1000000, 0)
	bf.Add(tokens)

	// Marshal the bloom filter
	var dst []byte
	dst = bf.marshal(dst)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBf := getBloomFilter()
		err := newBf.unmarshal(dst)
		if err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
		require.True(b, newBf.containsAll(tokens))
		require.Equal(b, bf.idx, newBf.idx)
		require.Equal(b, bf.appended, newBf.appended)
		require.Equal(b, bf.bits, newBf.bits)

		putBloomFilter(newBf)
	}
}
