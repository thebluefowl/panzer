package main

import (
	"bytes"
	"context"
	"testing"
)

func TestErasure_EncodeDecode(t *testing.T) {
	ctx := context.Background()

	input := []byte("test data")
	erasure, err := NewErasure(10, 2)
	if err != nil {
		t.Errorf("NewErasure() error = %v", err)
		return
	}

	shards, err := erasure.Encode(ctx, input)
	if err != nil {
		t.Errorf("Encode() error = %v", err)
		return
	}

	output, err := erasure.Decode(ctx, shards, len(input))
	if err != nil {
		t.Errorf("Decode() error = %v", err)
		return
	}

	if !bytes.Equal(input, output) {
		t.Errorf("Decode() got = %v, want %v", output, input)
	}
}

func TestErasure_EncodeDecodeCorrupt(t *testing.T) {
	ctx := context.Background()

	input := []byte("test data")
	erasure, err := NewErasure(10, 2)
	if err != nil {
		t.Errorf("NewErasure() error = %v", err)
		return
	}

	shards, err := erasure.Encode(ctx, input)
	if err != nil {
		t.Errorf("Encode() error = %v", err)
		return
	}

	// Corrupt one shard, should still be able to decode.
	shards[0] = nil

	output, err := erasure.Decode(ctx, shards, len(input))
	if err != nil {
		t.Errorf("Decode() error = %v", err)
		return
	}

	if !bytes.Equal(input, output) {
		t.Errorf("Decode() got = %v, want %v", output, input)
	}
}

func TestErasure_EncodeDecodeCorruptTooMany(t *testing.T) {
	ctx := context.Background()

	input := []byte("test data")
	erasure, err := NewErasure(10, 2)
	if err != nil {
		t.Errorf("NewErasure() error = %v", err)
		return
	}

	shards, err := erasure.Encode(ctx, input)
	if err != nil {
		t.Errorf("Encode() error = %v", err)
		return
	}

	// Corrupt one shard, should still be able to decode.
	shards[0] = nil
	shards[1] = nil
	shards[2] = nil

	_, err = erasure.Decode(ctx, shards, len(input))
	if err == nil {
		t.Errorf("Decode() error = %v, want %v", err, ErrEnsureFailedDecode)
		return
	}
}

func BenchmarkEncode(b *testing.B) {
	dataShardsN := 10
	parityShardsN := 4
	inputData := []byte("example data")

	e, err := NewErasure(dataShardsN, parityShardsN)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.Encode(context.Background(), inputData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	dataShardsN := 10
	parityShardsN := 4
	inputData := []byte("example data")

	e, err := NewErasure(dataShardsN, parityShardsN)
	if err != nil {
		b.Fatal(err)
	}

	shards, err := e.Encode(context.Background(), inputData)
	if err != nil {
		b.Fatal(err)
	}

	size := len(inputData)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := e.Decode(context.Background(), shards, size)
		if err != nil {
			b.Fatal(err)
		}
	}
}
