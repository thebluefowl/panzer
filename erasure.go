package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/klauspost/reedsolomon"
)

var ErrEnsureFailedDecode = errors.New("failed to decode data from shards")

type Erasure struct {
	enc reedsolomon.Encoder

	dataShardsN, parityShardsN int
}

func NewErasure(dataShardsN, parityShardsN int) (*Erasure, error) {
	if dataShardsN <= 0 || parityShardsN <= 0 || dataShardsN+parityShardsN > 256 {
		return nil, errors.New("invalid data/parity shards")
	}

	enc, err := reedsolomon.New(dataShardsN, parityShardsN, reedsolomon.WithAutoGoroutines(dataShardsN+parityShardsN))
	if err != nil {
		return nil, err
	}

	return &Erasure{
		dataShardsN:   dataShardsN,
		parityShardsN: parityShardsN,

		enc: enc,
	}, nil
}

func (e *Erasure) Encode(ctx context.Context, b []byte) ([][]byte, error) {
	// Parse command line parameters.
	shards, err := e.enc.Split(b)
	if err != nil {
		return nil, err
	}

	// Encode parity
	err = e.enc.Encode(shards)
	if err != nil {
		return nil, err
	}
	return shards, nil
}

func (e *Erasure) Decode(ctx context.Context, shards [][]byte, size int) ([]byte, error) {
	ok, _ := e.enc.Verify(shards)
	if !ok {
		if err := e.enc.Reconstruct(shards); err != nil {
			return nil, err
		}
		ok, err := e.enc.Verify(shards)
		if !ok {
			return nil, ErrEnsureFailedDecode
		}
		if err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := e.enc.Join(&buf, shards, size); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
