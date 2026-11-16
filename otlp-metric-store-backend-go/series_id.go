package main

import (
	"cmp"
	"encoding/binary"
	"math"
	"math/big"
	"slices"

	"github.com/zeebo/xxh3"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// SeriesID identifies a series: everything describing a data point except its timestamps, value and flags.
type SeriesID xxh3.Uint128

// BigInt returns the ID in the form the ClickHouse driver accepts for UInt128 columns.
func (id SeriesID) BigInt() *big.Int {
	b := xxh3.Uint128(id).Bytes()
	return new(big.Int).SetBytes(b[:])
}

const (
	valueEmpty byte = iota
	valueString
	valueBool
	valueInt
	valueDouble
	valueBytes
	valueArray
	valueKvList
)

// identityEncoder builds the canonical byte form of a series identity. Every variable-length field is
// length-prefixed and every value type-tagged, so different identities can never encode to the same bytes.
type identityEncoder struct {
	buf []byte
}

func (e *identityEncoder) reset() {
	e.buf = e.buf[:0]
}

func (e *identityEncoder) sum() SeriesID {
	return SeriesID(xxh3.Hash128(e.buf))
}

func (e *identityEncoder) id(id SeriesID) {
	b := xxh3.Uint128(id).Bytes()
	e.buf = append(e.buf, b[:]...)
}

func (e *identityEncoder) uint(v uint64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}

func (e *identityEncoder) bool(v bool) {
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

func (e *identityEncoder) string(s string) {
	e.buf = binary.AppendUvarint(e.buf, uint64(len(s)))
	e.buf = append(e.buf, s...)
}

// attributes encodes key-value pairs sorted by key, since senders don't agree on attribute order.
func (e *identityEncoder) attributes(kvs []*commonpb.KeyValue) {
	sorted := slices.SortedFunc(slices.Values(kvs), func(a, b *commonpb.KeyValue) int {
		return cmp.Compare(a.GetKey(), b.GetKey())
	})
	e.buf = binary.AppendUvarint(e.buf, uint64(len(sorted)))
	for _, kv := range sorted {
		e.string(kv.GetKey())
		e.value(kv.GetValue())
	}
}

func (e *identityEncoder) value(v *commonpb.AnyValue) {
	switch v := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		e.buf = append(e.buf, valueString)
		e.string(v.StringValue)
	case *commonpb.AnyValue_BoolValue:
		e.buf = append(e.buf, valueBool)
		e.bool(v.BoolValue)
	case *commonpb.AnyValue_IntValue:
		e.buf = append(e.buf, valueInt)
		e.uint(uint64(v.IntValue))
	case *commonpb.AnyValue_DoubleValue:
		e.buf = append(e.buf, valueDouble)
		e.uint(math.Float64bits(v.DoubleValue))
	case *commonpb.AnyValue_BytesValue:
		e.buf = append(e.buf, valueBytes)
		e.string(string(v.BytesValue))
	case *commonpb.AnyValue_ArrayValue:
		e.buf = append(e.buf, valueArray)
		values := v.ArrayValue.GetValues()
		e.buf = binary.AppendUvarint(e.buf, uint64(len(values)))
		for _, item := range values {
			e.value(item)
		}
	case *commonpb.AnyValue_KvlistValue:
		e.buf = append(e.buf, valueKvList)
		e.attributes(v.KvlistValue.GetValues())
	default:
		e.buf = append(e.buf, valueEmpty)
	}
}
