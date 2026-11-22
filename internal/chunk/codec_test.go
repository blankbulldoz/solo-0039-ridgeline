package chunk_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"ridgeline/internal/chunk"
	"ridgeline/internal/model"
)

// edgeSamples covers the losslessness contract: boundary timestamps,
// exact float64 bit patterns and arbitrary observation sequences.
func edgeSamples() []model.Sample {
	return []model.Sample{
		{Time: model.MinTime, Value: math.Copysign(0, -1), Sequence: 0},
		{Time: -1, Value: math.SmallestNonzeroFloat64, Sequence: math.MaxUint64},
		{Time: 0, Value: 0, Sequence: 1},
		{Time: 1, Value: -math.MaxFloat64, Sequence: 42},
		{Time: 1500, Value: 0.1 + 0.2, Sequence: 41},
		{Time: model.MaxTime, Value: math.MaxFloat64, Sequence: math.MaxUint64 - 1},
	}
}

func blockFor(points []model.Sample, format int) (model.Block, []byte) {
	payload, checksum, err := chunk.Encode(points, format)
	if err != nil {
		panic(err)
	}
	return model.Block{
		ID: "test-block", Start: points[0].Time, End: points[len(points)-1].Time + 1,
		Count: len(points), Bytes: len(payload), Checksum: checksum, Format: format,
	}, payload
}

func identical(a, b []model.Sample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Time != b[i].Time || a[i].Sequence != b[i].Sequence ||
			math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}

func TestRoundTripLossless(t *testing.T) {
	points := edgeSamples()
	for _, format := range []int{chunk.FormatV1, chunk.FormatV2} {
		block, payload := blockFor(points, format)
		decoded, err := chunk.Decode(payload, block)
		if err != nil {
			t.Fatalf("format %d decode: %v", format, err)
		}
		if !identical(points, decoded) {
			t.Fatalf("format %d round trip changed samples: %v != %v", format, decoded, points)
		}
	}
}

func TestFormatsAgree(t *testing.T) {
	points := edgeSamples()
	v1Block, v1Payload := blockFor(points, chunk.FormatV1)
	v2Block, v2Payload := blockFor(points, chunk.FormatV2)
	fromV1, err := chunk.Decode(v1Payload, v1Block)
	if err != nil {
		t.Fatal(err)
	}
	fromV2, err := chunk.Decode(v2Payload, v2Block)
	if err != nil {
		t.Fatal(err)
	}
	if !identical(fromV1, fromV2) {
		t.Fatal("v1 and v2 decodes disagree")
	}
}

func TestV2IsCompact(t *testing.T) {
	points := edgeSamples()
	_, v1Payload := blockFor(points, chunk.FormatV1)
	_, v2Payload := blockFor(points, chunk.FormatV2)
	if len(v2Payload) >= len(v1Payload) {
		t.Fatalf("v2 payload %d bytes not smaller than v1 %d bytes", len(v2Payload), len(v1Payload))
	}
}

func TestUnknownFormatFails(t *testing.T) {
	points := edgeSamples()
	block, payload := blockFor(points, chunk.FormatV2)
	block.Format = 7
	if _, err := chunk.Decode(payload, block); err == nil ||
		!strings.Contains(err.Error(), "unsupported block format 7") {
		t.Fatalf("unknown format must fail clearly, got %v", err)
	}
	if _, _, err := chunk.Encode(points, 9); err == nil {
		t.Fatal("encoding an unknown format must fail")
	}
}

func TestChecksumMismatchFails(t *testing.T) {
	points := edgeSamples()
	for _, format := range []int{chunk.FormatV1, chunk.FormatV2} {
		block, payload := blockFor(points, format)
		payload[len(payload)-1] ^= 0xff
		if _, err := chunk.Decode(payload, block); err == nil ||
			!strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("format %d corrupted payload must fail on checksum, got %v", format, err)
		}
	}
}

func TestV2TruncationFails(t *testing.T) {
	points := edgeSamples()
	block, payload := blockFor(points, chunk.FormatV2)
	for _, cut := range []int{0, 3, 8, len(payload) / 2, len(payload) - 1} {
		truncated := payload[:cut]
		block.Checksum = checksumOf(truncated)
		if _, err := chunk.Decode(truncated, block); err == nil {
			t.Fatalf("truncated payload of %d bytes must fail", cut)
		}
	}
}

func TestV2TrailingBytesFail(t *testing.T) {
	points := edgeSamples()
	block, payload := blockFor(points, chunk.FormatV2)
	padded := append(append([]byte{}, payload...), 0)
	block.Checksum = checksumOf(padded)
	if _, err := chunk.Decode(padded, block); err == nil ||
		!strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing garbage must fail, got %v", err)
	}
}

func TestV2FlagsFail(t *testing.T) {
	points := edgeSamples()
	block, payload := blockFor(points, chunk.FormatV2)
	payload[3] = 1
	block.Checksum = checksumOf(payload)
	if _, err := chunk.Decode(payload, block); err == nil ||
		!strings.Contains(err.Error(), "unsupported v2 flags") {
		t.Fatalf("reserved flags must fail, got %v", err)
	}
}

func TestV2CountMismatchFails(t *testing.T) {
	points := edgeSamples()
	block, payload := blockFor(points, chunk.FormatV2)
	block.Count++
	if _, err := chunk.Decode(payload, block); err == nil ||
		!strings.Contains(err.Error(), "count mismatch") {
		t.Fatalf("count mismatch must fail, got %v", err)
	}
}

// craftedV2 builds a v2 payload by hand for cases the encoder never emits.
func craftedV2(times []int64, values []uint64, sequences []uint64) []byte {
	out := []byte{'R', 'B', chunk.FormatV2, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(out[4:8], uint32(len(times)))
	var scratch [binary.MaxVarintLen64]byte
	var value [8]byte
	for i := range times {
		if i == 0 {
			n := binary.PutVarint(scratch[:], times[i])
			out = append(out, scratch[:n]...)
			n = binary.PutUvarint(scratch[:], sequences[i])
			out = append(out, scratch[:n]...)
		} else {
			n := binary.PutUvarint(scratch[:], uint64(times[i]-times[i-1]))
			out = append(out, scratch[:n]...)
			n = binary.PutVarint(scratch[:], int64(sequences[i]-sequences[i-1]))
			out = append(out, scratch[:n]...)
		}
		binary.LittleEndian.PutUint64(value[:], values[i])
		out = append(out, value[:]...)
	}
	return out
}

func TestV2NonFiniteValueFails(t *testing.T) {
	payload := craftedV2([]int64{1, 2}, []uint64{math.Float64bits(math.NaN()), math.Float64bits(1)}, []uint64{1, 2})
	block := model.Block{
		ID: "crafted", Start: 1, End: 3, Count: 2,
		Checksum: checksumOf(payload), Format: chunk.FormatV2,
	}
	if _, err := chunk.Decode(payload, block); err == nil ||
		!strings.Contains(err.Error(), "non-finite") {
		t.Fatalf("non-finite value must fail, got %v", err)
	}
}

func TestV2UnorderedPayloadFails(t *testing.T) {
	payload := craftedV2([]int64{5, 5}, []uint64{math.Float64bits(1), math.Float64bits(2)}, []uint64{1, 2})
	block := model.Block{
		ID: "crafted", Start: 5, End: 6, Count: 2,
		Checksum: checksumOf(payload), Format: chunk.FormatV2,
	}
	if _, err := chunk.Decode(payload, block); err == nil ||
		!strings.Contains(err.Error(), "order or bounds") {
		t.Fatalf("duplicate timestamps must fail, got %v", err)
	}
}

func TestBuildRejectsEmpty(t *testing.T) {
	if _, _, err := chunk.Build("series", 1, nil, chunk.FormatV2); err == nil {
		t.Fatal("empty block must fail")
	}
}

func checksumOf(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
