package chunk

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"ridgeline/internal/model"
)

// Block payload formats. Format 1 is the legacy gzip stream of an ordered
// JSON sample array. Format 2 is the compact binary encoding:
//
//	header:  'R' 'B' | version(0x02) | flags(must be 0) | count uint32 LE
//	sample:  time | value | sequence
//	  time:     first sample zigzag varint, then uvarint deltas (strictly
//	            increasing timestamps)
//	  value:    8 bytes little-endian IEEE-754 bits
//	  sequence: first sample uvarint, then zigzag varint deltas
//
// Both formats store SHA-256 over the encoded payload in the block record.
// Timestamps, float64 bit patterns and observation sequences round-trip
// exactly. A database may hold both formats at once; reads dispatch on the
// block record's format field.
const (
	FormatV1 = 1
	FormatV2 = 2
)

// Latest is the format new blocks are written in unless configured otherwise.
const Latest = FormatV2

const v2Header = 8

// Supported reports whether format is a known block payload format.
func Supported(format int) bool {
	return format == FormatV1 || format == FormatV2
}

// Encode serializes points in the requested format and returns the payload
// with its SHA-256 checksum.
func Encode(points []model.Sample, format int) ([]byte, string, error) {
	var payload []byte
	switch format {
	case FormatV1:
		raw, err := json.Marshal(points)
		if err != nil {
			return nil, "", err
		}
		var out bytes.Buffer
		writer := gzip.NewWriter(&out)
		if _, err := writer.Write(raw); err != nil {
			return nil, "", err
		}
		if err := writer.Close(); err != nil {
			return nil, "", err
		}
		payload = out.Bytes()
	case FormatV2:
		payload = encodeV2(points)
	default:
		return nil, "", fmt.Errorf("unsupported block format %d", format)
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

// Decode verifies and parses a stored payload according to the format
// recorded on the block. Unknown formats, checksum mismatches and malformed
// payloads fail with the block ID.
func Decode(payload []byte, block model.Block) ([]model.Sample, error) {
	if !Supported(block.Format) {
		return nil, fmt.Errorf("unsupported block format %d", block.Format)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != block.Checksum {
		return nil, fmt.Errorf("block %s checksum mismatch", block.ID)
	}
	var points []model.Sample
	var err error
	switch block.Format {
	case FormatV1:
		points, err = decodeV1(payload, block)
	case FormatV2:
		points, err = decodeV2(payload, block)
	}
	if err != nil {
		return nil, err
	}
	return validate(points, block)
}

func decodeV1(payload []byte, block model.Block) ([]model.Sample, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("block %s corrupt gzip stream: %w", block.ID, err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("block %s corrupt gzip stream: %w", block.ID, err)
	}
	var points []model.Sample
	if err := json.Unmarshal(raw, &points); err != nil {
		return nil, fmt.Errorf("block %s invalid sample encoding: %w", block.ID, err)
	}
	return points, nil
}

func encodeV2(points []model.Sample) []byte {
	out := make([]byte, 0, v2Header+len(points)*16)
	out = append(out, 'R', 'B', FormatV2, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(out[4:v2Header], uint32(len(points)))
	var scratch [binary.MaxVarintLen64]byte
	var value [8]byte
	var previousTime int64
	var previousSequence uint64
	for i, p := range points {
		if i == 0 {
			n := binary.PutVarint(scratch[:], p.Time)
			out = append(out, scratch[:n]...)
			n = binary.PutUvarint(scratch[:], p.Sequence)
			out = append(out, scratch[:n]...)
		} else {
			n := binary.PutUvarint(scratch[:], uint64(p.Time-previousTime))
			out = append(out, scratch[:n]...)
			// Subtraction and conversion wrap mod 2^64, so any pair of
			// uint64 sequences round-trips exactly.
			n = binary.PutVarint(scratch[:], int64(p.Sequence-previousSequence))
			out = append(out, scratch[:n]...)
		}
		binary.LittleEndian.PutUint64(value[:], math.Float64bits(p.Value))
		out = append(out, value[:]...)
		previousTime, previousSequence = p.Time, p.Sequence
	}
	return out
}

func decodeV2(payload []byte, block model.Block) ([]model.Sample, error) {
	if len(payload) < v2Header || payload[0] != 'R' || payload[1] != 'B' {
		return nil, fmt.Errorf("block %s invalid v2 header", block.ID)
	}
	if payload[2] != FormatV2 {
		return nil, fmt.Errorf("block %s v2 payload version %d does not match record", block.ID, payload[2])
	}
	if payload[3] != 0 {
		return nil, fmt.Errorf("block %s unsupported v2 flags %d", block.ID, payload[3])
	}
	count := binary.LittleEndian.Uint32(payload[4:v2Header])
	body := payload[v2Header:]
	// Every sample needs at least a 1-byte time field, 8 value bytes and a
	// 1-byte sequence field; reject impossible counts before allocating.
	if uint64(count)*10 > uint64(len(body)) {
		return nil, fmt.Errorf("block %s truncated v2 payload", block.ID)
	}
	points := make([]model.Sample, 0, count)
	var previousTime int64
	var previousSequence uint64
	for i := 0; i < int(count); i++ {
		var sample model.Sample
		if i == 0 {
			t, n := binary.Varint(body)
			if n <= 0 {
				return nil, fmt.Errorf("block %s corrupt v2 timestamp", block.ID)
			}
			sample.Time, body = t, body[n:]
			s, n := binary.Uvarint(body)
			if n <= 0 {
				return nil, fmt.Errorf("block %s corrupt v2 sequence", block.ID)
			}
			sample.Sequence, body = s, body[n:]
		} else {
			delta, n := binary.Uvarint(body)
			if n <= 0 {
				return nil, fmt.Errorf("block %s corrupt v2 timestamp delta", block.ID)
			}
			// An overflowing delta wraps the sum below the previous time and
			// is rejected by the strict ordering check in validate.
			sample.Time, body = previousTime+int64(delta), body[n:]
			deltaSeq, n := binary.Varint(body)
			if n <= 0 {
				return nil, fmt.Errorf("block %s corrupt v2 sequence delta", block.ID)
			}
			sample.Sequence, body = previousSequence+uint64(deltaSeq), body[n:]
		}
		if len(body) < 8 {
			return nil, fmt.Errorf("block %s truncated v2 payload", block.ID)
		}
		sample.Value = math.Float64frombits(binary.LittleEndian.Uint64(body[:8]))
		body = body[8:]
		points = append(points, sample)
		previousTime, previousSequence = sample.Time, sample.Sequence
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("block %s v2 payload has %d trailing bytes", block.ID, len(body))
	}
	return points, nil
}

func validate(points []model.Sample, block model.Block) ([]model.Sample, error) {
	if len(points) != block.Count {
		return nil, fmt.Errorf("block %s count mismatch", block.ID)
	}
	for i, p := range points {
		if !model.ValidTime(p.Time) || p.Time < block.Start || p.Time >= block.End ||
			(i > 0 && points[i-1].Time >= p.Time) {
			return nil, fmt.Errorf("block %s invalid point order or bounds", block.ID)
		}
		if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
			return nil, fmt.Errorf("block %s non-finite sample value", block.ID)
		}
	}
	return points, nil
}
