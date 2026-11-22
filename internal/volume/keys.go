package volume

import (
	"encoding/binary"
	"fmt"
)

// SeriesKeySize is the raw 32-byte series identifier used inside every
// persistent key. TimeSuffixSize is the fixed-width lexicographic timestamp.
const (
	SeriesKeySize   = 32
	TimeSuffixSize  = 8
	PointKeySize    = SeriesKeySize + TimeSuffixSize
	legacyHexIDSize = 64
	legacyKeySize   = legacyHexIDSize + TimeSuffixSize
)

func PointKey(series []byte, timestamp int64) []byte {
	key := make([]byte, len(series)+TimeSuffixSize)
	copy(key, series)
	binary.BigEndian.PutUint64(key[len(series):], uint64(timestamp)^(1<<63))
	return key
}

func PointTime(key []byte) (int64, error) {
	if len(key) != PointKeySize {
		return 0, fmt.Errorf("invalid point key length %d", len(key))
	}
	return int64(binary.BigEndian.Uint64(key[SeriesKeySize:]) ^ (1 << 63)), nil
}

// PointSeries returns the raw series key prefix of a point key.
func PointSeries(key []byte) []byte {
	return key[:SeriesKeySize]
}

// LegacyPointTime decodes the time suffix of a pre-migration 72-byte point
// key. It deliberately reads only the trailing 8 bytes so the old head key's
// timestamp is never mistaken for part of the series identifier.
func LegacyPointTime(key []byte) (int64, error) {
	if len(key) != legacyKeySize {
		return 0, fmt.Errorf("invalid legacy point key length %d", len(key))
	}
	return int64(binary.BigEndian.Uint64(key[legacyHexIDSize:]) ^ (1 << 63)), nil
}
