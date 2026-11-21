package volume

import (
	"encoding/binary"
	"fmt"
)

func PointKey(series string, timestamp int64) []byte {
	key := make([]byte, len(series)+8)
	copy(key, series)
	binary.BigEndian.PutUint64(key[len(series):], uint64(timestamp)^(1<<63))
	return key
}

func PointTime(key []byte) (int64, error) {
	if len(key) != 72 {
		return 0, fmt.Errorf("invalid point key length %d", len(key))
	}
	return int64(binary.BigEndian.Uint64(key[64:]) ^ (1 << 63)), nil
}

// ExemplarKey orders exemplars by series, then sample time, then commit
// sequence and finally in-batch offset. The ordering makes one series-time
// hold many exemplars while keeping time-ordered cursor scans natural.
func ExemplarKey(series string, timestamp int64, sequence uint64, offset int) []byte {
	key := make([]byte, len(series)+8+8+4)
	copy(key, series)
	binary.BigEndian.PutUint64(key[len(series):], uint64(timestamp)^(1<<63))
	binary.BigEndian.PutUint64(key[len(series)+8:], sequence)
	binary.BigEndian.PutUint32(key[len(series)+16:], uint32(offset))
	return key
}

func ExemplarKeyTime(key []byte) (int64, error) {
	if len(key) != 84 {
		return 0, fmt.Errorf("invalid exemplar key length %d", len(key))
	}
	return int64(binary.BigEndian.Uint64(key[64:]) ^ (1 << 63)), nil
}

func ExemplarKeySeries(key []byte) (string, error) {
	if len(key) != 84 {
		return "", fmt.Errorf("invalid exemplar key length %d", len(key))
	}
	return string(key[:64]), nil
}
