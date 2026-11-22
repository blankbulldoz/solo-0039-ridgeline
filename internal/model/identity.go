package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// SeriesID is the external, wire-facing series identifier: the lowercase
// hexadecimal rendering of the 32-byte canonical label digest.
func SeriesID(labels Labels) string {
	return hex.EncodeToString(SeriesKey(labels))
}

// SeriesKey is the internal, storage-facing series identifier: the raw
// 32-byte canonical label digest. Persistent keys use this form; HTTP payloads
// continue to expose SeriesID.
func SeriesKey(labels Labels) []byte {
	b, _ := json.Marshal(labels)
	hash := sha256.Sum256(b)
	return hash[:]
}

// EncodeSeriesKey converts an external 64-character hexadecimal series ID
// into its internal 32-byte key.
func EncodeSeriesKey(id string) ([]byte, error) {
	raw, err := hex.DecodeString(id)
	if err != nil || len(raw) != 32 {
		return nil, Bad("invalid stored series identifier")
	}
	return raw, nil
}

// DecodeSeriesKey renders an internal 32-byte key as the external hex ID.
func DecodeSeriesKey(key []byte) string {
	return hex.EncodeToString(key)
}
