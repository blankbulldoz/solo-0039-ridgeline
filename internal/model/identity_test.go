package model

import "testing"

func TestSeriesIdentityRoundTrip(t *testing.T) {
	cases := []Labels{
		{"__name__": "load_ratio", "zone": "z-a"},
		{"__name__": "load_ratio", "zone": "z-a", "rack": ""},
		{"__name__": "load_ratio", "zone": "z-a", "rack": "r1"},
	}
	for _, labels := range cases {
		hexID := SeriesID(labels)
		if len(hexID) != 64 {
			t.Fatalf("external id must stay 64 hex chars, got %d", len(hexID))
		}
		raw := SeriesKey(labels)
		if len(raw) != 32 {
			t.Fatalf("internal key must be 32 bytes, got %d", len(raw))
		}
		encoded, err := EncodeSeriesKey(hexID)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(raw) {
			t.Fatal("EncodeSeriesKey does not match SeriesKey")
		}
		if DecodeSeriesKey(raw) != hexID {
			t.Fatal("DecodeSeriesKey does not round-trip")
		}
	}
}

func TestMissingAndEmptyLabelsStayDistinct(t *testing.T) {
	missing := Labels{"__name__": "m", "zone": "z"}
	empty := Labels{"__name__": "m", "zone": "z", "rack": ""}
	if SeriesID(missing) == SeriesID(empty) {
		t.Fatal("absent and present-empty labels must produce distinct identities")
	}
}

func TestEncodeSeriesKeyRejectsLegacyShapes(t *testing.T) {
	for _, bad := range []string{
		"",
		"zz",     // invalid hex
		"abcdef", // wrong length
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef00", // 65 chars
	} {
		if _, err := EncodeSeriesKey(bad); err == nil {
			t.Fatalf("expected rejection for %q", bad)
		}
	}
}
