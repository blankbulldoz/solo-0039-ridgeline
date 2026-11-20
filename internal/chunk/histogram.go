package chunk

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"ridgeline/internal/model"
)

// HistogramFormat uses the same gzip + SHA-256 envelope as Format, but the
// JSON payload is an ordered cumulative histogram sample array.
const HistogramFormat = 2

func EncodeHistograms(points []model.HistogramSample) ([]byte, string, error) {
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
	digest := sha256.Sum256(out.Bytes())
	return out.Bytes(), hex.EncodeToString(digest[:]), nil
}

func DecodeHistograms(payload []byte, block model.Block) ([]model.HistogramSample, error) {
	if block.Format != HistogramFormat {
		return nil, fmt.Errorf("unsupported histogram block format %d", block.Format)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != block.Checksum {
		return nil, fmt.Errorf("histogram block %s checksum mismatch", block.ID)
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 64<<20))
	if err != nil {
		return nil, err
	}
	var points []model.HistogramSample
	if err := json.Unmarshal(raw, &points); err != nil {
		return nil, err
	}
	if len(points) != block.Count {
		return nil, fmt.Errorf("histogram block %s count mismatch", block.ID)
	}
	for i, p := range points {
		if err := model.ValidateHistogramSample(p); err != nil {
			return nil, fmt.Errorf("histogram block %s invalid sample: %w", block.ID, err)
		}
		if !model.ValidTime(p.Time) || p.Time < block.Start || p.Time >= block.End ||
			(i > 0 && points[i-1].Time >= p.Time) {
			return nil, fmt.Errorf("histogram block %s invalid order or bounds", block.ID)
		}
	}
	return points, nil
}

func BuildHistograms(series string, seq uint64, points []model.HistogramSample) (model.Block, []byte, error) {
	if len(points) == 0 {
		return model.Block{}, nil, fmt.Errorf("cannot build empty histogram block")
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time < points[j].Time })
	payload, checksum, err := EncodeHistograms(points)
	if err != nil {
		return model.Block{}, nil, err
	}
	block := model.Block{
		ID:       fmt.Sprintf("%020d-h-%s", seq, series),
		SeriesID: series, Start: points[0].Time,
		End: points[len(points)-1].Time + 1, Count: len(points),
		Bytes: len(payload), Checksum: checksum, Format: HistogramFormat,
	}
	return block, payload, nil
}
