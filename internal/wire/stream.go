package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
)

// streamingRequest reports whether a client asked for incremental aggregate
// output. The same route keeps its ordinary single-JSON response otherwise.
func streamingRequest(r *http.Request) bool {
	if r.URL.Query().Get("stream") == "sse" {
		return true
	}
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

// streamSink serializes named server-sent events. The write deadline is
// refreshed per event so an active stream is not cut off by the server's
// ordinary response deadline.
type streamSink struct {
	w    http.ResponseWriter
	rc   *http.ResponseController
	done bool
}

func (s *streamSink) start(sequence uint64) {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	h.Set("X-Ridge-Sequence", strconv.FormatUint(sequence, 10))
	s.rc = http.NewResponseController(s.w)
	s.done = true
}

func (s *streamSink) send(event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_ = s.rc.SetWriteDeadline(time.Now().Add(60 * time.Second))
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return err
	}
	return s.rc.Flush()
}

type streamDone struct {
	Sequence uint64          `json:"sequence"`
	Windows  int             `json:"windows"`
	Complete bool            `json:"complete"`
	Stats    model.ScanStats `json:"stats"`
}

type streamFailure struct {
	Code     string `json:"code"`
	Detail   string `json:"detail"`
	Sequence uint64 `json:"sequence"`
	Windows  int    `json:"windows"`
	Complete bool   `json:"complete"`
}

// serveAggregateStream publishes one window at a time in ascending time
// order over a single read view. Windows are final when sent, so a later
// decode or scan-budget failure cannot retract them; it only makes the stream
// end with an error event instead of the completion event.
func serveAggregateStream(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	var q model.Query
	if err := decode(w, r, &q); err != nil {
		respond(w, nil, err)
		return
	}
	space := r.PathValue("space")
	sink := &streamSink{w: w}
	windows := 0
	summary, err := e.AggregateStream(r.Context(), space, q, func(batch engine.WindowBatch) error {
		if !sink.done {
			sink.start(batch.Sequence)
			if err := sink.send("open", map[string]any{
				"sequence": batch.Sequence, "start": q.Start, "end": q.End, "step": q.Step,
			}); err != nil {
				return err
			}
		}
		windows++
		return sink.send("window", batch)
	})
	if err == nil {
		if !sink.done {
			sink.start(summary.Sequence)
			if err := sink.send("open", map[string]any{
				"sequence": summary.Sequence, "start": q.Start, "end": q.End, "step": q.Step,
			}); err != nil {
				return
			}
		}
		if err := sink.send("done", streamDone{
			Sequence: summary.Sequence, Windows: windows, Complete: true, Stats: summary.Stats,
		}); err != nil {
			log.Printf("aggregate stream write failed after %d windows: %v", windows, err)
		}
		return
	}
	if !sink.done {
		// Nothing was published yet, so ordinary JSON error semantics apply.
		respond(w, nil, err)
		return
	}
	if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
		log.Printf("aggregate stream cancelled after %d windows: %v", windows, err)
		return
	}
	failure := streamFailure{Sequence: summary.Sequence, Windows: windows, Complete: false}
	var domain *model.Error
	if errors.As(err, &domain) {
		failure.Code, failure.Detail = domain.Code, domain.Detail
	} else {
		failure.Code, failure.Detail = "internal", "stream ended before all windows were published"
	}
	if sendErr := sink.send("error", failure); sendErr != nil {
		log.Printf("aggregate stream write failed after %d windows: %v", windows, sendErr)
	}
	log.Printf("aggregate stream failed after %d windows: %v", windows, err)
}
