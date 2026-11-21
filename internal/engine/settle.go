package engine

import (
	"ridgeline/internal/model"
	"ridgeline/internal/settle"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// Settle resolves counter increments per device from one read view: count
// and boot evidence share corrections, deletions, the retention floor and
// the database sequence inside a single bbolt read transaction.
func (e *Engine) Settle(name string, req model.SettleRequest) (model.SettleResult, error) {
	var out model.SettleResult
	if err := model.ValidateSettle(req); err != nil {
		return out, err
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		sequence := volume.Current(tx)
		counts, err := scan(b, n, model.Query{
			Start: req.Start, End: req.End, Matchers: req.CountMatchers,
		}, sequence)
		if err != nil {
			return err
		}
		// Boot evidence is not clipped to the settlement window: the
		// observations covering an interval may sit outside it.
		boots, err := scan(b, n, model.Query{
			Start: model.MinTime, End: model.MaxTime + 1, Matchers: req.BootMatchers,
		}, sequence)
		if err != nil {
			return err
		}
		decoded := counts.Stats.Decoded + boots.Stats.Decoded
		if decoded > n.MaxScan {
			return model.Capacity("settlement decoded point budget exceeded")
		}
		groups, err := settle.Groups(counts.Series, boots.Series, req)
		if err != nil {
			return err
		}
		stats := counts.Stats
		stats.Series += boots.Stats.Series
		stats.Blocks += boots.Stats.Blocks
		stats.Head += boots.Stats.Head
		stats.Decoded = decoded
		stats.Visible += boots.Stats.Visible
		out = model.SettleResult{
			Sequence: sequence, Floor: n.Floor, Groups: groups, Stats: stats,
		}
		return nil
	})
	return out, err
}
