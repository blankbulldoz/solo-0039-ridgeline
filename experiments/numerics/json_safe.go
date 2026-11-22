package main

import "encoding/json"

// finiteOrNull encodes NaN/Inf as JSON null so error fields whose denominator
// (the exact result) is zero, and values that overflowed, remain representable.
func finiteOrNull(f float64) any {
	if f != f || f > 1.7976931348623157e+308 || f < -1.7976931348623157e+308 {
		return nil
	}
	return f
}

func (s StrategyResult) MarshalJSON() ([]byte, error) {
	type alias StrategyResult
	return json.Marshal(struct {
		alias
		Value  any `json:"value"`
		Exact  any `json:"exact"`
		AbsErr any `json:"abs_err"`
		RelErr any `json:"rel_err"`
		ULP    any `json:"ulp"`
	}{alias(s), finiteOrNull(s.Value), finiteOrNull(s.Exact),
		finiteOrNull(s.AbsErr), finiteOrNull(s.RelErr), finiteOrNull(s.ULP)})
}

func (o OrderingProbe) MarshalJSON() ([]byte, error) {
	type alias OrderingProbe
	return json.Marshal(struct {
		alias
		SeqValue    any `json:"seq_value"`
		PairValue   any `json:"pairwise_value"`
		NeumaierVal any `json:"neumaier_value"`
		Exact       any `json:"exact"`
		SeqRelErr   any `json:"seq_rel_err"`
		SeqULP      any `json:"seq_ulp"`
	}{alias(o), finiteOrNull(o.SeqValue), finiteOrNull(o.PairValue),
		finiteOrNull(o.NeumaierVal), finiteOrNull(o.Exact),
		finiteOrNull(o.SeqRelErr), finiteOrNull(o.SeqULP)})
}

func (c RateCase) MarshalJSON() ([]byte, error) {
	type alias RateCase
	return json.Marshal(struct {
		alias
		Production  any `json:"production_value"`
		Compensated any `json:"neumaier_value"`
		Exact       any `json:"exact_value"`
		RelErr      any `json:"production_rel_err"`
		ULP         any `json:"production_ulp"`
	}{alias(c), finiteOrNull(c.Production), finiteOrNull(c.Compensated),
		finiteOrNull(c.Exact), finiteOrNull(c.RelErr), finiteOrNull(c.ULP)})
}

func (c OverflowCase) MarshalJSON() ([]byte, error) {
	type alias OverflowCase
	return json.Marshal(struct {
		alias
		Seq      any `json:"seq"`
		Pairwise any `json:"pairwise"`
		Neumaier any `json:"neumaier"`
		Exact    any `json:"exact"`
	}{alias(c), finiteOrNull(c.Seq), finiteOrNull(c.Pairwise),
		finiteOrNull(c.Neumaier), finiteOrNull(c.Exact)})
}
