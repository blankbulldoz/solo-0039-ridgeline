"""Piecewise linear calibration view workflow.

Demonstrates, against a real running service:

* raw observations are never overwritten by a calibration view;
* coefficients are selected purely by observation timestamp, so late data
  arriving out of order is calibrated with the segment effective at its
  observation time;
* half-open [start, end) intervals join at boundaries without overlap, and
  time ranges with no segment pass the raw value through explicitly;
* correcting an old coefficient changes calibrated history and materialized
  rollups while the raw range stays byte-for-byte identical;
* every calibrated point is traceable to its raw value and coefficients.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from walkthrough import launch, request, stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
FLOW = "calibration"

NAMESPACE = {
    "name": "lab",
    "retention_ms": 100000,
    "lateness_ms": 300000,
    "future_ms": 1000,
    "max_series": 100,
    "max_write": 10000,
    "max_scan": 100000,
}

SENSOR = {"__name__": "probe_celsius", "node": "thermo-1", "zone": "bench"}

# Old factory coefficients: calibrated = 1.0 * raw + 0.0 on [0, 2000).
OLD_RULE = {
    "matchers": [{"label": "__name__", "op": "=", "value": "probe_celsius"}],
    "segments": [
        {"start": 0, "end": 2000, "factor": 1.0, "offset": 0.0},
        {"start": 2000, "end": 10000, "factor": 1.1, "offset": 2.0},
    ],
}

# Corrected coefficients: the pre-2000 sensor actually read 20% low and had
# a +1 bias. Post-2000 coefficients are left untouched.
CORRECTED_RULE = {
    "matchers": [{"label": "__name__", "op": "=", "value": "probe_celsius"}],
    "segments": [
        {"start": 0, "end": 2000, "factor": 1.2, "offset": 1.0},
        {"start": 2000, "end": 10000, "factor": 1.1, "offset": 2.0},
    ],
}


def expect(condition, detail):
    if not condition:
        raise RuntimeError(f"calibration workflow failed: {detail}")


def exercise_calibration(observations):
    with tempfile.TemporaryDirectory(prefix="ridgeline-cal-") as scratch:
        scratch = Path(scratch)
        binary = scratch / "ridgeline"
        subprocess.run(
            ["go", "build", "-o", str(binary), "./cmd/ridgeline"],
            cwd=ROOT, check=True,
        )
        with (scratch / "service.log").open("wb") as log:
            process, base = launch(binary, scratch / "cal.db", log)
            try:
                space = "spaces/lab"

                def op(label, route, payload=None, **options):
                    out = request(base, route, payload, **options)
                    observations.append({"step": label, "request": payload, "response": out})
                    return out

                op("create namespace", "spaces", NAMESPACE)

                # Two points in the first coefficient era and one at the
                # half-open boundary t=2000, which must use the later segment.
                points = [
                    {"labels": SENSOR, "time": 1000, "value": 10.0},
                    {"labels": SENSOR, "time": 1500, "value": 20.0},
                    {"labels": SENSOR, "time": 2000, "value": 30.0},
                ]
                op("ingest ordered points", space + "/points", {"now": 2000, "points": points})

                # Overlapping segments inside one rule are rejected at write.
                overlapping = {
                    "rules": [{
                        "matchers": OLD_RULE["matchers"],
                        "segments": [
                            {"start": 0, "end": 2500, "factor": 1.0, "offset": 0.0},
                            {"start": 2000, "end": 10000, "factor": 1.1, "offset": 2.0},
                        ],
                    }]
                }
                op("reject overlapping segments", space + "/calibrations/bad",
                   overlapping, method="PUT", status=400)

                # Two rules matching the same series are an ambiguous overlap.
                cross_rules = {
                    "rules": [
                        OLD_RULE,
                        {"matchers": [{"label": "node", "op": "=", "value": "thermo-1"}],
                         "segments": [{"start": 0, "end": 10000, "factor": 2.0, "offset": 0.0}]},
                    ]
                }
                op("reject overlapping rule selectors", space + "/calibrations/bad2",
                   cross_rules, method="PUT", status=400)

                op("define factory view", space + "/calibrations/factory",
                   {"rules": [OLD_RULE]}, method="PUT")
                inventory = op("list views", space + "/calibrations")
                expect(len(inventory) == 1 and inventory[0]["revision"] == 1,
                       "new view should be revision 1")

                raw_query = {"start": 0, "end": 3000,
                             "matchers": [{"label": "__name__", "op": "=", "value": "probe_celsius"}]}
                raw_before = op("raw range before correction", space + "/range", raw_query)

                cal_query = {**raw_query, "calibration": "factory"}
                calibrated = op("calibrated range", space + "/calibrated-range", cal_query)
                pts = {p["time"]: p for p in calibrated["series"][0]["points"]}
                expect(calibrated["calibration"] == "factory", "response must name the view")
                # t=1000: 1.0*10+0 = 10 ; t=1500: 1.0*20 = 20 ; t=2000 boundary: 1.1*30+2 = 35.
                expect(pts[1000]["value"] == 10.0 and pts[1000]["raw_value"] == 10.0
                       and pts[1000]["factor"] == 1.0 and pts[1000]["offset"] == 0.0
                       and pts[1000]["calibrated"] is True, "first segment provenance")
                expect(pts[2000]["value"] == 35.0 and pts[2000]["factor"] == 1.1
                       and pts[2000]["offset"] == 2.0,
                       "boundary timestamp belongs to the later half-open segment")

                # A gap with no segment: no coefficient covers t >= 10000 in
                # the second view definition, so the raw value passes through
                # with calibrated=false and identity coefficients.
                op("define gapped view", space + "/calibrations/gapped",
                   {"rules": [{"matchers": OLD_RULE["matchers"],
                               "segments": [{"start": 0, "end": 2000,
                                             "factor": 1.0, "offset": 0.0}]}]},
                   method="PUT")
                gapped = op("gap passes raw through", space + "/calibrated-range",
                            {**raw_query, "calibration": "gapped"})
                gp = {p["time"]: p for p in gapped["series"][0]["points"]}
                expect(gp[2000]["calibrated"] is False and gp[2000]["value"] == 30.0
                       and gp[2000]["raw_value"] == 30.0 and gp[2000]["factor"] == 1.0
                       and gp[2000]["offset"] == 0.0,
                       "uncalibrated time range must pass the raw value through explicitly")

                # Late data: ingest t=500 after t=2000. It must be handled by
                # the segment effective at observation time 500, not arrival.
                op("late arrival", space + "/points",
                   {"now": 2500, "points": [{"labels": SENSOR, "time": 500, "value": 40.0}]})
                late = op("late point uses observation-time coefficient",
                          space + "/calibrated-range",
                          {**raw_query, "start": 0, "calibration": "factory"})
                lp = {p["time"]: p for p in late["series"][0]["points"]}
                expect(lp[500]["value"] == 40.0 and lp[500]["calibrated"] is True
                       and lp[500]["factor"] == 1.0,
                       "late data is calibrated at its observation timestamp")

                # Ordinary aggregation stays raw unless a view is selected.
                agg = {**raw_query, "start": 0, "end": 3000, "step": 3000, "origin": 0,
                       "group_by": ["zone"], "function": "avg"}
                raw_agg = op("raw aggregate", space + "/aggregate", agg)
                cal_agg = op("calibrated aggregate", space + "/aggregate",
                             {**agg, "calibration": "factory"})
                raw_values = sorted(p["value"] for p in
                                    raw_before["series"][0]["points"]) + [40.0]
                expect(abs(raw_agg["cells"][0]["value"] - sum(raw_values) / len(raw_values)) < 1e-12,
                       "plain aggregate must use raw values")
                # Calibrated values: 40, 10, 20, 35 -> avg 26.25.
                expect(abs(cal_agg["cells"][0]["value"] - 26.25) < 1e-12,
                       "calibrated aggregate must use view values")

                # Materialize against the factory view, then correct the old
                # coefficient. Raw points never change; calibrated history and
                # the rollup staleness flag do.
                op("materialize calibrated rollup", space + "/rollups/bench_cal",
                   {**agg, "calibration": "factory"}, method="PUT")
                rollup_before = op("read rollup", space + "/rollups/bench_cal")
                expect(rollup_before["calibration"] == "factory"
                       and rollup_before["stale"] is False,
                       "fresh calibrated rollup records its view")

                raw_before_correction = op("raw range snapshot", space + "/range", raw_query)
                op("correct old coefficient", space + "/calibrations/factory",
                   {"rules": [CORRECTED_RULE]}, method="PUT")
                revision = op("view revision after correction", space + "/calibrations/factory")
                expect(revision["revision"] == 2, "replacing a view increments its revision")

                raw_after = op("raw range after correction", space + "/range", raw_query)
                expect(raw_after["series"] == raw_before_correction["series"],
                       "calibration correction must not modify any raw point")
                # The raw series grew only because of the explicit late ingest,
                # never because of a calibration write.
                raw_times_before = {p["time"] for s in raw_before["series"] for p in s["points"]}
                raw_times_after = {p["time"] for s in raw_after["series"] for p in s["points"]}
                expect(raw_times_before.issubset(raw_times_after)
                       and raw_times_after - raw_times_before == {500},
                       "raw observations change only through ingestion, never calibration")
                shared = {p["time"]: p["value"]
                          for s in raw_before_correction["series"] for p in s["points"]}
                for sp in raw_after["series"]:
                    for p in sp["points"]:
                        if p["time"] in shared:
                            expect(p["value"] == shared[p["time"]],
                                   "existing raw values are immutable under coefficient correction")

                corrected = op("calibrated history after correction",
                               space + "/calibrated-range", cal_query)
                cp = {p["time"]: p for p in corrected["series"][0]["points"]}
                # t=1000: 1.2*10+1 = 13 ; t=1500: 1.2*20+1 = 25 ; t=2000 stays 35.
                expect(cp[1000]["value"] == 13.0 and cp[1000]["factor"] == 1.2
                       and cp[1000]["offset"] == 1.0,
                       "old-era history is recomputed with the corrected coefficients")
                expect(cp[1500]["value"] == 25.0, "corrected second old-era point")
                expect(cp[2000]["value"] == 35.0 and cp[2000]["factor"] == 1.1,
                       "untouched segment keeps its original calibrated value")

                rollup_stale = op("rollup stale after coefficient correction",
                                  space + "/rollups/bench_cal")
                expect(rollup_stale["stale"] is True,
                       "coefficient correction must mark the old materialization stale")
                op("refresh rollup", space + "/rollups/bench_cal",
                   {**agg, "calibration": "factory"}, method="PUT")
                rollup_fresh = op("refreshed rollup", space + "/rollups/bench_cal")
                # Corrected calibrated values across 4 points: 40->49, 10->13,
                # 20->25, 30->35 => avg = (49+13+25+35)/4 = 30.5.
                expect(rollup_fresh["stale"] is False
                       and abs(rollup_fresh["result"]["cells"][0]["value"] - 30.5) < 1e-12,
                       "refreshed rollup reflects the corrected coefficients")

                # Unknown views are explicit 404s, never silent raw fallbacks.
                op("unknown view rejected", space + "/calibrated-range",
                   {**raw_query, "calibration": "missing"}, status=404)
            finally:
                stop(process)
    print(json.dumps({"workflow": FLOW, "state": "complete",
                      "checks": "boundaries,gaps,late-data,correction,provenance,rollup-staleness"}))
