"""Reference implementation of the simplified NEWS2-style score computed by flink/jobs/vitals_1m.sql.

The Flink job is the production implementation; this module exists so that the smoke test can
recompute expected scores from the raw source rows, and so that tests/unit/test_news2_parity.py
can fail the build if the two drift apart.

Simplified: scores window *averages*, ignores supplemental O2 and consciousness level.
NOT FOR CLINICAL USE.
"""

from __future__ import annotations

import math

# (upper bound inclusive, points). Last band has an infinite upper bound (the SQL ELSE branch).
INF = math.inf

THRESHOLDS: dict[str, list[tuple[float, int]]] = {
    "avg_resp_rate": [(8, 3), (11, 1), (20, 0), (24, 2), (INF, 3)],
    "avg_spo2": [(91, 3), (93, 2), (95, 1), (INF, 0)],
    "avg_systolic_bp": [(90, 3), (100, 2), (110, 1), (219, 0), (INF, 3)],
    "avg_heart_rate": [(40, 3), (50, 1), (90, 0), (110, 1), (130, 2), (INF, 3)],
    "avg_temperature_c": [(35.0, 3), (36.0, 1), (38.0, 0), (39.0, 1), (INF, 2)],
}


def band_points(metric: str, value: float) -> int:
    for upper, points in THRESHOLDS[metric]:
        if value <= upper:
            return points
    raise AssertionError("unreachable: last band is unbounded")


def score(window: dict[str, float]) -> int:
    """Score a window given its (already rounded) averages, keyed like the curated contract."""
    return sum(band_points(metric, window[metric]) for metric in THRESHOLDS)


def risk_level(news2_score: int) -> str:
    if news2_score >= 7:
        return "high"
    if news2_score >= 5:
        return "medium"
    return "low"
