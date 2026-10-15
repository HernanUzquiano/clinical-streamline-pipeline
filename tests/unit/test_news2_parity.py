"""The scoring logic lives twice: in Flink SQL (production) and pipeline_tools/news2.py (validation).
Parse the SQL and fail if the thresholds have drifted apart."""

import math
import re
from pathlib import Path

import pytest

from pipeline_tools import news2

SQL = (Path(__file__).resolve().parents[2] / "flink/jobs/vitals_1m.sql").read_text()


def sql_bands(metric: str) -> list[tuple[float, int]]:
    block = re.search(rf"\(CASE (WHEN {metric} .*?)END\)", SQL, re.DOTALL)
    assert block, f"no CASE block for {metric} in vitals_1m.sql"
    body = block.group(1)
    bands = re.findall(rf"WHEN {metric} <= ([\d.]+) THEN (\d+)", body)
    (else_points,) = re.findall(r"ELSE (\d+)", body)
    return [*((float(upper), int(points)) for upper, points in bands), (math.inf, int(else_points))]


@pytest.mark.parametrize("metric", sorted(news2.THRESHOLDS))
def test_sql_thresholds_match_python(metric):
    assert sql_bands(metric) == [(float(u), p) for u, p in news2.THRESHOLDS[metric]]


def test_sql_risk_levels_match_python():
    assert "WHEN news2_score >= 7 THEN 'high'" in SQL
    assert "WHEN news2_score >= 5 THEN 'medium'" in SQL
    assert news2.risk_level(7) == "high" and news2.risk_level(5) == "medium" and news2.risk_level(4) == "low"
