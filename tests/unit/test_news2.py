import pytest

from pipeline_tools import news2

NORMAL = {
    "avg_resp_rate": 16,
    "avg_spo2": 98,
    "avg_systolic_bp": 120,
    "avg_heart_rate": 75,
    "avg_temperature_c": 36.8,
}


def test_normal_window_scores_zero():
    assert news2.score(NORMAL) == 0
    assert news2.risk_level(0) == "low"


@pytest.mark.parametrize(
    ("metric", "value", "points"),
    [
        ("avg_resp_rate", 8, 3),
        ("avg_resp_rate", 8.01, 1),
        ("avg_resp_rate", 20, 0),
        ("avg_resp_rate", 24, 2),
        ("avg_resp_rate", 25, 3),
        ("avg_spo2", 91, 3),
        ("avg_spo2", 93, 2),
        ("avg_spo2", 95, 1),
        ("avg_spo2", 96, 0),
        ("avg_systolic_bp", 90, 3),
        ("avg_systolic_bp", 219, 0),
        ("avg_systolic_bp", 220, 3),
        ("avg_heart_rate", 40, 3),
        ("avg_heart_rate", 50, 1),
        ("avg_heart_rate", 91, 1),
        ("avg_heart_rate", 131, 3),
        ("avg_temperature_c", 35.0, 3),
        ("avg_temperature_c", 36.0, 1),
        ("avg_temperature_c", 38.5, 1),
        ("avg_temperature_c", 39.1, 2),
    ],
)
def test_band_boundaries(metric, value, points):
    assert news2.band_points(metric, value) == points


def test_septic_picture_is_high_risk():
    window = {
        "avg_resp_rate": 26,
        "avg_spo2": 90,
        "avg_systolic_bp": 95,
        "avg_heart_rate": 125,
        "avg_temperature_c": 39.4,
    }
    score = news2.score(window)
    assert score == 3 + 3 + 2 + 2 + 2
    assert news2.risk_level(score) == "high"


@pytest.mark.parametrize(("score", "level"), [(4, "low"), (5, "medium"), (6, "medium"), (7, "high")])
def test_risk_levels(score, level):
    assert news2.risk_level(score) == level
