import random

from generator.simulate import PatientState, next_vitals
from pipeline_tools import news2


def run(state: PatientState, ticks: int, rng: random.Random):
    return [next_vitals(state, rng) for _ in range(ticks)]


def window_score(readings) -> int:
    n = len(readings)
    return news2.score(
        {
            "avg_heart_rate": sum(r.heart_rate for r in readings) / n,
            "avg_resp_rate": sum(r.resp_rate for r in readings) / n,
            "avg_spo2": sum(r.spo2 for r in readings) / n,
            "avg_systolic_bp": sum(r.systolic_bp for r in readings) / n,
            "avg_temperature_c": sum(r.temperature_c for r in readings) / n,
        }
    )


def test_vitals_stay_within_physiological_bounds():
    rng = random.Random(1)
    for _ in range(50):
        for v in run(PatientState.baseline(rng, deteriorating=True), 300, rng):
            assert 30 <= v.heart_rate <= 220
            assert 6 <= v.resp_rate <= 60
            assert 70 <= v.spo2 <= 100
            assert 60 <= v.systolic_bp <= 250
            assert 33.0 <= v.temperature_c <= 42.0


def test_stable_patients_stay_low_risk():
    rng = random.Random(2)
    for _ in range(50):
        readings = run(PatientState.baseline(rng, deteriorating=False), 60, rng)
        assert window_score(readings) <= 4


def test_deteriorating_patient_score_rises():
    rng = random.Random(3)
    state = PatientState.baseline(rng, deteriorating=True)
    first = run(state, 60, rng)
    for _ in range(3):
        run(state, 60, rng)
    later = run(state, 60, rng)
    assert window_score(later) > window_score(first)
    assert state.deterioration > 0
