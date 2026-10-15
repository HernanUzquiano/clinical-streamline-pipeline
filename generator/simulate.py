"""Synthetic hospital simulator: writes patients, encounters and vital signs into the source Postgres.

Every write is an ordinary OLTP transaction; Debezium picks the changes up from the WAL. The data is
entirely synthetic (random MRNs, birth years and vitals) and contains no PHI.

    python -m generator.simulate --hospitals 3 --duration 120
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg

from pipeline_tools.settings import settings

log = logging.getLogger("simulator")

WARDS = ["ICU", "MED-A", "MED-B", "SURG", "ED"]
ENCOUNTER_TYPES = ["inpatient", "inpatient", "emergency", "observation"]


@dataclass
class PatientState:
    """Per-encounter physiology. `deterioration` in [0, 1] pushes vitals toward abnormal ranges."""

    heart_rate: float
    resp_rate: float
    spo2: float
    systolic_bp: float
    temperature_c: float
    deterioration: float = 0.0
    trend: float = 0.0  # change in deterioration per tick

    @classmethod
    def baseline(cls, rng: random.Random, deteriorating: bool) -> PatientState:
        return cls(
            heart_rate=rng.gauss(78, 8),
            resp_rate=rng.gauss(15, 2),
            spo2=rng.gauss(97, 1),
            systolic_bp=rng.gauss(122, 10),
            temperature_c=rng.gauss(36.8, 0.3),
            trend=rng.uniform(0.004, 0.012) if deteriorating else 0.0,
        )


@dataclass
class Vitals:
    heart_rate: int
    resp_rate: int
    spo2: int
    systolic_bp: int
    temperature_c: float


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def next_vitals(state: PatientState, rng: random.Random) -> Vitals:
    """Advance one tick of a patient's physiology and return a plausible, bounded reading."""
    state.deterioration = clamp(state.deterioration + state.trend, 0.0, 1.0)
    d = state.deterioration
    return Vitals(
        heart_rate=round(clamp(state.heart_rate + 55 * d + rng.gauss(0, 3), 30, 220)),
        resp_rate=round(clamp(state.resp_rate + 14 * d + rng.gauss(0, 1), 6, 60)),
        spo2=round(clamp(state.spo2 - 10 * d + rng.gauss(0, 0.7), 70, 100)),
        systolic_bp=round(clamp(state.systolic_bp - 40 * d + rng.gauss(0, 4), 60, 250)),
        temperature_c=round(clamp(state.temperature_c + 2.2 * d + rng.gauss(0, 0.1), 33.0, 42.0), 1),
    )


@dataclass
class Hospital:
    tenant_id: str
    patient_ids: list[int] = field(default_factory=list)
    active: dict[int, tuple[int, PatientState]] = field(default_factory=dict)  # encounter_id -> (patient_id, state)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Simulator:
    def __init__(
        self,
        conn: psycopg.Connection,
        rng: random.Random,
        patients_per_hospital: int,
        deteriorating_ratio: float,
        turnover: float,
    ):
        self.conn = conn
        self.rng = rng
        self.patients_per_hospital = patients_per_hospital
        self.deteriorating_ratio = deteriorating_ratio
        self.turnover = turnover
        self.hospitals: list[Hospital] = []

    def setup(self, n_hospitals: int, tenant_prefix: str = "H") -> None:
        with self.conn.transaction(), self.conn.cursor() as cur:
            for i in range(1, n_hospitals + 1):
                tenant = f"{tenant_prefix}{i:03d}"
                cur.execute(
                    "INSERT INTO hospitals (tenant_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (tenant, f"Synthetic General Hospital {i}"),
                )
                hospital = Hospital(tenant)
                cur.execute("SELECT patient_id FROM patients WHERE tenant_id = %s", (tenant,))
                hospital.patient_ids = [r[0] for r in cur.fetchall()]
                while len(hospital.patient_ids) < self.patients_per_hospital:
                    cur.execute(
                        "INSERT INTO patients (tenant_id, mrn, birth_year, sex) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT (tenant_id, mrn) DO NOTHING RETURNING patient_id",
                        (
                            tenant,
                            f"MRN-{self.rng.randrange(10**8):08d}",
                            self.rng.randint(1935, 2020),
                            self.rng.choices(["F", "M", "U"], weights=[49, 49, 2])[0],
                        ),
                    )
                    row = cur.fetchone()
                    if row:
                        hospital.patient_ids.append(row[0])
                # Close encounters left open by a previous run; their in-memory state is gone.
                cur.execute(
                    "UPDATE encounters SET status = 'discharged', discharged_at = %s, updated_at = %s "
                    "WHERE tenant_id = %s AND status = 'admitted'",
                    (utcnow(), utcnow(), tenant),
                )
                self.hospitals.append(hospital)
            for hospital in self.hospitals:
                for patient_id in self.rng.sample(hospital.patient_ids, k=int(len(hospital.patient_ids) * 0.6)):
                    self._admit(cur, hospital, patient_id)
        log.info(
            "setup done: %d hospitals, %d active encounters",
            len(self.hospitals),
            sum(len(h.active) for h in self.hospitals),
        )

    def _admit(self, cur: psycopg.Cursor, hospital: Hospital, patient_id: int) -> None:
        now = utcnow()
        cur.execute(
            "INSERT INTO encounters (tenant_id, patient_id, encounter_type, status, ward, admitted_at, updated_at) "
            "VALUES (%s, %s, %s, 'admitted', %s, %s, %s) RETURNING encounter_id",
            (hospital.tenant_id, patient_id, self.rng.choice(ENCOUNTER_TYPES), self.rng.choice(WARDS), now, now),
        )
        encounter_id = cur.fetchone()[0]
        deteriorating = self.rng.random() < self.deteriorating_ratio
        hospital.active[encounter_id] = (patient_id, PatientState.baseline(self.rng, deteriorating))

    def _discharge(self, cur: psycopg.Cursor, hospital: Hospital, encounter_id: int) -> None:
        now = utcnow()
        cur.execute(
            "UPDATE encounters SET status = 'discharged', discharged_at = %s, updated_at = %s WHERE encounter_id = %s",
            (now, now, encounter_id),
        )
        del hospital.active[encounter_id]

    def tick(self) -> int:
        """One round: a vital reading for every active encounter, plus occasional discharges/admissions."""
        now = utcnow()
        rows = []
        with self.conn.transaction(), self.conn.cursor() as cur:
            for hospital in self.hospitals:
                for encounter_id, (patient_id, state) in hospital.active.items():
                    v = next_vitals(state, self.rng)
                    rows.append(
                        (
                            hospital.tenant_id,
                            encounter_id,
                            patient_id,
                            now,
                            v.heart_rate,
                            v.resp_rate,
                            v.spo2,
                            v.systolic_bp,
                            v.temperature_c,
                        )
                    )
                if hospital.active and self.rng.random() < self.turnover:
                    self._discharge(cur, hospital, self.rng.choice(list(hospital.active)))
                    admitted = {p for p, _ in hospital.active.values()}
                    waiting = [p for p in hospital.patient_ids if p not in admitted]
                    if waiting:
                        self._admit(cur, hospital, self.rng.choice(waiting))
            cur.executemany(
                "INSERT INTO vital_observations (tenant_id, encounter_id, patient_id, observed_at, heart_rate, "
                "resp_rate, spo2, systolic_bp, temperature_c) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                rows,
            )
        return len(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hospitals", type=int, default=3)
    parser.add_argument("--patients-per-hospital", type=int, default=25)
    parser.add_argument("--duration", type=float, default=120, help="seconds to run; 0 = until interrupted")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between vital-sign rounds")
    parser.add_argument("--deteriorating-ratio", type=float, default=0.15)
    parser.add_argument("--turnover", type=float, default=0.05, help="per-hospital discharge probability per tick")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--tenant-prefix", default="H", help="tenant ids are <prefix>001, <prefix>002, ...")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    with psycopg.connect(settings.pg_dsn) as conn:
        sim = Simulator(
            conn, random.Random(args.seed), args.patients_per_hospital, args.deteriorating_ratio, args.turnover
        )
        sim.setup(args.hospitals, args.tenant_prefix)
        started = time.monotonic()
        total = 0
        while not stop and (args.duration == 0 or time.monotonic() - started < args.duration):
            tick_started = time.monotonic()
            total += sim.tick()
            time.sleep(max(0.0, args.interval - (time.monotonic() - tick_started)))
        log.info("stopped after %.0fs, %d vital observations written", time.monotonic() - started, total)


if __name__ == "__main__":
    main()
