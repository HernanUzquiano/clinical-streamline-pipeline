"""End-to-end smoke gate. Needs the running stack (`make pipeline`); takes ~3 minutes.

Runs the simulator against fresh, isolated tenants and then checks, against the source of record:
  * completeness  — every encounter reaches ClickHouse with its latest state and patient attributes
  * correctness   — every closed 1-minute window matches an independent recomputation from Postgres
  * scoring       — Flink's NEWS2 score equals pipeline_tools.news2 on the same averages
  * contracts     — records on both curated topics validate against contracts/*.json
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
import uuid

import clickhouse_connect
import psycopg
import pytest
from confluent_kafka import Consumer

from pipeline_tools import contracts, news2
from pipeline_tools.settings import settings

pytestmark = pytest.mark.smoke

SIM_SECONDS = 140
HOSPITALS = 2
PREFIX = f"SMK{uuid.uuid4().hex[:6].upper()}-"
TENANTS = [f"{PREFIX}{i:03d}" for i in range(1, HOSPITALS + 1)]
AVG_TOLERANCE = 0.011  # Flink rounds a double average, Postgres an exact numeric one


def eventually(check, timeout: float, interval: float = 2.0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return check()
        except AssertionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(interval)


@pytest.fixture(scope="module")
def pg():
    with psycopg.connect(settings.pg_dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="module")
def ch():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


@pytest.fixture(scope="module")
def simulated():
    subprocess.run(
        [
            sys.executable,
            "-m",
            "generator.simulate",
            "--hospitals",
            str(HOSPITALS),
            "--patients-per-hospital",
            "15",
            "--duration",
            str(SIM_SECONDS),
            "--tenant-prefix",
            PREFIX,
            "--deteriorating-ratio",
            "0.3",
            "--seed",
            "42",
        ],
        check=True,
    )
    return TENANTS


def test_pipeline_components_running():
    with urllib.request.urlopen("http://localhost:8083/connectors/ehr-postgres/status") as r:
        status = json.load(r)
    assert status["connector"]["state"] == "RUNNING"
    assert all(t["state"] == "RUNNING" for t in status["tasks"])

    with urllib.request.urlopen("http://localhost:8081/jobs/overview") as r:
        running = {j["name"] for j in json.load(r)["jobs"] if j["state"] == "RUNNING"}
    assert {"encounters-curated", "vitals-1m"} <= running


def test_encounters_complete_and_current(simulated, pg, ch):
    source = {
        row[0]: row[1:]
        for row in pg.execute(
            "SELECT e.encounter_id, e.status, p.sex FROM encounters e JOIN patients p USING (patient_id) "
            "WHERE e.tenant_id = ANY(%s)",
            (simulated,),
        )
    }
    assert source, "simulator wrote no encounters"

    def check():
        result = ch.query(
            "SELECT encounter_id, status, sex FROM encounters FINAL WHERE tenant_id IN {t:Array(String)}",
            parameters={"t": simulated},
        )
        served = {row[0]: row[1:] for row in result.result_rows}
        assert served == source

    eventually(check, timeout=60)


def test_closed_windows_match_source(simulated, pg, ch):
    def closed_windows():
        result = ch.query(
            "SELECT encounter_id, toDateTime(window_start) AS ws, sample_count, avg_heart_rate, avg_resp_rate, "
            "avg_spo2, avg_systolic_bp, avg_temperature_c, min_spo2, max_heart_rate, news2_score, risk_level "
            "FROM vitals_1m FINAL WHERE tenant_id IN {t:Array(String)}",
            parameters={"t": simulated},
        )
        rows = [dict(zip(result.column_names, r, strict=True)) for r in result.result_rows]
        # >= 2 full minutes of data were written, so every encounter active for them has closed windows.
        assert len({r["ws"] for r in rows}) >= 2
        return rows

    served = eventually(closed_windows, timeout=90)

    expected = {
        (r[0], r[1]): r[2:]
        for r in pg.execute(
            """
            SELECT encounter_id, date_trunc('minute', observed_at) AS ws, count(*),
                   round(avg(heart_rate), 2), round(avg(resp_rate), 2), round(avg(spo2), 2),
                   round(avg(systolic_bp), 2), round(avg(temperature_c)::numeric, 2),
                   min(spo2), max(heart_rate)
            FROM vital_observations WHERE tenant_id = ANY(%s)
            GROUP BY 1, 2
            """,
            (simulated,),
        )
    }

    metrics = ["avg_heart_rate", "avg_resp_rate", "avg_spo2", "avg_systolic_bp", "avg_temperature_c"]
    for w in served:
        count, *avgs, min_spo2, max_hr = expected[(w["encounter_id"], w["ws"])]
        assert w["sample_count"] == count, f"window {w['encounter_id']}@{w['ws']}: lost or duplicated readings"
        for metric, exp in zip(metrics, avgs, strict=True):
            assert abs(w[metric] - float(exp)) <= AVG_TOLERANCE, (metric, w, exp)
        assert (w["min_spo2"], w["max_heart_rate"]) == (min_spo2, max_hr)
        assert w["news2_score"] == news2.score(w)
        assert w["risk_level"] == news2.risk_level(w["news2_score"])

    assert any(w["risk_level"] != "low" for w in served), "expected deteriorating patients to raise alerts"


@pytest.mark.parametrize("topic", contracts.all_topics())
def test_curated_topics_honour_contract(simulated, topic):
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"smoke-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    checked, deadline = 0, time.monotonic() + 30
    try:
        while checked < 300 and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error() or msg.value() is None:  # None value = tombstone
                continue
            record = json.loads(msg.value())
            assert contracts.validate(topic, record) == [], record
            key = json.loads(msg.key())
            assert list(key) == contracts.key_fields(topic)
            checked += 1
    finally:
        consumer.close()
    assert checked > 0, f"no records read from {topic}"
