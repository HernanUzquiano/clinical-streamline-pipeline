"""Read API over the ClickHouse serving layer.

Every endpoint is tenant-scoped through the X-Tenant-ID header; tenant_id is the leading column of
every ORDER BY key, so tenant filters prune to that tenant's granules.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from fastapi import Depends, FastAPI, Header, HTTPException, Query

from pipeline_tools.settings import settings

app = FastAPI(
    title="Clinical Stream API",
    description="Ward census, vital-sign windows and early-warning alerts from the streaming pipeline. "
    "Synthetic data only. Not for clinical use.",
    version="1.0.0",
)


@lru_cache
def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


Tenant = Annotated[str, Header(alias="X-Tenant-ID", min_length=1, max_length=32)]
ClickHouse = Annotated[Client, Depends(get_client)]


def rows(client: Client, sql: str, params: dict) -> list[dict]:
    result = client.query(sql, parameters=params)
    return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]


@app.get("/health")
def health(client: ClickHouse) -> dict:
    try:
        client.command("SELECT 1")
    except Exception as exc:  # any driver/network failure means unhealthy
        raise HTTPException(status_code=503, detail=f"clickhouse unavailable: {exc}") from exc
    return {"status": "ok"}


@app.get("/v1/encounters")
def list_encounters(
    tenant: Tenant,
    client: ClickHouse,
    status: Literal["admitted", "discharged"] | None = None,
    ward: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    return rows(
        client,
        """
        SELECT encounter_id, patient_id, encounter_type, status, ward, admitted_at, discharged_at,
               sex, age_band, length_of_stay_hours
        FROM encounters FINAL
        WHERE tenant_id = {tenant:String}
          AND ({status:Nullable(String)} IS NULL OR status = {status:Nullable(String)})
          AND ({ward:Nullable(String)} IS NULL OR ward = {ward:Nullable(String)})
        ORDER BY admitted_at DESC
        LIMIT {limit:UInt32}
        """,
        {"tenant": tenant, "status": status, "ward": ward, "limit": limit},
    )


@app.get("/v1/census")
def census(tenant: Tenant, client: ClickHouse) -> list[dict]:
    """Currently admitted patients per ward."""
    return rows(
        client,
        """
        SELECT ward, count() AS admitted
        FROM encounters FINAL
        WHERE tenant_id = {tenant:String} AND status = 'admitted'
        GROUP BY ward
        ORDER BY ward
        """,
        {"tenant": tenant},
    )


@app.get("/v1/encounters/{encounter_id}/vitals")
def encounter_vitals(
    tenant: Tenant,
    client: ClickHouse,
    encounter_id: int,
    minutes: int = Query(60, ge=1, le=24 * 60),
) -> list[dict]:
    return rows(
        client,
        """
        SELECT window_start, window_end, sample_count, avg_heart_rate, avg_resp_rate, avg_spo2,
               avg_systolic_bp, avg_temperature_c, min_spo2, max_heart_rate, news2_score, risk_level
        FROM vitals_1m FINAL
        WHERE tenant_id = {tenant:String}
          AND encounter_id = {encounter_id:Int64}
          AND window_start >= now64(3) - toIntervalMinute({minutes:UInt32})
        ORDER BY window_start
        """,
        {"tenant": tenant, "encounter_id": encounter_id, "minutes": minutes},
    )


@app.get("/v1/alerts")
def alerts(
    tenant: Tenant,
    client: ClickHouse,
    min_score: int = Query(5, ge=0, le=15),
    since_minutes: int = Query(15, ge=1, le=24 * 60),
) -> list[dict]:
    """Admitted encounters whose most recent window scores at or above `min_score`."""
    return rows(
        client,
        """
        SELECT v.encounter_id, v.patient_id, e.ward, v.last_window,
               v.news2_score, v.risk_level, v.avg_heart_rate, v.avg_resp_rate, v.avg_spo2,
               v.avg_systolic_bp, v.avg_temperature_c
        FROM (
            SELECT encounter_id,
                   argMax(patient_id, window_start)        AS patient_id,
                   max(window_start)                       AS last_window,
                   argMax(news2_score, window_start)       AS news2_score,
                   argMax(risk_level, window_start)        AS risk_level,
                   argMax(avg_heart_rate, window_start)    AS avg_heart_rate,
                   argMax(avg_resp_rate, window_start)     AS avg_resp_rate,
                   argMax(avg_spo2, window_start)          AS avg_spo2,
                   argMax(avg_systolic_bp, window_start)   AS avg_systolic_bp,
                   argMax(avg_temperature_c, window_start) AS avg_temperature_c
            FROM vitals_1m FINAL
            WHERE tenant_id = {tenant:String}
              AND window_start >= now64(3) - toIntervalMinute({since:UInt32})
            GROUP BY encounter_id
        ) AS v
        INNER JOIN (
            SELECT encounter_id, ward FROM encounters FINAL
            WHERE tenant_id = {tenant:String} AND status = 'admitted'
        ) AS e USING (encounter_id)
        WHERE v.news2_score >= {min_score:UInt8}
        ORDER BY v.news2_score DESC, v.encounter_id
        """,
        {"tenant": tenant, "since": since_minutes, "min_score": min_score},
    )
