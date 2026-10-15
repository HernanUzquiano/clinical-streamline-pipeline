"""CDC freshness benchmark: time from a committed source UPDATE to the new version being queryable in ClickHouse.

Path measured: Postgres WAL -> Debezium -> Kafka -> Flink (join + upsert) -> Kafka -> ClickHouse Kafka engine -> MV.
Exits non-zero when p95 breaches the contract SLO (contracts/curated.encounters.v1.json, x-freshness-slo).

    python scripts/bench_freshness.py --samples 50
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from datetime import datetime, timezone

import clickhouse_connect
import psycopg

from pipeline_tools.settings import settings


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(pct / 100 * (len(ordered) - 1)))]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--slo-p95", type=float, default=15.0, help="seconds")
    parser.add_argument("--timeout", type=float, default=60.0, help="per-sample seconds")
    args = parser.parse_args()

    ch = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
    latencies: list[float] = []
    with psycopg.connect(settings.pg_dsn, autocommit=True) as pg:
        encounter_ids = [
            r[0] for r in pg.execute("SELECT encounter_id FROM encounters ORDER BY random() LIMIT %s", (args.samples,))
        ]
        if not encounter_ids:
            print("no encounters in the source database; run the simulator first", file=sys.stderr)
            return 2
        for i in range(args.samples):
            encounter_id = encounter_ids[i % len(encounter_ids)]
            marker = datetime.now(timezone.utc).replace(tzinfo=None)
            pg.execute("UPDATE encounters SET updated_at = %s WHERE encounter_id = %s", (marker, encounter_id))
            committed = time.monotonic()
            while True:
                visible = ch.command(
                    "SELECT count() FROM encounters "
                    "WHERE encounter_id = {id:Int64} AND updated_at >= {m:DateTime64(3)}",
                    parameters={"id": encounter_id, "m": marker.isoformat(sep=" ", timespec="milliseconds")},
                )
                if int(visible) > 0:
                    latencies.append(time.monotonic() - committed)
                    break
                if time.monotonic() - committed > args.timeout:
                    print(f"sample {i}: encounter {encounter_id} not visible after {args.timeout}s", file=sys.stderr)
                    return 1
                time.sleep(0.05)

    p50, p95 = percentile(latencies, 50), percentile(latencies, 95)
    print(
        f"samples={len(latencies)}  p50={p50:.2f}s  p95={p95:.2f}s  max={max(latencies):.2f}s  "
        f"mean={statistics.mean(latencies):.2f}s  (SLO p95 < {args.slo_p95:.0f}s)"
    )
    return 0 if p95 < args.slo_p95 else 1


if __name__ == "__main__":
    sys.exit(main())
