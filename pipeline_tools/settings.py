"""Connection settings, overridable by environment. Defaults target the local docker compose stack from the host."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("PGHOST", "localhost")
    pg_port: int = int(os.getenv("PGPORT", "15432"))
    pg_user: str = os.getenv("PGUSER", "ehr")
    pg_password: str = os.getenv("PGPASSWORD", "ehr")
    pg_database: str = os.getenv("PGDATABASE", "ehr")

    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "clinical")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "clinical")
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "clinical")

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} user={self.pg_user} "
            f"password={self.pg_password} dbname={self.pg_database}"
        )


settings = Settings()
