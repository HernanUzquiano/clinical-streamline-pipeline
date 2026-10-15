from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from api.app import app, get_client


class FakeClickHouse:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.queries: list[tuple[str, dict]] = []

    def command(self, sql):
        if self.fail:
            raise ConnectionError("refused")
        return 1

    def query(self, sql, parameters):
        self.queries.append((sql, parameters))
        return SimpleNamespace(column_names=["ward", "admitted"], result_rows=[("ICU", 3)])


@pytest.fixture
def fake():
    client = FakeClickHouse()
    app.dependency_overrides[get_client] = lambda: client
    yield client
    app.dependency_overrides.clear()


def test_tenant_header_is_required(fake):
    response = TestClient(app).get("/v1/census")
    assert response.status_code == 422
    assert fake.queries == []


def test_queries_are_tenant_scoped_and_parameterised(fake):
    response = TestClient(app).get("/v1/census", headers={"X-Tenant-ID": "H002"})
    assert response.status_code == 200
    assert response.json() == [{"ward": "ICU", "admitted": 3}]
    sql, params = fake.queries[0]
    assert "tenant_id = {tenant:String}" in sql
    assert params == {"tenant": "H002"}


def test_alert_threshold_is_validated(fake):
    response = TestClient(app).get("/v1/alerts?min_score=99", headers={"X-Tenant-ID": "H001"})
    assert response.status_code == 422


def test_health_reports_unavailable_clickhouse():
    app.dependency_overrides[get_client] = lambda: FakeClickHouse(fail=True)
    try:
        response = TestClient(app).get("/health")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 503
