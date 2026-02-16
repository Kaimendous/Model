from __future__ import annotations

import types

import pytest

from racing_form_etl.api.the_racing_api_client import TheRacingAPIClient, normalize_region_code


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        raise RuntimeError(f"HTTP {self.status_code}: {self.text}")


def test_fetch_daily_racecards_uses_summaries_with_region_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TheRacingAPIClient(username="u", password="p")
    captured: dict[str, object] = {}

    def fake_request(self, method, path, params=None, cancel_event=None):
        captured["method"] = method
        captured["path"] = path
        captured["params"] = params
        return {"data": []}

    monkeypatch.setattr(TheRacingAPIClient, "_request", fake_request)

    client.fetch_daily_racecards("2026-02-15", ["GB", "IRE", "hk"])

    assert captured["method"] == "GET"
    assert captured["path"] == "racecards/summaries"
    assert captured["params"] == {"date": "2026-02-15", "region_codes[]": ["gb", "ire", "hk"]}


def test_request_401_basic_plan_required_has_friendly_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TheRacingAPIClient(username="u", password="p", max_retries=0)

    def fake_request(*args, **kwargs):
        return _DummyResponse(401, text='{"detail":"Basic Plan required"}')

    import racing_form_etl.api.the_racing_api_client as module

    monkeypatch.setattr(module, "requests", types.SimpleNamespace(request=fake_request))

    with pytest.raises(RuntimeError, match="Basic plan needed"):
        client.fetch_daily_racecard_summaries("2026-02-15", ["gb"])


def test_probe_capabilities_auth_uses_free_regions_endpoint() -> None:
    calls: list[tuple[str, object]] = []

    class _Client(TheRacingAPIClient):
        def fetch_course_regions(self, cancel_event=None):
            calls.append(("courses/regions", None))
            return {"data": ["gb"]}

        def fetch_daily_racecard_summaries(self, date, regions, cancel_event=None):
            calls.append(("racecards/summaries", {"date": date, "regions": regions}))
            return {"data": []}

    client = _Client(username="u", password="p")
    caps = client.probe_capabilities("2026-02-15", ["GB"])

    assert caps["auth_ok"] is True
    assert caps["can_racecards"] is True
    assert calls[0][0] == "courses/regions"
    assert calls[1] == ("racecards/summaries", {"date": "2026-02-15", "regions": ["gb"]})


def test_normalize_region_code_maps_country_aliases() -> None:
    assert normalize_region_code("GB") == "gb"
    assert normalize_region_code("ire") == "ire"
    assert normalize_region_code("HKG") == "hk"
