from __future__ import annotations

import base64
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date as date_cls
from threading import Event
from typing import Any, Optional

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None


@dataclass
class TheRacingAPIClient:
    username: str
    password: str
    api_key: str | None = None
    base_url: str = "https://api.theracingapi.com/v1"
    max_retries: int = 4
    rate_limit_rps: float = 2.0

    def __post_init__(self) -> None:
        self._last_request_at: float = 0.0

    @property
    def min_request_interval(self) -> float:
        return max(0.55, 1.0 / max(self.rate_limit_rps, 0.1))

    def _sleep_with_cancel(self, seconds: float, cancel_event: Event | None) -> None:
        elapsed = 0.0
        while elapsed < seconds:
            if cancel_event and cancel_event.is_set():
                raise RuntimeError("Request cancelled")
            step = min(0.2, seconds - elapsed)
            time.sleep(step)
            elapsed += step

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        cancel_event: Event | None = None,
    ) -> Any:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        backoff = 1.0
        for attempt in range(self.max_retries + 1):
            if cancel_event and cancel_event.is_set():
                raise RuntimeError("Request cancelled")
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.min_request_interval:
                self._sleep_with_cancel(self.min_request_interval - elapsed, cancel_event)
            try:
                if requests is not None:
                    resp = requests.request(
                        method,
                        url,
                        params=params,
                        headers=headers,
                        auth=(self.username, self.password),
                        timeout=(10, 30),
                    )
                    if 200 <= resp.status_code < 300:
                        self._last_request_at = time.monotonic()
                        return resp.json()
                    if resp.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                        self._sleep_with_cancel(backoff + random.uniform(0.0, 0.4), cancel_event)
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                else:
                    query = urllib.parse.urlencode(params or {})
                    req_url = f"{url}?{query}" if query else url
                    token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
                    req_headers = {
                        **headers,
                        "Authorization": f"Basic {token}",
                        "Accept": "application/json",
                    }
                    req = urllib.request.Request(req_url, headers=req_headers, method=method)
                    with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310
                        body = resp.read().decode("utf-8")
                        self._last_request_at = time.monotonic()
                        return json.loads(body)
            except Exception as exc:
                code = getattr(exc, "code", None)
                retryable = code in {429, 500, 502, 503, 504} or isinstance(
                    exc,
                    (
                        urllib.error.URLError,
                        TimeoutError,
                    ),
                )
                if attempt >= self.max_retries or not retryable:
                    raise
                self._sleep_with_cancel(backoff + random.uniform(0.0, 0.4), cancel_event)
                backoff *= 2
        raise RuntimeError("Unexpected retry loop exit")

    def fetch_daily_racecards(self, date: str, countries: list[str], cancel_event: Event | None = None) -> Any:
        return self._request("GET", "racecards", params={"date": date, "country": ",".join(countries)}, cancel_event=cancel_event)

    def fetch_daily_racecard_summaries(self, date: str, countries: list[str], cancel_event: Event | None = None) -> Any:
        params = {"date": date, "country": ",".join(countries)}
        try:
            return self._request("GET", "racecards/summaries", params=params, cancel_event=cancel_event)
        except Exception:
            return self.fetch_daily_racecards(date, countries, cancel_event=cancel_event)

    def fetch_daily_results(self, date: str, countries: list[str], cancel_event: Event | None = None) -> Any:
        return self._request("GET", "results", params={"date": date, "country": ",".join(countries)}, cancel_event=cancel_event)

    def probe_capabilities(self, date: str | None = None, regions: list[str] | None = None, cancel_event: Event | None = None) -> dict[str, Any]:
        probe_date = date or date_cls.today().isoformat()
        probe_regions = regions or ["gb"]
        result = {
            "auth_ok": False,
            "can_racecards": False,
            "plan_message": "",
            "rate_limit_rps": self.rate_limit_rps,
            "available_regions": [],
        }
        try:
            self.fetch_daily_results(probe_date, probe_regions, cancel_event=cancel_event)
            result["auth_ok"] = True
        except Exception as exc:
            result["plan_message"] = _capability_message(exc)
            return result

        available_regions: list[str] = []
        blocked_regions: list[str] = []
        for region in probe_regions:
            if cancel_event and cancel_event.is_set():
                raise RuntimeError("Request cancelled")
            try:
                self.fetch_daily_racecard_summaries(probe_date, [region], cancel_event=cancel_event)
                available_regions.append(region)
            except Exception as exc:
                blocked_regions.append(region)
                msg = _capability_message(exc)
                if msg and not result["plan_message"]:
                    result["plan_message"] = msg

        result["available_regions"] = available_regions
        result["can_racecards"] = bool(available_regions)
        if blocked_regions and not result["plan_message"]:
            result["plan_message"] = f"Plan limits racecards for: {', '.join(blocked_regions)}"
        if result["can_racecards"] and not result["plan_message"]:
            result["plan_message"] = "Racecard summary access available."
        return result


def _capability_message(exc: Exception) -> str:
    text = str(exc)
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            text = response.text or text
        except Exception:
            pass
    text_lower = text.lower()
    if "basic" in text_lower and "plan" in text_lower:
        return "Basic plan restriction detected for this endpoint."
    if "forbidden" in text_lower or "401" in text_lower or "403" in text_lower:
        return "Authentication failed. Check API credentials."
    if "rate" in text_lower and "limit" in text_lower:
        return "Rate-limited by API. Throttling is active at 2 requests/sec."
    return text[:200]
