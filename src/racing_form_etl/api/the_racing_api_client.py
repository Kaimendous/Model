from __future__ import annotations

import base64
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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

    def fetch_daily_results(self, date: str, countries: list[str], cancel_event: Event | None = None) -> Any:
        return self._request("GET", "results", params={"date": date, "country": ",".join(countries)}, cancel_event=cancel_event)
