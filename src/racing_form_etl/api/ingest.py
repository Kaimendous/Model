from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import Event
from typing import Any, Callable

from racing_form_etl.api.the_racing_api_client import TheRacingAPIClient
from racing_form_etl.config import get_api_credentials
from racing_form_etl.db.migrations import ensure_db


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def _get_id(d: dict[str, Any], fallback_keys: list[str]) -> str | None:
    for k in ["id", "uuid", "runner_id", "race_id", "meeting_id", *fallback_keys]:
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            return str(v)
    return None


def _extract_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["data", "meetings", "races", "results", "racecards"]:
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def flatten_json(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.update(flatten_json(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            out.update(flatten_json(v, key))
        if prefix and not obj:
            out[prefix] = []
    else:
        out[prefix] = obj
    return out


def _typed_values(value: Any) -> tuple[str | None, float | None, str | None]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return None, float(value), None
    if isinstance(value, str):
        return value, None, None
    return None, None, _compact_json(value)


def _upsert_entity(
    conn: sqlite3.Connection,
    entity_type: str,
    entity_id: str,
    raw: dict[str, Any],
    parent_id: str | None,
    meeting_date: str | None,
    country: str | None,
) -> int:
    conn.execute(
        """
        INSERT INTO api_entities(entity_type, entity_id, parent_id, meeting_date, country, raw_json)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(entity_type, entity_id) DO UPDATE SET
            parent_id=excluded.parent_id,
            meeting_date=excluded.meeting_date,
            country=excluded.country,
            raw_json=excluded.raw_json
        """,
        (entity_type, entity_id, parent_id, meeting_date, country, _compact_json(raw)),
    )
    flat = flatten_json(raw)
    for k, v in flat.items():
        v_text, v_num, v_json = _typed_values(v)
        conn.execute(
            """
            INSERT INTO api_kv(entity_type, entity_id, k, v_text, v_num, v_json)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(entity_type, entity_id, k) DO UPDATE SET
              v_text=excluded.v_text,
              v_num=excluded.v_num,
              v_json=excluded.v_json
            """,
            (entity_type, entity_id, k, v_text, v_num, v_json),
        )
    return len(flat)


def _infer_entity_type(obj: dict[str, Any]) -> str | None:
    name = str(obj.get("type") or obj.get("entity_type") or "").lower()
    if name in {"horse", "trainer", "jockey", "runner", "race", "meeting"}:
        return name
    for k in ("horse", "trainer", "jockey"):
        if any(x.startswith(f"{k}_") for x in obj.keys()) or k in obj:
            return k
    return None


def ingest_api_day(
    db_path: str,
    date: str,
    regions: list[str],
    cancel_event: Event | None = None,
    outdir: str = "output",
    client: TheRacingAPIClient | None = None,
    progress_cb: Callable[[str, float], None] | None = None,
    minimal_payload: bool = False,
) -> dict[str, Any]:
    ensure_db(db_path)
    Path(outdir).mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "date": date,
        "regions": regions,
        "counts": {
            "meetings": 0,
            "races": 0,
            "runners": 0,
            "results": 0,
            "entities": 0,
            "kv_pairs": 0,
        },
        "errors": [],
    }

    username, password, api_key = get_api_credentials()
    api_client = client or TheRacingAPIClient(username=username or "", password=password or "", api_key=api_key)
    if progress_cb:
        progress_cb("Auth check", 10)
        progress_cb("Discover", 20)

    racecards = (
        api_client.fetch_daily_racecard_summaries(date, regions, cancel_event=cancel_event)
        if minimal_payload
        else api_client.fetch_daily_racecards(date, regions, cancel_event=cancel_event)
    )
    if progress_cb:
        progress_cb("Fetch summaries", 45)
    if cancel_event and cancel_event.is_set():
        return report
    results: Any = {"results": []}
    try:
        results = api_client.fetch_daily_results(date, regions, cancel_event=cancel_event)
    except Exception as exc:
        if "standard" in str(exc).lower() and "plan" in str(exc).lower():
            report["errors"].append("Results endpoint unavailable on current plan; ingesting racecards only.")
        else:
            raise
    if progress_cb:
        progress_cb("Fetch details", 65)

    meetings = _extract_list(racecards)
    results_rows = _extract_list(results)

    with sqlite3.connect(db_path) as conn:
        for meeting in meetings:
            if cancel_event and cancel_event.is_set():
                break
            meeting_id = _get_id(meeting, ["meeting_id", "track_id", "venue_id"]) or f"m_{date}_{report['counts']['meetings']}"
            meeting_date = str(meeting.get("meeting_date") or meeting.get("date") or date)
            country = str(meeting.get("country") or (regions[0] if regions else ""))
            venue = str(meeting.get("venue") or meeting.get("track") or "")
            conn.execute(
                """
                INSERT INTO api_meetings(meeting_id, meeting_date, country, venue, raw_json)
                VALUES(?,?,?,?,?)
                ON CONFLICT(meeting_id) DO UPDATE SET
                    meeting_date=excluded.meeting_date,
                    country=excluded.country,
                    venue=excluded.venue,
                    raw_json=excluded.raw_json
                """,
                (meeting_id, meeting_date, country, venue, _compact_json(meeting)),
            )
            report["counts"]["meetings"] += 1
            report["counts"]["entities"] += 1
            report["counts"]["kv_pairs"] += _upsert_entity(conn, "meeting", meeting_id, meeting, None, meeting_date, country)

            races = meeting.get("races") if isinstance(meeting.get("races"), list) else []
            for race in races:
                if not isinstance(race, dict):
                    continue
                race_id = _get_id(race, ["race_id"]) or f"{meeting_id}_r{report['counts']['races'] + 1}"
                conn.execute(
                    """
                    INSERT INTO api_races(race_id, meeting_id, race_no, scheduled_start_time, distance, class, raw_json)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(race_id) DO UPDATE SET
                      meeting_id=excluded.meeting_id,
                      race_no=excluded.race_no,
                      scheduled_start_time=excluded.scheduled_start_time,
                      distance=excluded.distance,
                      class=excluded.class,
                      raw_json=excluded.raw_json
                    """,
                    (
                        race_id,
                        meeting_id,
                        race.get("race_no") or race.get("number"),
                        race.get("scheduled_start_time") or race.get("off_time") or race.get("start_time"),
                        race.get("distance"),
                        race.get("class") or race.get("grade"),
                        _compact_json(race),
                    ),
                )
                report["counts"]["races"] += 1
                report["counts"]["entities"] += 1
                report["counts"]["kv_pairs"] += _upsert_entity(conn, "race", race_id, race, meeting_id, meeting_date, country)

                runners = race.get("runners") if isinstance(race.get("runners"), list) else []
                for runner in runners:
                    if not isinstance(runner, dict):
                        continue
                    runner_id = _get_id(runner, ["runner_id", "horse_id"]) or f"{race_id}_ru{report['counts']['runners'] + 1}"
                    conn.execute(
                        """
                        INSERT INTO api_runners(runner_id, race_id, runner_name, barrier, weight, jockey, trainer, raw_json)
                        VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(runner_id) DO UPDATE SET
                            race_id=excluded.race_id,
                            runner_name=excluded.runner_name,
                            barrier=excluded.barrier,
                            weight=excluded.weight,
                            jockey=excluded.jockey,
                            trainer=excluded.trainer,
                            raw_json=excluded.raw_json
                        """,
                        (
                            runner_id,
                            race_id,
                            runner.get("runner_name") or runner.get("name"),
                            runner.get("barrier") or runner.get("draw"),
                            runner.get("weight"),
                            runner.get("jockey") if isinstance(runner.get("jockey"), str) else (runner.get("jockey") or {}).get("name"),
                            runner.get("trainer") if isinstance(runner.get("trainer"), str) else (runner.get("trainer") or {}).get("name"),
                            _compact_json(runner),
                        ),
                    )
                    report["counts"]["runners"] += 1
                    report["counts"]["entities"] += 1
                    report["counts"]["kv_pairs"] += _upsert_entity(conn, "runner", runner_id, runner, race_id, meeting_date, country)

                    for key, value in runner.items():
                        if isinstance(value, dict):
                            inferred = _infer_entity_type(value) or key.lower()
                            nested_id = _get_id(value, [f"{inferred}_id", "id"]) or f"{runner_id}_{key}"
                            report["counts"]["entities"] += 1
                            report["counts"]["kv_pairs"] += _upsert_entity(conn, inferred, nested_id, value, runner_id, meeting_date, country)

        for row in results_rows:
            if not isinstance(row, dict):
                continue
            race_id = _get_id(row, ["race_id"])
            if not race_id:
                continue
            finish_order = row.get("finish_order") or row.get("placing") or row.get("positions") or []
            winner = row.get("winner_runner_id")
            if winner is None and isinstance(finish_order, list) and finish_order:
                top = finish_order[0]
                winner = top.get("runner_id") if isinstance(top, dict) else top
            conn.execute(
                """
                INSERT INTO api_results(race_id, status, winner_runner_id, finish_order_json, raw_json)
                VALUES(?,?,?,?,?)
                ON CONFLICT(race_id) DO UPDATE SET
                    status=excluded.status,
                    winner_runner_id=excluded.winner_runner_id,
                    finish_order_json=excluded.finish_order_json,
                    raw_json=excluded.raw_json
                """,
                (race_id, row.get("status"), str(winner) if winner is not None else None, _compact_json(finish_order), _compact_json(row)),
            )
            report["counts"]["results"] += 1
            report["counts"]["entities"] += 1
            report["counts"]["kv_pairs"] += _upsert_entity(conn, "result", race_id, row, race_id, date, None)
        conn.commit()
    if progress_cb:
        progress_cb("Write DB", 90)

    out_path = Path(outdir) / f"ingest_report_{date}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if progress_cb:
        progress_cb("Report", 100)
    return report
