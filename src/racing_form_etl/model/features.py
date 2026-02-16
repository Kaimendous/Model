from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime
from typing import Any

FINISHED_STATUSES = {"finished", "official", "complete", "completed", "resulted"}


def _to_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _hour(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).hour
    except ValueError:
        return None


def _pick_kv_features(conn: sqlite3.Connection, min_non_null: float = 0.6) -> tuple[list[str], list[str]]:
    rows = conn.execute(
        """
        SELECT entity_type, k,
               SUM(CASE WHEN v_num IS NOT NULL THEN 1 ELSE 0 END) AS n_num,
               SUM(CASE WHEN v_text IS NOT NULL THEN 1 ELSE 0 END) AS n_text,
               COUNT(*) AS n_total,
               COUNT(DISTINCT v_text) AS n_text_unique,
               COUNT(DISTINCT v_num) AS n_num_unique
        FROM api_kv
        WHERE entity_type IN ('runner','horse','race','meeting')
        GROUP BY entity_type, k
        """
    ).fetchall()
    numeric, categorical = [], []
    bad_tokens = {"id", "name", "raw", "json", "uuid", "description", "comment"}
    for entity_type, key, n_num, n_text, n_total, n_text_unique, n_num_unique in rows:
        key = str(key)
        non_null = max(n_num, n_text) / max(n_total, 1)
        if non_null < min_non_null or any(tok in key.lower() for tok in bad_tokens):
            continue
        label = f"{entity_type}:{key}"
        if n_num >= n_text and n_num_unique > 1:
            numeric.append(label)
        elif n_text_unique > 1 and n_text_unique <= 100:
            categorical.append(label)
    return sorted(set(numeric)), sorted(set(categorical))


def build_runner_rows(db_path: str, include_unfinished: bool = False) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, Any]]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        records = conn.execute(
            """
            SELECT r.runner_id, r.runner_name, r.race_id, rc.meeting_id, rc.race_no, rc.distance,
                   rc.class AS race_class, rc.scheduled_start_time, m.venue, m.meeting_date,
                   m.country, r.barrier, r.weight, rs.status, rs.winner_runner_id
            FROM api_runners r
            JOIN api_races rc ON rc.race_id=r.race_id
            JOIN api_meetings m ON m.meeting_id=rc.meeting_id
            LEFT JOIN api_results rs ON rs.race_id=r.race_id
            """
        ).fetchall()
        if not records:
            return [], [], [], []

        field_size = Counter([r["race_id"] for r in records])
        numeric_kv, categorical_kv = _pick_kv_features(conn)

        kv_rows = conn.execute(
            """
            SELECT entity_type, entity_id, k, v_num, v_text
            FROM api_kv
            WHERE entity_type IN ('runner','horse','race','meeting')
            """
        ).fetchall()
        kv_map: dict[tuple[str, str, str], tuple[Any, Any]] = {}
        for kvr in kv_rows:
            kv_map[(kvr["entity_type"], str(kvr["entity_id"]), str(kvr["k"]))] = (kvr["v_num"], kvr["v_text"])

        horse_links = dict(
            conn.execute("SELECT parent_id, entity_id FROM api_entities WHERE entity_type='horse' AND parent_id IS NOT NULL").fetchall()
        )

    rows: list[dict[str, Any]] = []
    for r in records:
        status = str(r["status"] or "").lower()
        if not include_unfinished and (status not in FINISHED_STATUSES or not r["winner_runner_id"]):
            continue
        row: dict[str, Any] = {
            "runner_id": str(r["runner_id"]),
            "runner_name": r["runner_name"],
            "race_id": str(r["race_id"]),
            "meeting_id": str(r["meeting_id"]),
            "race_no": r["race_no"],
            "meeting_date": str(r["meeting_date"]),
            "distance": _to_float(r["distance"]),
            "barrier": _to_float(r["barrier"]),
            "weight": _to_float(r["weight"]),
            "field_size": float(field_size[str(r["race_id"])]),
            "start_hour": _to_float(_hour(r["scheduled_start_time"])),
            "race_class": str(r["race_class"] or "UNKNOWN"),
            "venue": str(r["venue"] or "UNKNOWN"),
            "winner": int(str(r["runner_id"]) == str(r["winner_runner_id"])) if r["winner_runner_id"] else 0,
        }

        for label in numeric_kv:
            entity_type, key = label.split(":", 1)
            entity_id = row["runner_id"] if entity_type == "runner" else row["race_id"] if entity_type == "race" else row["meeting_id"] if entity_type == "meeting" else horse_links.get(row["runner_id"])
            if entity_id:
                row[f"kv_num:{label}"] = _to_float(kv_map.get((entity_type, str(entity_id), key), (None, None))[0])

        for label in categorical_kv:
            entity_type, key = label.split(":", 1)
            entity_id = row["runner_id"] if entity_type == "runner" else row["race_id"] if entity_type == "race" else row["meeting_id"] if entity_type == "meeting" else horse_links.get(row["runner_id"])
            if entity_id:
                row[f"kv_cat:{label}"] = kv_map.get((entity_type, str(entity_id), key), (None, None))[1]

        rows.append(row)

    metadata = [
        {
            "race_id": row["race_id"],
            "race_no": row["race_no"],
            "runner_id": row["runner_id"],
            "runner_name": row["runner_name"],
            "meeting_date": row["meeting_date"],
        }
        for row in rows
    ]

    numeric_cols = ["distance", "barrier", "weight", "field_size", "start_hour"] + [k for k in rows[0].keys() if k.startswith("kv_num:")] if rows else []
    categorical_cols = ["race_class", "venue"] + [k for k in rows[0].keys() if k.startswith("kv_cat:")] if rows else []
    return rows, sorted(set(numeric_cols)), sorted(set(categorical_cols)), metadata


def rows_for_date(db_path: str, date: str) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, Any]]]:
    rows, num, cat, meta = build_runner_rows(db_path, include_unfinished=True)
    filt = [i for i, row in enumerate(rows) if row["meeting_date"] == date]
    return [rows[i] for i in filt], num, cat, [meta[i] for i in filt]
