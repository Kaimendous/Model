from __future__ import annotations

import sqlite3
from pathlib import Path


TABLES = {
    "api_meetings": """
        CREATE TABLE IF NOT EXISTS api_meetings (
            meeting_id TEXT PRIMARY KEY,
            meeting_date TEXT,
            country TEXT,
            venue TEXT,
            raw_json TEXT
        )
    """,
    "api_races": """
        CREATE TABLE IF NOT EXISTS api_races (
            race_id TEXT PRIMARY KEY,
            meeting_id TEXT,
            race_no INTEGER,
            scheduled_start_time TEXT,
            distance INTEGER,
            class TEXT,
            raw_json TEXT
        )
    """,
    "api_runners": """
        CREATE TABLE IF NOT EXISTS api_runners (
            runner_id TEXT PRIMARY KEY,
            race_id TEXT,
            runner_name TEXT,
            barrier INTEGER,
            weight REAL,
            jockey TEXT,
            trainer TEXT,
            raw_json TEXT
        )
    """,
    "api_results": """
        CREATE TABLE IF NOT EXISTS api_results (
            race_id TEXT PRIMARY KEY,
            status TEXT,
            winner_runner_id TEXT,
            finish_order_json TEXT,
            raw_json TEXT
        )
    """,
    "api_entities": """
        CREATE TABLE IF NOT EXISTS api_entities (
            entity_type TEXT,
            entity_id TEXT,
            parent_id TEXT,
            meeting_date TEXT,
            country TEXT,
            raw_json TEXT,
            PRIMARY KEY(entity_type, entity_id)
        )
    """,
    "api_kv": """
        CREATE TABLE IF NOT EXISTS api_kv (
            entity_type TEXT,
            entity_id TEXT,
            k TEXT,
            v_text TEXT,
            v_num REAL,
            v_json TEXT,
            PRIMARY KEY(entity_type, entity_id, k)
        )
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_api_meetings_date_country ON api_meetings(meeting_date, country)",
    "CREATE INDEX IF NOT EXISTS idx_api_races_meeting_id ON api_races(meeting_id)",
    "CREATE INDEX IF NOT EXISTS idx_api_races_start ON api_races(scheduled_start_time)",
    "CREATE INDEX IF NOT EXISTS idx_api_runners_race_id ON api_runners(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_api_results_race_id ON api_results(race_id)",
    "CREATE INDEX IF NOT EXISTS idx_api_entities_date_country ON api_entities(meeting_date, country)",
    "CREATE INDEX IF NOT EXISTS idx_api_kv_entity_type_k ON api_kv(entity_type, k)",
    "CREATE INDEX IF NOT EXISTS idx_api_kv_entity_id ON api_kv(entity_id)",
]

EXPECTED_COLUMNS = {
    "api_races": {
        "class": "TEXT",
    }
}


def ensure_db(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for ddl in TABLES.values():
            cur.execute(ddl)
        for table, cols in EXPECTED_COLUMNS.items():
            existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
            for col, typ in cols.items():
                if col not in existing:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        for idx in INDEXES:
            cur.execute(idx)
        conn.commit()
