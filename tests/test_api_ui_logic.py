from __future__ import annotations

import sqlite3

from racing_form_etl.api.the_racing_api_client import _capability_message
from racing_form_etl.db.migrations import ensure_db
from racing_form_etl.ui.tabs.api_tab import training_readiness


class _Exc(Exception):
    pass


def test_capability_message_detects_basic_plan() -> None:
    msg = _capability_message(_Exc("Basic Plan required for racecards endpoint"))
    assert "Basic plan restriction" in msg


def test_training_readiness_requires_results(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    ensure_db(str(db))
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO api_meetings(meeting_id, meeting_date, country, venue, raw_json) VALUES('m1','2025-01-01','gb','A','{}')")
        conn.execute("INSERT INTO api_races(race_id, meeting_id, race_no, raw_json) VALUES('r1','m1',1,'{}')")
        conn.execute("INSERT INTO api_runners(runner_id, race_id, runner_name, raw_json) VALUES('ru1','r1','One','{}')")
        conn.commit()

    ok, message = training_readiness(str(db))
    assert not ok
    assert "Need >=" in message
