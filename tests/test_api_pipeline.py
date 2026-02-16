from __future__ import annotations

import json
import sqlite3
from pathlib import Path


from racing_form_etl.api.ingest import ingest_api_day
from racing_form_etl.model.predict import generate_picks
from racing_form_etl.model.train import train_model


class MockClient:
    def __init__(self, racecards, results):
        self._racecards = racecards
        self._results = results

    def fetch_daily_racecards(self, date, countries, cancel_event=None):
        data = json.loads(json.dumps(self._racecards))
        data["data"][0]["meeting_date"] = date
        for race in data["data"][0]["races"]:
            race["scheduled_start_time"] = f"{date}T13:00:00"
        return data

    def fetch_daily_results(self, date, countries, cancel_event=None):
        return json.loads(json.dumps(self._results))


def _load_fixture(name: str):
    return json.loads((Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8"))


def test_ingest_api_day_creates_rows_and_kv(tmp_path):
    db = tmp_path / "test.sqlite"
    outdir = tmp_path / "out"
    client = MockClient(_load_fixture("racecards.json"), _load_fixture("results.json"))
    report = ingest_api_day(str(db), "2025-01-01", ["AU"], outdir=str(outdir), client=client)

    assert report["counts"]["meetings"] == 1
    assert report["counts"]["races"] == 1
    assert report["counts"]["runners"] == 2
    assert report["counts"]["results"] == 1

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM api_entities").fetchone()[0] >= 8
        assert conn.execute("SELECT COUNT(*) FROM api_kv").fetchone()[0] > 20
        key = conn.execute(
            "SELECT k FROM api_kv WHERE entity_type='runner' AND entity_id='ru1' AND k='form.last_10[0].position'"
        ).fetchone()
        assert key is not None


def test_train_predict_end_to_end(tmp_path):
    db = tmp_path / "train.sqlite"
    outdir = tmp_path / "out"
    outdir.mkdir()

    racecards = _load_fixture("racecards.json")
    results = _load_fixture("results.json")

    client = MockClient(racecards, results)
    ingest_api_day(str(db), "2025-01-01", ["AU"], outdir=str(outdir), client=client)

    racecards_2 = json.loads(json.dumps(racecards))
    racecards_2["data"][0]["meeting_id"] = "m2"
    racecards_2["data"][0]["races"][0]["race_id"] = "r2"
    racecards_2["data"][0]["races"][0]["runners"][0]["runner_id"] = "ru3"
    racecards_2["data"][0]["races"][0]["runners"][0]["runner_name"] = "Fresh Three"
    racecards_2["data"][0]["races"][0]["runners"][1]["runner_id"] = "ru4"
    racecards_2["data"][0]["races"][0]["runners"][1]["runner_name"] = "Fresh Four"

    results_2 = {"results": [{"race_id": "r2", "status": "finished", "winner_runner_id": "ru4", "finish_order": [{"runner_id": "ru4", "position": 1}, {"runner_id": "ru3", "position": 2}]}]}
    ingest_api_day(str(db), "2025-01-02", ["AU"], outdir=str(outdir), client=MockClient(racecards_2, results_2))

    report = train_model(str(db), str(outdir))
    assert (outdir / "model.pkl").exists()
    assert report["rows_train"] > 0 and report["rows_val"] > 0

    picks_csv = outdir / "picks_2025-01-02.csv"
    generate_picks(str(db), "2025-01-02", str(picks_csv), str(outdir / "model.pkl"))
    assert picks_csv.exists()

    import csv
    with open(picks_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert {"race_id", "race_no", "runner_name", "prob_win", "rank"}.issubset(rows[0].keys())
    mins = {}
    for r in rows:
        mins[r["race_id"]] = min(int(r["rank"]), mins.get(r["race_id"], 10**9))
    assert all(v == 1 for v in mins.values())
