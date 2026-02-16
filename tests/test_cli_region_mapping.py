from __future__ import annotations

from racing_form_etl import cli


def test_cli_country_flag_maps_to_regions(monkeypatch) -> None:
    captured = {}

    def fake_ingest(db, date, regions, outdir="output"):
        captured["db"] = db
        captured["date"] = date
        captured["regions"] = regions
        captured["outdir"] = outdir
        return {"ok": True}

    monkeypatch.setattr(cli, "ingest_api_day", fake_ingest)

    rc = cli.main([
        "api",
        "ingest",
        "--db",
        "output/racing.sqlite",
        "--date",
        "2026-02-15",
        "--country",
        "GB",
        "--country",
        "IRE",
    ])

    assert rc == 0
    assert captured["regions"] == ["gb", "ire"]


def test_cli_region_flag_supported(monkeypatch) -> None:
    captured = {}

    def fake_ingest(db, date, regions, outdir="output"):
        captured["regions"] = regions
        return {"ok": True}

    monkeypatch.setattr(cli, "ingest_api_day", fake_ingest)

    rc = cli.main([
        "api",
        "ingest",
        "--db",
        "output/racing.sqlite",
        "--date",
        "2026-02-15",
        "--region",
        "HK",
    ])

    assert rc == 0
    assert captured["regions"] == ["hk"]
