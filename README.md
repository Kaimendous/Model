# Racing Form ETL

API-first TheRacingAPI vertical slice pipeline: ingest -> SQLite -> auto-features -> train -> picks, plus Tk UI.

## Configure secrets (PowerShell)

```powershell
$env:THERACINGAPI_USERNAME = "your_username"
$env:THERACINGAPI_PASSWORD = "your_password"
$env:THERACINGAPI_API_KEY = "optional_api_key"
```

Check status (never prints secret values):

```powershell
python -m racing_form_etl api status
```

`.env` is gitignored and should never be committed. In UI, you can click **Apply** and optionally **Save to local .env**.

## CLI usage

```powershell
python -m racing_form_etl api ingest --db output\racing.sqlite --date 2025-01-01 --country AU --country US --outdir output
python -m racing_form_etl api train --db output\racing.sqlite --outdir output
python -m racing_form_etl api picks --db output\racing.sqlite --date 2025-01-01 --out output\picks_2025-01-01.csv --model output\model.pkl
```

## UI

```powershell
python -c "from racing_form_etl.ui.app import main; raise SystemExit(main())"
```

UI work is background-threaded; updates are delivered with queue + Tk `after()`.

## Tests

```powershell
pytest
```

Tests are network-free and use fixture payloads + mocked client methods.
