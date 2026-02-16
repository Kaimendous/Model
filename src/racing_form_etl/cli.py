from __future__ import annotations

import argparse
import json

from racing_form_etl.api.ingest import ingest_api_day
from racing_form_etl.api.the_racing_api_client import normalize_region_code
from racing_form_etl.config import get_secret_status, load_dotenv
from racing_form_etl.model.predict import generate_picks
from racing_form_etl.model.train import train_model


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m racing_form_etl")
    sub = parser.add_subparsers(dest="mode", required=True)

    api = sub.add_parser("api")
    api_sub = api.add_subparsers(dest="cmd", required=True)

    api_sub.add_parser("status")

    ingest = api_sub.add_parser("ingest")
    ingest.add_argument("--db", required=True)
    ingest.add_argument("--date", required=True)
    ingest.add_argument("--region", dest="regions", action="append", default=[])
    ingest.add_argument("--country", dest="regions", action="append", default=[])
    ingest.add_argument("--outdir", default="output")

    train = api_sub.add_parser("train")
    train.add_argument("--db", required=True)
    train.add_argument("--outdir", default="output")

    picks = api_sub.add_parser("picks")
    picks.add_argument("--db", required=True)
    picks.add_argument("--date", required=True)
    picks.add_argument("--out", required=True)
    picks.add_argument("--model", default="output/model.pkl")

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)

    if args.mode == "api" and args.cmd == "status":
        status = get_secret_status()
        for k, v in status.items():
            print(f"{k}: {'configured' if v else 'missing'}")
        return 0

    if args.mode == "api" and args.cmd == "ingest":
        regions = [normalize_region_code(region) for region in args.regions]
        report = ingest_api_day(args.db, args.date, regions, outdir=args.outdir)
        print(json.dumps(report, indent=2))
        return 0

    if args.mode == "api" and args.cmd == "train":
        report = train_model(args.db, args.outdir)
        print(json.dumps(report, indent=2))
        return 0

    if args.mode == "api" and args.cmd == "picks":
        path = generate_picks(args.db, args.date, args.out, args.model)
        print(path)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
