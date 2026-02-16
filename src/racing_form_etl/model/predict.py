from __future__ import annotations

import csv
import math
import pickle
from pathlib import Path

from racing_form_etl.model.features import rows_for_date


def _sigmoid(x: float) -> float:
    if x < -50:
        return 0.0
    if x > 50:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def generate_picks(db_path: str, date: str, out_csv: str, model_path: str = "output/model.pkl") -> str:
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    rows, _num, _cat, metadata = rows_for_date(db_path, date)
    if not rows:
        raise ValueError(f"No runner rows for date {date}")

    numeric_cols = model["numeric_cols"]
    categorical_cols = model["categorical_cols"]
    numeric_imputers = model["numeric_imputers"]
    cat_vocab = model["cat_vocab"]
    weights = model["weights"]

    probs = []
    for row in rows:
        vector: list[float] = []
        for col in numeric_cols:
            v = row.get(col)
            if not isinstance(v, (int, float)):
                v = numeric_imputers[col]
            vector.append(float(v))
        for col in categorical_cols:
            value = str(row.get(col, "__MISSING__"))
            for key in sorted(cat_vocab[col].keys()):
                vector.append(1.0 if value == key else 0.0)
        z = weights[-1] + sum(weights[j] * vector[j] for j in range(len(vector)))
        probs.append(_sigmoid(z))

    enriched = []
    for meta, p in zip(metadata, probs):
        enriched.append({**meta, "prob_win": p})

    grouped: dict[str, list[dict]] = {}
    for row in enriched:
        grouped.setdefault(row["race_id"], []).append(row)

    final_rows: list[dict] = []
    for _race_id, group in grouped.items():
        ordered = sorted(group, key=lambda r: r["prob_win"], reverse=True)
        for idx, row in enumerate(ordered, start=1):
            row["rank"] = idx
            final_rows.append(row)

    final_rows.sort(key=lambda r: (r["race_id"], r["rank"]))

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["race_id", "race_no", "runner_name", "prob_win", "rank"])
        writer.writeheader()
        for row in final_rows:
            writer.writerow({k: row[k] for k in writer.fieldnames})
    return out_csv
