from __future__ import annotations

import json
import math
import pickle
from pathlib import Path
from typing import Any

from racing_form_etl.model.features import build_runner_rows


def _median(values: list[float]) -> float:
    arr = sorted(values)
    if not arr:
        return 0.0
    n = len(arr)
    return arr[n // 2] if n % 2 == 1 else (arr[n // 2 - 1] + arr[n // 2]) / 2.0


def _sigmoid(x: float) -> float:
    if x < -50:
        return 0.0
    if x > 50:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def _fit_logistic(X: list[list[float]], y: list[int], steps: int = 800, lr: float = 0.05) -> list[float]:
    if not X:
        raise ValueError("No training rows")
    n_features = len(X[0])
    w = [0.0] * (n_features + 1)
    for _ in range(steps):
        grad = [0.0] * (n_features + 1)
        for row, yi in zip(X, y):
            z = w[-1] + sum(w[j] * row[j] for j in range(n_features))
            p = _sigmoid(z)
            diff = p - yi
            for j in range(n_features):
                grad[j] += diff * row[j]
            grad[-1] += diff
        m = max(len(X), 1)
        for j in range(n_features + 1):
            w[j] -= lr * grad[j] / m
    return w


def _proba(X: list[list[float]], w: list[float]) -> list[float]:
    out = []
    for row in X:
        z = w[-1] + sum(w[j] * row[j] for j in range(len(row)))
        out.append(_sigmoid(z))
    return out


def _log_loss(y_true: list[int], y_prob: list[float]) -> float:
    eps = 1e-12
    total = 0.0
    for y, p in zip(y_true, y_prob):
        p = min(max(p, eps), 1 - eps)
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / max(len(y_true), 1)


def _roc_auc(y_true: list[int], y_prob: list[float]) -> float | None:
    pos = [(p, y) for p, y in zip(y_prob, y_true) if y == 1]
    neg = [(p, y) for p, y in zip(y_prob, y_true) if y == 0]
    if not pos or not neg:
        return None
    wins = 0.0
    ties = 0.0
    for p, _ in pos:
        for n, _ in neg:
            if p > n:
                wins += 1
            elif p == n:
                ties += 1
    return (wins + 0.5 * ties) / (len(pos) * len(neg))


def train_model(db_path: str, outdir: str = "output") -> dict[str, Any]:
    Path(outdir).mkdir(parents=True, exist_ok=True)
    rows, numeric_cols, categorical_cols, _meta = build_runner_rows(db_path)
    if not rows:
        raise ValueError("No training data available")

    dates = sorted({r["meeting_date"] for r in rows})
    if len(dates) < 2:
        raise ValueError("Need at least 2 meeting dates for train/validation split")
    val_date = dates[-1]

    train_rows = [r for r in rows if r["meeting_date"] != val_date]
    val_rows = [r for r in rows if r["meeting_date"] == val_date]

    numeric_imputers: dict[str, float] = {}
    for col in numeric_cols:
        vals = [r.get(col) for r in train_rows if isinstance(r.get(col), (int, float))]
        numeric_imputers[col] = _median([float(v) for v in vals])

    cat_vocab: dict[str, dict[str, int]] = {}
    for col in categorical_cols:
        values = sorted({str(r.get(col, "__MISSING__")) for r in train_rows})
        cat_vocab[col] = {v: i for i, v in enumerate(values)}

    ordered_features = [f"num::{c}" for c in numeric_cols]
    for col in categorical_cols:
        for key in sorted(cat_vocab[col].keys()):
            ordered_features.append(f"cat::{col}::{key}")

    def encode(dataset: list[dict[str, Any]]) -> tuple[list[list[float]], list[int]]:
        X: list[list[float]] = []
        y: list[int] = []
        for row in dataset:
            vector: list[float] = []
            for col in numeric_cols:
                value = row.get(col)
                if not isinstance(value, (int, float)):
                    value = numeric_imputers[col]
                vector.append(float(value))
            for col in categorical_cols:
                value = str(row.get(col, "__MISSING__"))
                for key in sorted(cat_vocab[col].keys()):
                    vector.append(1.0 if value == key else 0.0)
            X.append(vector)
            y.append(int(row["winner"]))
        return X, y

    X_train, y_train = encode(train_rows)
    X_val, y_val = encode(val_rows)

    weights = _fit_logistic(X_train, y_train)
    val_prob = _proba(X_val, weights)

    report = {
        "rows_train": len(train_rows),
        "rows_val": len(val_rows),
        "train_dates": dates[:-1],
        "val_date": val_date,
        "metrics": {
            "roc_auc": _roc_auc(y_val, val_prob),
            "log_loss": _log_loss(y_val, val_prob),
        },
        "numeric_feature_count": len(numeric_cols),
        "categorical_feature_count": len(categorical_cols),
        "total_feature_count": len(ordered_features),
    }

    model_payload = {
        "weights": weights,
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "numeric_imputers": numeric_imputers,
        "cat_vocab": cat_vocab,
    }
    model_path = Path(outdir) / "model.pkl"
    with model_path.open("wb") as f:
        pickle.dump(model_payload, f)
    report["model_path"] = str(model_path)

    (Path(outdir) / "train_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
