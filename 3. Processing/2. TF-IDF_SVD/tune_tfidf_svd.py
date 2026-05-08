#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from pathlib import Path

import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score


DATA_DIR = Path(__file__).resolve().parent / "0. data"
TRAIN_FILE = DATA_DIR / "train_data.json"
VAL_FILE = DATA_DIR / "val_data.json"

TFIDF_CANDIDATES = [3000, 5000, 8000, 12000]
SVD_CANDIDATES = [100, 200, 300, 400, 500]


def read_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def to_text(tokens):
    if isinstance(tokens, list):
        return " ".join(tokens)
    return ""


def load_xy(path: Path):
    docs = read_jsonl(path)
    x_raw = [to_text(d.get("full_text_tokens", [])) for d in docs]
    y = np.array([int(d.get("category_encoded", -1)) for d in docs], dtype=np.int32)
    return x_raw, y


def main():
    print("Loading train/val data...")
    x_train_raw, y_train = load_xy(TRAIN_FILE)
    x_val_raw, y_val = load_xy(VAL_FILE)

    if np.any(y_train < 0) or np.any(y_val < 0):
        raise ValueError("Found unlabeled rows in train/val (category_encoded = -1).")

    print(f"Train size: {len(x_train_raw)}, Val size: {len(x_val_raw)}")

    results = []

    for max_features in TFIDF_CANDIDATES:
        for n_components in SVD_CANDIDATES:
            t0 = time.time()
            tfidf = TfidfVectorizer(
                max_features=max_features,
                ngram_range=(1, 2),
                sublinear_tf=True,
                min_df=2,
                max_df=0.8,
                token_pattern=r"\S+",
                lowercase=False,
            )

            x_train_tfidf = tfidf.fit_transform(x_train_raw)
            x_val_tfidf = tfidf.transform(x_val_raw)

            max_valid_components = min(x_train_tfidf.shape[0] - 1, x_train_tfidf.shape[1] - 1)
            use_components = min(n_components, max_valid_components)
            if use_components < 2:
                continue

            svd = TruncatedSVD(n_components=use_components, random_state=42)
            x_train = svd.fit_transform(x_train_tfidf)
            x_val = svd.transform(x_val_tfidf)

            clf = LogisticRegression(
                max_iter=3000,
                random_state=42,
            )
            clf.fit(x_train, y_train)
            y_pred = clf.predict(x_val)
            macro_f1 = f1_score(y_val, y_pred, average="macro")

            elapsed = time.time() - t0
            row = {
                "tfidf_max_features": max_features,
                "svd_n_components": use_components,
                "macro_f1": float(macro_f1),
                "explained_variance_sum": float(svd.explained_variance_ratio_.sum()),
                "elapsed_sec": round(elapsed, 2),
            }
            results.append(row)
            print(
                f"TFIDF={max_features:5d} | SVD={use_components:3d} "
                f"| macro_f1={macro_f1:.4f} | evr={row['explained_variance_sum']:.4f} "
                f"| {elapsed:.1f}s"
            )

    if not results:
        raise RuntimeError("No valid tuning result generated.")

    results.sort(key=lambda r: (r["macro_f1"], r["explained_variance_sum"]), reverse=True)
    best = results[0]

    print("\n=== TOP 5 CONFIGS ===")
    for i, r in enumerate(results[:5], 1):
        print(
            f"{i}. TFIDF={r['tfidf_max_features']}, SVD={r['svd_n_components']}, "
            f"macro_f1={r['macro_f1']:.4f}, evr={r['explained_variance_sum']:.4f}"
        )

    out_path = DATA_DIR / "tfidf_svd_tuning_results.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"best": best, "results": results}, f, ensure_ascii=False, indent=2)

    print("\n=== BEST ===")
    print(best)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
