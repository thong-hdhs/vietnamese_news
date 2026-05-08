#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pickle
from pathlib import Path

import numpy as np
from scipy.sparse import csr_matrix, hstack
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "0. data"

TRAIN_FILE = DATA_DIR / "train_data.json"
VAL_FILE = DATA_DIR / "val_data.json"
UNLABELED_FILE = DATA_DIR / "unlabeled_data.json"

TFIDF_VECTORIZER_FILE = DATA_DIR / "tfidf_vectorizer.pkl"
SVD_MODEL_FILE = DATA_DIR / "svd_model.pkl"
X_TRAIN_FILE = DATA_DIR / "X_train.pkl"
X_VAL_FILE = DATA_DIR / "X_val.pkl"
X_UNLABELED_FILE = DATA_DIR / "X_unlabeled.pkl"
Y_TRAIN_FILE = DATA_DIR / "y_train.pkl"
Y_VAL_FILE = DATA_DIR / "y_val.pkl"
STATS_FILE = DATA_DIR / "feature_stats.json"

# Tuned values from current project data
TFIDF_MAX_FEATURES = 3000
SVD_COMPONENTS = 300
SITE_DIM = 4


def read_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def tokens_to_text(tokens):
    if isinstance(tokens, list):
        return " ".join(tokens)
    return ""


def normalize_site_onehot(value):
    if isinstance(value, list):
        vec = [int(x) for x in value[:SITE_DIM]]
    else:
        vec = []
    if len(vec) < SITE_DIM:
        vec += [0] * (SITE_DIM - len(vec))
    return vec


def extract_dataset(rows, expect_label):
    x_raw = []
    site_features = []
    labels = []

    for row in rows:
        x_raw.append(tokens_to_text(row.get("full_text_tokens", [])))
        site_features.append(normalize_site_onehot(row.get("site_onehot")))
        if expect_label:
            labels.append(int(row.get("category_encoded", -1)))

    site_array = np.array(site_features, dtype=np.float32)
    if expect_label:
        y = np.array(labels, dtype=np.int32)
        return x_raw, site_array, y
    return x_raw, site_array, None


def save_pickle(path: Path, obj):
    with path.open("wb") as f:
        pickle.dump(obj, f)


def main():
    print("Loading split files...")
    train_rows = read_jsonl(TRAIN_FILE)
    val_rows = read_jsonl(VAL_FILE)
    unlabeled_rows = read_jsonl(UNLABELED_FILE)

    x_train_raw, site_train, y_train = extract_dataset(train_rows, expect_label=True)
    x_val_raw, site_val, y_val = extract_dataset(val_rows, expect_label=True)
    x_unlabeled_raw, site_unlabeled, _ = extract_dataset(unlabeled_rows, expect_label=False)

    if np.any(y_train < 0) or np.any(y_val < 0):
        raise ValueError("Train/Val contains unlabeled rows (category_encoded < 0).")

    print(f"Train/Val/Unlabeled sizes: {len(x_train_raw)}/{len(x_val_raw)}/{len(x_unlabeled_raw)}")

    print("Step 1: TF-IDF fit on train...")
    tfidf = TfidfVectorizer(
        max_features=TFIDF_MAX_FEATURES,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.8,
        sublinear_tf=True,
        token_pattern=r"\S+",
        lowercase=False,
    )
    x_train_tfidf = tfidf.fit_transform(x_train_raw)

    print("Step 2: SVD fit on train TF-IDF...")
    max_valid_components = min(x_train_tfidf.shape[0] - 1, x_train_tfidf.shape[1] - 1)
    use_components = min(SVD_COMPONENTS, max_valid_components)
    if use_components < 2:
        raise ValueError(f"Invalid SVD components: {use_components}")

    svd = TruncatedSVD(n_components=use_components, random_state=42)
    x_train_svd = svd.fit_transform(x_train_tfidf)

    print("Step 3: Transform val + unlabeled with fitted transformers...")
    x_val_svd = svd.transform(tfidf.transform(x_val_raw))
    x_unlabeled_svd = svd.transform(tfidf.transform(x_unlabeled_raw))

    print("Step 4: Concat site_onehot (4D) after SVD...")
    x_train_final = hstack([csr_matrix(x_train_svd), csr_matrix(site_train)], format="csr")
    x_val_final = hstack([csr_matrix(x_val_svd), csr_matrix(site_val)], format="csr")
    x_unlabeled_final = hstack(
        [csr_matrix(x_unlabeled_svd), csr_matrix(site_unlabeled)], format="csr"
    )

    print("Step 5: Save artifacts...")
    save_pickle(TFIDF_VECTORIZER_FILE, tfidf)
    save_pickle(SVD_MODEL_FILE, svd)
    save_pickle(X_TRAIN_FILE, x_train_final)
    save_pickle(X_VAL_FILE, x_val_final)
    save_pickle(X_UNLABELED_FILE, x_unlabeled_final)
    save_pickle(Y_TRAIN_FILE, y_train)
    save_pickle(Y_VAL_FILE, y_val)

    stats = {
        "input_files": {
            "train": str(TRAIN_FILE),
            "val": str(VAL_FILE),
            "unlabeled": str(UNLABELED_FILE),
        },
        "config": {
            "tfidf_max_features": TFIDF_MAX_FEATURES,
            "tfidf_ngram_range": [1, 2],
            "svd_n_components": int(use_components),
            "site_onehot_dim": SITE_DIM,
        },
        "transform_policy": {
            "fit_on_train_only": True,
            "transform_on_val_unlabeled_only": True,
        },
        "output_shapes": {
            "X_train": list(x_train_final.shape),
            "X_val": list(x_val_final.shape),
            "X_unlabeled": list(x_unlabeled_final.shape),
            "y_train": list(y_train.shape),
            "y_val": list(y_val.shape),
        },
        "tfidf_vocab_size": int(len(tfidf.vocabulary_)),
        "svd_explained_variance_sum": float(svd.explained_variance_ratio_.sum()),
    }
    with STATS_FILE.open("w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"X_train shape: {x_train_final.shape}")
    print(f"X_val shape: {x_val_final.shape}")
    print(f"X_unlabeled shape: {x_unlabeled_final.shape}")
    print(f"Saved stats: {STATS_FILE}")


if __name__ == "__main__":
    main()
