#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Print category counts for train/val/unlabeled JSON Lines files."""

import json
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "0. data"
FILES = {
    "train": "train_data.json",
    "val": "val_data.json",
    "unlabeled_data": "unlabeled_data.json",
}


def normalize_category(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return "EMPTY_STRING"
        if cleaned.lower() == "none":
            return "STRING_NONE"
        return cleaned
    return str(value)


def count_categories(file_path: Path):
    counts = Counter()
    total = 0

    with file_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                counts["INVALID_JSON_LINE"] += 1
                continue

            category = normalize_category(doc.get("category"))
            counts[category] += 1
            total += 1

    return total, counts


def print_report(dataset_name: str, total: int, counts: Counter):
    print(f"{dataset_name}:")
    if total == 0:
        print("  - no data")
        print()
        return

    print(f"  total: {total}")
    for category, count in counts.most_common():
        print(f"  - {category}: {count}")
    print()


def main():
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

    for dataset_name, file_name in FILES.items():
        file_path = DATA_DIR / file_name
        if not file_path.exists():
            print(f"{dataset_name}:")
            print(f"  - file not found: {file_name}")
            print()
            continue

        total, counts = count_categories(file_path)
        print_report(dataset_name, total, counts)


if __name__ == "__main__":
    main()
