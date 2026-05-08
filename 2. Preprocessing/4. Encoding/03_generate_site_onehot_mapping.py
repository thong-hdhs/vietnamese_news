#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate canonical site_onehot mapping from MongoDB.
- Reads distinct `site` values from news_data_preprocessing
- Excludes Tuoi Tre variants
- Saves JSON mapping file only (no vectorization)
"""

import json
from datetime import datetime
from pymongo import MongoClient

MONGODB_URI = (
    "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/"
    "?appName=Cluster0&retryWrites=true&w=majority"
)
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_preprocessing"
OUTPUT_FILE = "site_onehot_mapping.json"


def is_tuoitre(site_name: str) -> bool:
    normalized = site_name.lower().replace(" ", "").replace("-", "").replace("_", "")
    return normalized in {"tuoitre", "tuoi-tre", "tuoi_tre"}


def main() -> None:
    client = MongoClient(MONGODB_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    raw_sites = [
        s
        for s in collection.distinct("site")
        if isinstance(s, str) and s.strip() and s.strip().lower() != "none"
    ]
    unique_sites = sorted([s.strip() for s in raw_sites if not is_tuoitre(s)])

    site_to_index = {site: idx for idx, site in enumerate(unique_sites)}
    index_to_site = {idx: site for site, idx in site_to_index.items()}

    onehot_vectors = {}
    for site, idx in site_to_index.items():
        vector = [0] * len(unique_sites)
        vector[idx] = 1
        onehot_vectors[site] = vector

    payload = {
        "schema_version": "site_onehot_v2",
        "generated_at": datetime.now().isoformat(),
        "num_sites": len(unique_sites),
        "sites": unique_sites,
        "site_to_index": site_to_index,
        "index_to_site": index_to_site,
        "site_to_onehot": onehot_vectors,
        "excluded_sites": ["tuoitre"],
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    print(f"Saved: {OUTPUT_FILE}")
    print(f"Sites ({len(unique_sites)}): {', '.join(unique_sites)}")
    print("One-hot length:", len(unique_sites))

    client.close()


if __name__ == "__main__":
    main()
