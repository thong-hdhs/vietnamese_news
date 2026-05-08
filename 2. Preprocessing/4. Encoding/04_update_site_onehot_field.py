#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update MongoDB field `site_onehot` using canonical mapping JSON.
"""

import json
from pathlib import Path
from pymongo import MongoClient, UpdateOne

MONGODB_URI = (
    "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/"
    "?appName=Cluster0&retryWrites=true&w=majority"
)
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_preprocessing"
MAPPING_PATH = Path(__file__).resolve().parent / "site_onehot_mapping.json"


def normalize_site(site_value: str) -> str:
    return site_value.strip().lower()


def main() -> None:
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping file not found: {MAPPING_PATH}")

    with open(MAPPING_PATH, "r", encoding="utf-8") as file:
        mapping = json.load(file)

    site_to_onehot = mapping.get("site_to_onehot", {})
    onehot_length = int(mapping.get("num_sites", 0))
    if not site_to_onehot or onehot_length <= 0:
        raise ValueError("Invalid mapping file: missing site_to_onehot or num_sites")

    normalized_mapping = {
        normalize_site(site): vector for site, vector in site_to_onehot.items()
    }

    client = MongoClient(MONGODB_URI)
    collection = client[DB_NAME][COLLECTION_NAME]

    updates = []
    unknown_site_count = 0
    total_docs = 0

    projection = {"_id": 1, "site": 1}
    for doc in collection.find({}, projection):
        total_docs += 1
        site = doc.get("site")

        if isinstance(site, str):
            vector = normalized_mapping.get(normalize_site(site))
        else:
            vector = None

        if vector is None:
            unknown_site_count += 1
            vector = [0] * onehot_length

        updates.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": {"site_onehot": vector}})
        )

        if len(updates) >= 1000:
            collection.bulk_write(updates, ordered=False)
            updates.clear()

    if updates:
        collection.bulk_write(updates, ordered=False)

    # Verify length distribution after update
    pipeline = [
        {"$project": {"len": {"$cond": [{"$isArray": "$site_onehot"}, {"$size": "$site_onehot"}, -1]}}},
        {"$group": {"_id": "$len", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    len_dist = list(collection.aggregate(pipeline))

    print(f"Updated site_onehot for {total_docs} documents")
    print(f"Unknown site documents: {unknown_site_count}")
    print(f"site_onehot length distribution: {len_dist}")

    client.close()


if __name__ == "__main__":
    main()
