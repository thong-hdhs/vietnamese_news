#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BACKUP: Export post-stopword removal data to JSON
- Backup 13,159 documents after PHASE 4.3 (stopword removal)
- Save to batch JSON files (1000 docs per batch)
- Timestamp: 2026-04-08 (after stopword removal completed)
"""

import json
import os
from datetime import datetime
from pymongo import MongoClient

print("=" * 80)
print("BACKUP: Export post-stopword removal data")
print("=" * 80)

# MongoDB Connection
try:
    client = MongoClient(
        'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    )
    db = client["vietnamese_news"]
    collection = db["news_data_preprocessing"]
    print("\n✓ Connected to MongoDB")
except Exception as e:
    print(f"\n[ERROR] MongoDB connection failed: {e}")
    exit(1)

# Count total documents
total_docs = collection.count_documents({})
print(f"Total documents: {total_docs:,}")

# Create backup directory
backup_dir = "../backup_post_stopword_removal"
os.makedirs(backup_dir, exist_ok=True)
print(f"\nBackup directory: {backup_dir}")

# Export to JSON batches
batch_size = 1000
batch_num = 0
batch_data = []
total_exported = 0

start_time = datetime.now()
print(f"\nStarting export... {start_time.isoformat()}\n")

try:
    for idx, doc in enumerate(collection.find({}), 1):
        # Convert _id to string for JSON serialization
        doc['_id'] = str(doc['_id'])
        batch_data.append(doc)
        
        # Save batch when reaches batch_size
        if len(batch_data) == batch_size:
            batch_num += 1
            batch_file = os.path.join(backup_dir, f"backup_batch_{batch_num:03d}.json")
            
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)
            
            total_exported += len(batch_data)
            file_size_mb = os.path.getsize(batch_file) / (1024 * 1024)
            print(f"  Batch {batch_num}: Saved {len(batch_data)} documents ({file_size_mb:.2f} MB)")
            
            batch_data = []
            
            if idx % 5000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = idx / elapsed if elapsed > 0 else 0
                print(f"    Progress: {idx:,}/{total_docs:,} ({rate:.1f} docs/sec)")
    
    # Save remaining batch
    if batch_data:
        batch_num += 1
        batch_file = os.path.join(backup_dir, f"backup_batch_{batch_num:03d}.json")
        
        with open(batch_file, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, ensure_ascii=False, indent=2)
        
        total_exported += len(batch_data)
        file_size_mb = os.path.getsize(batch_file) / (1024 * 1024)
        print(f"  Batch {batch_num}: Saved {len(batch_data)} documents ({file_size_mb:.2f} MB)")

except Exception as e:
    print(f"\n[ERROR] Export failed: {e}")
    client.close()
    exit(1)

end_time = datetime.now()
elapsed = (end_time - start_time).total_seconds()

# Summary
print("\n" + "=" * 80)
print("[BACKUP SUMMARY]")
print("=" * 80)

print(f"\nTotal documents exported: {total_exported:,}")
print(f"Total batches: {batch_num}")
print(f"Execution time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")

# Calculate total size
total_size = 0
for i in range(1, batch_num + 1):
    batch_file = os.path.join(backup_dir, f"backup_batch_{i:03d}.json")
    if os.path.exists(batch_file):
        total_size += os.path.getsize(batch_file)

total_size_mb = total_size / (1024 * 1024)
total_size_gb = total_size / (1024 * 1024 * 1024)

print(f"\nTotal backup size: {total_size_mb:.2f} MB ({total_size_gb:.3f} GB)")

# Get sample document for verification
sample = collection.find_one()
if sample:
    print(f"\n[SAMPLE VERIFICATION]")
    print(f"Document ID: {sample.get('_id')}")
    print(f"Fields: {list(sample.keys())}")
    print(f"full_text_tokens count: {len(sample.get('full_text_tokens', []))}")
    print(f"metadata_text_tokens count: {len(sample.get('metadata_text_tokens', []))}")
    if sample.get('full_text_tokens'):
        print(f"First 5 tokens: {' '.join(sample['full_text_tokens'][:5])}")

print("\n" + "=" * 80)
print("✅ BACKUP COMPLETE")
print("=" * 80)
print(f"\nBackup location: {os.path.abspath(backup_dir)}")
print(f"Timestamp: {end_time.isoformat()}")
print(f"\nFiles created:")
for i in range(1, batch_num + 1):
    batch_file = os.path.join(backup_dir, f"backup_batch_{i:03d}.json")
    if os.path.exists(batch_file):
        size_mb = os.path.getsize(batch_file) / (1024 * 1024)
        print(f"  - backup_batch_{i:03d}.json ({size_mb:.2f} MB)")

client.close()
