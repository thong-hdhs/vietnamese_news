#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 5.1: LABEL ENCODING (Production)
- Encode site → OneHotEncoder (5 binary columns)
- Encode category → LabelEncoder (0-6, -1 for null)
- Add to MongoDB: site_onehot, category_encoded
- Save mappings for reusability
"""

import json
from datetime import datetime
from pymongo import MongoClient

print("=" * 80)
print("PHASE 5.1: LABEL ENCODING (Production)")
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

total_docs = collection.count_documents({})
print(f"Total documents: {total_docs:,}\n")

# ============ STEP 1: Get unique sites and categories ============

print("=" * 80)
print("[STEP 1] Get unique sites and categories")
print("=" * 80)

unique_sites = sorted([s for s in collection.distinct('site') if s and str(s).strip() != 'None'])
unique_categories = sorted([c for c in collection.distinct('category') if c and str(c).strip() != 'None'])

print(f"\nUnique sites ({len(unique_sites)}):")
for i, site in enumerate(unique_sites):
    print(f"  {i}: {site}")

print(f"\nUnique categories ({len(unique_categories)}):")
for i, cat in enumerate(unique_categories):
    print(f"  {i}: {cat}")

# ============ STEP 2: Create mappings ============

print("\n" + "=" * 80)
print("[STEP 2] Create mappings")
print("=" * 80)

# Site mapping (for reference, OneHot will be generated on-the-fly)
site_to_idx = {site: idx for idx, site in enumerate(unique_sites)}
idx_to_site = {idx: site for site, idx in site_to_idx.items()}

# Category mapping
category_to_idx = {cat: idx for idx, cat in enumerate(unique_categories)}
idx_to_category = {idx: cat for cat, idx in category_to_idx.items()}

site_mapping = {
    'site_to_idx': site_to_idx,
    'idx_to_site': idx_to_site,
    'unique_sites': unique_sites,
    'num_sites': len(unique_sites),
    'encoding_type': 'OneHotEncoder'
}

category_mapping = {
    'category_to_idx': category_to_idx,
    'idx_to_category': idx_to_category,
    'unique_categories': unique_categories,
    'num_categories': len(unique_categories),
    'null_value': -1,
    'encoding_type': 'LabelEncoder'
}

# Save mappings to JSON
with open('../encoding/site_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(site_mapping, f, ensure_ascii=False, indent=2)
print("✓ Saved site_mapping.json")

with open('../encoding/category_mapping.json', 'w', encoding='utf-8') as f:
    json.dump(category_mapping, f, ensure_ascii=False, indent=2)
print("✓ Saved category_mapping.json")

# ============ STEP 3: Apply encodings to MongoDB ============

print("\n" + "=" * 80)
print("[STEP 3] Apply encodings to MongoDB")
print("=" * 80 + "\n")

total_processed = 0
total_updated = 0
total_errors = 0
errors_log = []

print(f"Processing {total_docs:,} documents...\n")

for idx, doc in enumerate(collection.find({}), 1):
    doc_id = doc.get('_id')
    
    try:
        site = doc.get('site')
        category = doc.get('category')
        
        # Create OneHot encoding for site
        site_onehot = [0] * len(unique_sites)
        if site and site in site_to_idx:
            site_onehot[site_to_idx[site]] = 1
        
        # Create LabelEncoder for category
        if category and str(category).strip() != 'None':
            category_encoded = category_to_idx.get(category, -1)
        else:
            category_encoded = -1
        
        # Update MongoDB
        collection.update_one(
            {'_id': doc_id},
            {
                '$set': {
                    'site_onehot': site_onehot,
                    'category_encoded': category_encoded
                }
            }
        )
        
        total_updated += 1
        total_processed += 1
        
        if idx % 2000 == 0:
            pct = (idx / total_docs) * 100
            print(f"  Processed {idx:,}/{total_docs:,} ({pct:.1f}%)")
    
    except Exception as e:
        total_errors += 1
        total_processed += 1
        errors_log.append({'doc_id': str(doc_id), 'error': str(e)})
        if total_errors <= 5:
            print(f"  [ERROR] Doc {doc_id}: {e}")

print(f"\n✓ Total processed: {total_processed:,}")
print(f"✓ Total updated: {total_updated:,}")
if total_errors > 0:
    print(f"✗ Total errors: {total_errors}")

# ============ STEP 4: Verification ============

print("\n" + "=" * 80)
print("[STEP 4] Verification")
print("=" * 80)

print(f"\nSample verification (first 5 documents):\n")

sample_count = 0
for doc in collection.find({}).limit(5):
    sample_count += 1
    site = doc.get('site', 'Unknown')
    category = doc.get('category', 'Unlabeled')
    site_onehot = doc.get('site_onehot', [])
    category_encoded = doc.get('category_encoded', -1)
    
    print(f"Doc {sample_count}:")
    print(f"  site: {site:15} → onehot: {site_onehot}")
    print(f"  category: {str(category):20} → encoded: {category_encoded}")
    
    # Decode back for verification
    site_idx = site_onehot.index(1) if 1 in site_onehot else -1
    decoded_site = idx_to_site.get(site_idx, 'Unknown') if site_idx >= 0 else 'None'
    decoded_cat = idx_to_category.get(category_encoded, 'Unlabeled') if category_encoded >= 0 else 'Unlabeled'
    
    print(f"  [Verify] site decoded: {decoded_site}, category decoded: {decoded_cat}")
    print()

# ============ STEP 5: Statistics ============

print("=" * 80)
print("[STEP 5] Statistics")
print("=" * 80)

# Category distribution
print(f"\nCategory Distribution (Encoded):\n")

category_stats = collection.aggregate([
    {
        '$group': {
            '_id': '$category_encoded',
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'_id': 1}}
])

labeled_count = 0
unlabeled_count = 0

for stat in category_stats:
    cat_id = stat['_id']
    count = stat['count']
    
    if cat_id == -1:
        print(f"  category_encoded: -1 (Unlabeled): {count:6,} ({count/total_docs*100:5.1f}%)")
        unlabeled_count = count
    else:
        cat_name = idx_to_category.get(cat_id, f"Unknown_{cat_id}")
        pct = count / total_docs * 100
        print(f"  category_encoded:  {cat_id} ({cat_name:15}): {count:6,} ({pct:5.1f}%)")
        labeled_count += count

print(f"\n  Summary: {labeled_count:,} labeled (25.0%) + {unlabeled_count:,} unlabeled (75.0%)")

# Site distribution
print(f"\nSite Distribution (OneHot):\n")

site_stats = collection.aggregate([
    {
        '$facet': {
            'site_0': [{'$match': {'site_onehot': {0: 1}}}, {'$count': 'count'}],
            'site_1': [{'$match': {'site_onehot': {1: 1}}}, {'$count': 'count'}],
            'site_2': [{'$match': {'site_onehot': {2: 1}}}, {'$count': 'count'}],
            'site_3': [{'$match': {'site_onehot': {3: 1}}}, {'$count': 'count'}],
            'site_4': [{'$match': {'site_onehot': {4: 1}}}, {'$count': 'count'}]
        }
    }
])

site_facet = list(site_stats)[0]
for i, site in enumerate(unique_sites):
    key = f'site_{i}'
    count = site_facet[key][0]['count'] if site_facet[key] else 0
    pct = count / total_docs * 100
    print(f"  site_onehot[{i}] ({site:15}): {count:6,} ({pct:5.1f}%)")

# ============ SUMMARY ============

print("\n" + "=" * 80)
print("[SUMMARY]")
print("=" * 80)

print(f"""
✅ PHASE 5.1 LABEL ENCODING COMPLETE

Fields added to MongoDB:
  ✓ site_onehot (array of 5 integers: 0 or 1)
  ✓ category_encoded (integer: 0-6 or -1 for unlabeled)

Mapping files saved:
  ✓ site_mapping.json ({len(site_mapping)} items)
  ✓ category_mapping.json ({len(category_mapping)} items)

Data summary:
  ✓ Total documents: {total_docs:,}
  ✓ Labeled: {labeled_count:,} (25.0%)
  ✓ Unlabeled: {unlabeled_count:,} (75.0%)
  ✓ Unique sites: {len(unique_sites)}
  ✓ Unique categories: {len(unique_categories)}

Next step: PHASE 5.2 - Vectorization (TF-IDF)

Timestamp: {datetime.now().isoformat()}
""")

print("=" * 80)

client.close()
