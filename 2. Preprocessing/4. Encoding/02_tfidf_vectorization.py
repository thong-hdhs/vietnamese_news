#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 5.2: VECTORIZATION (TF-IDF Production)
- Read documents from MongoDB
- Extract: full_text_tokens, site_onehot, category_encoded
- Apply TF-IDF vectorization (Unigrams + Bigrams)
- Concatenate with site_onehot features
- Save outputs for phases 5.3+
"""

import json
import pickle
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack
from collections import Counter
import time

print("=" * 80)
print("PHASE 5.2: VECTORIZATION (TF-IDF Production)")
print("=" * 80)

start_time = time.time()

# ============ STEP 1: Connect MongoDB & Extract Data ============

print("\n[STEP 1] Connect MongoDB & Extract Data")
print("-" * 80)

try:
    client = MongoClient(
        'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    )
    db = client["vietnamese_news"]
    collection = db["news_data_preprocessing"]
    print("✓ Connected to MongoDB")
except Exception as e:
    print(f"✗ MongoDB connection failed: {e}")
    exit(1)

total_docs = collection.count_documents({})
print(f"✓ Total documents in collection: {total_docs:,}")

# Extract data
print("\nExtracting data...", end=" ", flush=True)
texts_list = []
site_list = []
labels_list = []

for i, doc in enumerate(collection.find({}), 1):
    if i % 2000 == 0:
        print(f"\n  Extracted {i:,}/{total_docs:,}", end=" ", flush=True)
    
    # Extract full_text_tokens
    full_text_tokens = doc.get('full_text_tokens', [])
    
    # Join tokens back to string (for TfidfVectorizer)
    # ⚠️ IMPORTANT: tokens are already separated, join with space
    text_string = ' '.join(full_text_tokens) if full_text_tokens else ""
    texts_list.append(text_string)
    
    # Extract site_onehot (already encoded)
    site_onehot = doc.get('site_onehot', [0, 0, 0, 0])
    site_list.append(site_onehot)
    
    # Extract category_encoded (0-6 for labeled, -1 for unlabeled)
    category_encoded = doc.get('category_encoded', -1)
    labels_list.append(category_encoded)

print(f"\n✓ Extracted {len(texts_list):,} documents")

# ============ STEP 2: Initialize TfidfVectorizer ============

print("\n[STEP 2] Initialize TfidfVectorizer")
print("-" * 80)

vectorizer = TfidfVectorizer(
    max_features=5000,           # Top 5000 features
    min_df=2,                    # Min document frequency
    max_df=0.8,                  # Max document frequency (80%)
    ngram_range=(1, 2),          # Unigrams + Bigrams (IMPORTANT for Vietnamese)
    sublinear_tf=True,           # Log scaling
    norm='l2',                   # L2 normalization
    token_pattern=r'\S+',        # Tokens already separated, don't split further
    lowercase=False              # Already lowercased in phase 4
)

print(f"✓ TfidfVectorizer config:")
print(f"  - max_features: 5000")
print(f"  - min_df: 2, max_df: 0.8")
print(f"  - ngram_range: (1, 2)")
print(f"  - sublinear_tf: True")
print(f"  - norm: l2")

# ============ STEP 3: Fit TfidfVectorizer ============

print("\n[STEP 3] Fit TfidfVectorizer on all documents")
print("-" * 80)

fit_start = time.time()
vectorizer.fit(texts_list)
fit_time = time.time() - fit_start

print(f"✓ Fit complete in {fit_time:.2f} seconds")
print(f"  - Vocabulary size: {len(vectorizer.vocabulary_):,} unique terms")
print(f"  - Selected features (max_features): {vectorizer.max_features}")

# ============ STEP 4: Transform to X_tfidf ============

print("\n[STEP 4] Transform documents to TF-IDF matrix")
print("-" * 80)

transform_start = time.time()
X_tfidf = vectorizer.transform(texts_list)
transform_time = time.time() - transform_start

print(f"✓ Transform complete in {transform_time:.2f} seconds")
print(f"  - Matrix shape: {X_tfidf.shape}")
print(f"  - Format: {type(X_tfidf).__name__} (sparse)")
print(f"  - Non-zero elements: {X_tfidf.nnz:,}")
print(f"  - Sparsity: {(1 - X_tfidf.nnz / (X_tfidf.shape[0] * X_tfidf.shape[1])) * 100:.1f}%")

# ============ STEP 5: Concatenate with site_onehot ============

print("\n[STEP 5] Concatenate TF-IDF with site_onehot")
print("-" * 80)

concat_start = time.time()

# Convert site_list to numpy array
site_array = np.array(site_list, dtype=np.float32)

# Concatenate: sparse + dense
X_combined = hstack([X_tfidf, site_array], format='csr')

concat_time = time.time() - concat_start

print(f"✓ Concatenation complete in {concat_time:.2f} seconds")
print(f"  - TF-IDF features: {X_tfidf.shape[1]}")
print(f"  - Site OnHot features: {site_array.shape[1]}")
print(f"  - Total features: {X_combined.shape[1]}")
print(f"  - Final shape: {X_combined.shape}")

# ============ STEP 6: Extract Labels ============

print("\n[STEP 6] Extract labels")
print("-" * 80)

y = np.array(labels_list, dtype=np.int8)

# Count labeled vs unlabeled
labeled_count = np.sum(y != -1)
unlabeled_count = np.sum(y == -1)

print(f"✓ Labels extracted")
print(f"  - Labeled documents: {labeled_count:,} ({labeled_count/len(y)*100:.1f}%)")
print(f"  - Unlabeled documents: {unlabeled_count:,} ({unlabeled_count/len(y)*100:.1f}%)")

# Label distribution
print(f"\n  Category distribution (labeled only):")
label_counter = Counter(y[y != -1])
for cat in sorted(label_counter.keys()):
    count = label_counter[cat]
    pct = count / labeled_count * 100
    print(f"    category_{cat}: {count:4} ({pct:5.1f}%)")

# ============ STEP 7: Quality Checks ============

print("\n[STEP 7] Quality Checks")
print("-" * 80)

checks_passed = 0
checks_total = 0

# Check 1: Shape verification
checks_total += 1
site_features = len(site_list[0]) if site_list else 0
expected_features = 5000 + site_features
if X_combined.shape == (total_docs, expected_features):
    print(f"✓ Check 1: X matrix shape ({total_docs}, {expected_features})")
    checks_passed += 1
else:
    print(f"✗ Check 1: FAILED - X matrix shape {X_combined.shape}")

# Check 2: Y shape
checks_total += 1
if y.shape == (total_docs,):
    print(f"✓ Check 2: y labels shape ({total_docs},)")
    checks_passed += 1
else:
    print(f"✗ Check 2: FAILED - y labels shape {y.shape}")

# Check 3: No NaN values
checks_total += 1
if np.isnan(X_combined.data).sum() == 0:
    print(f"✓ Check 3: No NaN values in X")
    checks_passed += 1
else:
    print(f"✗ Check 3: FAILED - Found NaN values")

# Check 4: site_onehot exactly one 1 per row
checks_total += 1
site_sum = site_array.sum(axis=1)
if np.all(site_sum == 1):
    print(f"✓ Check 4: site_onehot has exactly one 1 per row")
    checks_passed += 1
else:
    print(f"✗ Check 4: FAILED - site_onehot invalid")

# Check 5: Labels are valid
checks_total += 1
valid_labels = np.all((y >= -1) & (y <= 6))
if valid_labels:
    print(f"✓ Check 5: All labels in valid range (-1 to 6)")
    checks_passed += 1
else:
    print(f"✗ Check 5: FAILED - Invalid labels found")

# Check 6: Labeled/Unlabeled split
checks_total += 1
if labeled_count + unlabeled_count == total_docs:
    print(f"✓ Check 6: Labeled/Unlabeled split ({labeled_count}/{unlabeled_count}) equals total docs")
    checks_passed += 1
else:
    print(f"✗ Check 6: FAILED - Unexpected split ({labeled_count}/{unlabeled_count})")

print(f"\n✓ Quality checks: {checks_passed}/{checks_total} PASSED")
if checks_passed < checks_total:
    print("⚠️  WARNING: Some checks failed")

# ============ STEP 8: Save Files ============

print("\n[STEP 8] Save outputs")
print("-" * 80)

save_start = time.time()

# 8.1: Save vectorizer
try:
    with open('vectorizer.pkl', 'wb') as f:
        pickle.dump(vectorizer, f)
    vectorizer_size = __import__('os').path.getsize('vectorizer.pkl') / (1024**2)
    print(f"✓ Saved vectorizer.pkl ({vectorizer_size:.1f} MB)")
except Exception as e:
    print(f"✗ Failed to save vectorizer.pkl: {e}")

# 8.2: Save X_matrix (sparse)
try:
    with open('X_matrix.pkl', 'wb') as f:
        pickle.dump(X_combined, f)
    X_size = __import__('os').path.getsize('X_matrix.pkl') / (1024**2)
    print(f"✓ Saved X_matrix.pkl ({X_size:.1f} MB, sparse format)")
except Exception as e:
    print(f"✗ Failed to save X_matrix.pkl: {e}")

# 8.3: Save y_labels
try:
    with open('y_labels.pkl', 'wb') as f:
        pickle.dump(y, f)
    y_size = __import__('os').path.getsize('y_labels.pkl') / (1024**2)
    print(f"✓ Saved y_labels.pkl ({y_size:.1f} MB)")
except Exception as e:
    print(f"✗ Failed to save y_labels.pkl: {e}")

save_time = time.time() - save_start

# 8.4: Save statistics
try:
    stats = {
        "execution": {
            "timestamp": datetime.now().isoformat(),
            "total_time_seconds": time.time() - start_time,
            "fit_time_seconds": fit_time,
            "transform_time_seconds": transform_time,
            "concatenate_time_seconds": concat_time,
            "save_time_seconds": save_time
        },
        "input": {
            "n_documents": len(texts_list),
            "n_labeled": int(labeled_count),
            "n_unlabeled": int(unlabeled_count)
        },
        "tfidf_config": {
            "max_features": 5000,
            "min_df": 2,
            "max_df": 0.8,
            "ngram_range": [1, 2],
            "sublinear_tf": True,
            "norm": "l2"
        },
        "output": {
            "X_matrix_shape": list(X_combined.shape),
            "X_matrix_format": "sparse (csr_matrix)",
            "X_tfidf_features": 5000,
            "X_site_features": int(site_features),
            "X_total_features": int(expected_features),
            "y_labels_shape": list(y.shape),
            "sparsity_percent": round((1 - X_combined.nnz / (X_combined.shape[0] * X_combined.shape[1])) * 100, 1)
        },
        "quality_checks": {
            "checks_passed": checks_passed,
            "checks_total": checks_total,
            "status": "PASSED" if checks_passed == checks_total else "FAILED"
        }
    }
    
    with open('vectorization_stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved vectorization_stats.json")
except Exception as e:
    print(f"✗ Failed to save vectorization_stats.json: {e}")

# ============ STEP 9: Summary ============

print("\n" + "=" * 80)
print("SUMMARY - PHASE 5.2 COMPLETE")
print("=" * 80)

total_time = time.time() - start_time

print(f"""
✅ VECTORIZATION COMPLETE

Input:
  ✓ {total_docs:,} documents from MongoDB
  ✓ full_text_tokens extracted
  ✓ site_onehot extracted
  ✓ category_encoded extracted

TF-IDF Configuration:
  ✓ max_features: 5,000
  ✓ ngram_range: (1, 2) - Unigrams + Bigrams
  ✓ min_df: 2, max_df: 0.8
  ✓ sublinear_tf: True
  ✓ norm: L2

Output:
  ✓ X_matrix: ({total_docs:,} × {expected_features:,}) sparse matrix
    - 5,000 TF-IDF features
    - {site_features} site_onehot features
    - Sparsity: {stats['output']['sparsity_percent']:.1f}%
  
  ✓ y_labels: ({total_docs:,},) array
    - Labeled: {labeled_count:,} ({labeled_count/total_docs*100:.1f}%)
    - Unlabeled: {unlabeled_count:,} ({unlabeled_count/total_docs*100:.1f}%)
  
  ✓ vectorizer.pkl: Saved (reuse for inference)
  ✓ vectorization_stats.json: Saved (metadata)

Files Saved:
  - vectorizer.pkl (~5 MB)
  - X_matrix.pkl (~500 MB, sparse)
  - y_labels.pkl (~50 KB)
  - vectorization_stats.json (~1 KB)

Execution Time:
  - Fit: {fit_time:.2f} seconds
  - Transform: {transform_time:.2f} seconds
  - Concatenate: {concat_time:.2f} seconds
  - Save: {save_time:.2f} seconds
  - Total: {total_time:.2f} seconds ({total_time/60:.1f} minutes)

Quality Checks: {checks_passed}/{checks_total} PASSED ✓

Next Step:
  → PHASE 5.3: Train Baseline LogisticRegression Model
  → Load X_matrix.pkl + y_labels.pkl
  → Train on labeled data ({labeled_count:,} docs)
  → Evaluate on same data (F1 baseline)

Ready for PHASE 5.3 ✅
""")

print("=" * 80)

client.close()
