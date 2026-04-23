#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 3.5: FINAL INFERENCE ON ALL UNLABELED DATA
- Dùng best model từ Iteration 13 để gán nhãn cho toàn bộ 9,855 unlabeled samples
- Không dùng Top-K hay threshold, lấy prediction cao nhất
- Tạo file kết quả cuối cùng với labels và confidence scores
"""

import json
import pickle
from pathlib import Path
from datetime import datetime

import numpy as np

# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "2. TF-IDF_SVD" / "0. Data"
UNLABELED_JSONL = BASE_DIR.parent / "1. Data Split" / "0. data" / "unlabeled_data.json"
FINAL_MODEL_FILE = BASE_DIR.parent / "4. Self-Training Loop" / "output" / "final_model_calibrated.pkl"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FINAL_PREDICTIONS_FILE = OUTPUT_DIR / "final_predictions.json"
FINAL_PREDICTIONS_CSV = OUTPUT_DIR / "final_predictions.csv"


def load_pickle(path: Path):
    """Load pickle file"""
    with path.open("rb") as f:
        return pickle.load(f)


def read_jsonl(path: Path):
    """Read JSONL file"""
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    print("=" * 80)
    print("PHASE 3.5: FINAL INFERENCE ON ALL UNLABELED DATA")
    print("=" * 80)
    print(f"\nTimestamp: {datetime.now().isoformat()}")
    
    # ========== STEP 1: Load Model & Data ==========
    print("\n[STEP 1] Load Model & Data")
    print("-" * 80)
    
    print("Loading best model...")
    model = load_pickle(FINAL_MODEL_FILE)
    print(f"  ✓ Model loaded from: {FINAL_MODEL_FILE}")
    
    print("\nLoading unlabeled features...")
    X_unlabeled = load_pickle(DATA_DIR / "X_unlabeled.pkl")
    print(f"  ✓ X_unlabeled shape: {X_unlabeled.shape}")
    
    print("\nLoading unlabeled metadata...")
    unlabeled_docs = read_jsonl(UNLABELED_JSONL)
    print(f"  ✓ Loaded {len(unlabeled_docs)} documents")
    
    # Verify consistency
    assert len(unlabeled_docs) == X_unlabeled.shape[0], "Mismatch between docs and features"
    print(f"  ✓ Consistency check passed")
    
    # ========== STEP 2: Predict All Unlabeled ==========
    print("\n[STEP 2] Predict All Unlabeled Samples")
    print("-" * 80)
    
    print("Predicting labels...")
    y_pred = model.predict(X_unlabeled)
    print(f"  ✓ Predictions completed: {len(y_pred)} samples")
    
    print("Predicting probabilities...")
    y_proba = model.predict_proba(X_unlabeled)
    print(f"  ✓ Probabilities completed: {y_proba.shape}")
    
    # Get confidence (max probability)
    confidence = y_proba.max(axis=1)
    print(f"  ✓ Confidence scores extracted")
    
    # ========== STEP 3: Statistics ==========
    print("\n[STEP 3] Prediction Statistics")
    print("-" * 80)
    
    # Label distribution
    unique, counts = np.unique(y_pred, return_counts=True)
    print(f"\nLabel Distribution:")
    for label, count in zip(unique, counts):
        print(f"  Class {label}: {count} samples ({count/len(y_pred)*100:.2f}%)")
    
    # Confidence statistics
    print(f"\nConfidence Statistics:")
    print(f"  Min: {confidence.min():.4f}")
    print(f"  Max: {confidence.max():.4f}")
    print(f"  Mean: {confidence.mean():.4f}")
    print(f"  Median: {np.median(confidence):.4f}")
    print(f"  25th percentile: {np.percentile(confidence, 25):.4f}")
    print(f"  75th percentile: {np.percentile(confidence, 75):.4f}")
    
    # High confidence samples (> 0.9)
    high_conf = (confidence > 0.9).sum()
    print(f"\nHigh Confidence (> 0.9): {high_conf} samples ({high_conf/len(confidence)*100:.2f}%)")
    
    # Low confidence samples (< 0.7)
    low_conf = (confidence < 0.7).sum()
    print(f"Low Confidence (< 0.7): {low_conf} samples ({low_conf/len(confidence)*100:.2f}%)")
    
    # ========== STEP 4: Combine with Metadata ==========
    print("\n[STEP 4] Combine Predictions with Metadata")
    print("-" * 80)
    
    final_results = []
    
    for i, (doc, pred_label, conf) in enumerate(zip(unlabeled_docs, y_pred, confidence)):
        result = {
            'index': i,
            'title': doc.get('title', ''),
            'url': doc.get('url', ''),
            'site': doc.get('site', ''),
            'predicted_label': int(pred_label),
            'confidence': float(conf),
            'top_3_classes': []
        }
        
        # Get top 3 classes with probabilities
        top3_indices = np.argsort(y_proba[i])[-3:][::-1]
        for idx in top3_indices:
            result['top_3_classes'].append({
                'class': int(idx),
                'probability': float(y_proba[i, idx])
            })
        
        final_results.append(result)
    
    print(f"  ✓ Combined {len(final_results)} results")
    
    # ========== STEP 5: Save Results ==========
    print("\n[STEP 5] Save Results")
    print("-" * 80)
    
    # Save as JSON
    with open(FINAL_PREDICTIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Saved JSON to: {FINAL_PREDICTIONS_FILE}")
    
    # Save as CSV (for easy viewing)
    import csv
    with open(FINAL_PREDICTIONS_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Index', 'Title', 'Site', 'Predicted_Label', 'Confidence', 'URL'])
        for r in final_results:
            writer.writerow([
                r['index'],
                r['title'][:100] if r['title'] else '',  # Truncate title
                r['site'],
                r['predicted_label'],
                f"{r['confidence']:.4f}",
                r['url']
            ])
    print(f"  ✓ Saved CSV to: {FINAL_PREDICTIONS_CSV}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "=" * 80)
    print("FINAL INFERENCE COMPLETE")
    print("=" * 80)
    
    print(f"""
✅ SUMMARY

Total Unlabeled Samples: {len(final_results)}

Label Distribution:
""")
    for label, count in zip(unique, counts):
        print(f"  Class {label}: {count} samples ({count/len(y_pred)*100:.2f}%)")
    
    print(f"""
Confidence Statistics:
  Mean: {confidence.mean():.4f}
  Median: {np.median(confidence):.4f}
  High confidence (>0.9): {high_conf} samples ({high_conf/len(confidence)*100:.2f}%)
  Low confidence (<0.7): {low_conf} samples ({low_conf/len(confidence)*100:.2f}%)

Outputs Saved:
  - final_predictions.json (full results with top-3 classes)
  - final_predictions.csv (summary for easy viewing)

Next Step:
  → Analyze results by category and site
  → Generate final report
  → Optionally: Review low-confidence samples manually
""")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
