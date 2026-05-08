#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDATE MODEL ON VALIDATION SET
- Load trained model and predict on validation set
- Compare predictions with ground truth
- Calculate accuracy metrics and visualize results
"""

import json
import pickle
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report

# Category mapping
category_names = {
    0: "Cong nghe",
    1: "Giao duc",
    2: "Giai tri",
    3: "Kinh te",
    4: "Suc khoe",
    5: "The thao",
    6: "Thoi su"
}

# Paths
BASE_DIR = Path("d:\\vietnamese_news\\3. Processing")
DATA_DIR = BASE_DIR / "2. TF-IDF_SVD" / "0. Data"
VAL_JSONL = BASE_DIR / "1. Data Split" / "0. data" / "val_data.json"
MODEL_FILE = BASE_DIR / "4. Self-Training Loop" / "output" / "final_model_calibrated.pkl"
OUTPUT_DIR = BASE_DIR / "5. Final Evaluation" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def load_pickle(path):
    """Load pickle file"""
    with path.open("rb") as f:
        return pickle.load(f)

def read_jsonl(path):
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
    print("VALIDATE MODEL ON VALIDATION SET")
    print("=" * 80)
    
    # ========== STEP 1: Load Model & Data ==========
    print("\n[STEP 1] Load Model & Data")
    print("-" * 80)
    
    print("Loading model...")
    model = load_pickle(MODEL_FILE)
    print(f"  Model loaded from: {MODEL_FILE}")
    
    print("\nLoading validation features...")
    X_val = load_pickle(DATA_DIR / "X_val.pkl")
    y_val = load_pickle(DATA_DIR / "y_val.pkl")
    print(f"  X_val shape: {X_val.shape}")
    print(f"  y_val shape: {y_val.shape}")
    
    print("\nLoading validation metadata...")
    val_docs = read_jsonl(VAL_JSONL)
    print(f"  Loaded {len(val_docs)} documents")
    
    # Verify consistency
    assert len(val_docs) == X_val.shape[0], "Mismatch between docs and features"
    assert len(val_docs) == len(y_val), "Mismatch between docs and labels"
    print(f"  Consistency check passed")
    
    # ========== STEP 2: Predict ==========
    print("\n[STEP 2] Predict on Validation Set")
    print("-" * 80)
    
    print("Predicting labels...")
    y_pred = model.predict(X_val)
    print(f"  Predictions completed: {len(y_pred)} samples")
    
    print("Predicting probabilities...")
    y_proba = model.predict_proba(X_val)
    print(f"  Probabilities completed: {y_proba.shape}")
    
    # Get confidence scores
    confidence = y_proba.max(axis=1)
    
    # ========== STEP 3: Calculate Metrics ==========
    print("\n[STEP 3] Calculate Metrics")
    print("-" * 80)
    
    accuracy = accuracy_score(y_val, y_pred)
    precision_macro = precision_score(y_val, y_pred, average='macro')
    recall_macro = recall_score(y_val, y_pred, average='macro')
    f1_macro = f1_score(y_val, y_pred, average='macro')
    f1_weighted = f1_score(y_val, y_pred, average='weighted')
    
    print(f"\nOverall Metrics:")
    print(f"  Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"  Precision (macro): {precision_macro:.4f}")
    print(f"  Recall (macro): {recall_macro:.4f}")
    print(f"  F1 Score (macro): {f1_macro:.4f}")
    print(f"  F1 Score (weighted): {f1_weighted:.4f}")
    
    # Per-class metrics
    print(f"\nPer-Class Metrics:")
    print(f"{'Category':<15} {'Precision':<12} {'Recall':<12} {'F1':<12} {'Support':<10}")
    print("-" * 70)
    
    class_report = classification_report(y_val, y_pred, target_names=[category_names[i] for i in range(7)], output_dict=True)
    for i in range(7):
        cat_name = category_names[i]
        metrics = class_report[cat_name]
        print(f"{cat_name:<15} {metrics['precision']:<12.4f} {metrics['recall']:<12.4f} {metrics['f1-score']:<12.4f} {int(metrics['support']):<10}")
    
    # ========== STEP 4: Confusion Matrix ==========
    print("\n[STEP 4] Confusion Matrix")
    print("-" * 80)
    
    cm = confusion_matrix(y_val, y_pred)
    
    # Plot confusion matrix
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=[category_names[i] for i in range(7)],
                yticklabels=[category_names[i] for i in range(7)])
    plt.title('Confusion Matrix - Validation Set', fontsize=16, fontweight='bold')
    plt.xlabel('Predicted Label', fontsize=12)
    plt.ylabel('True Label', fontsize=12)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'val_confusion_matrix.png', dpi=300, bbox_inches='tight')
    print("  Saved: val_confusion_matrix.png")
    
    # ========== STEP 5: Prepare Results ==========
    print("\n[STEP 5] Prepare Results")
    print("-" * 80)
    
    results = []
    correct_count = 0
    
    for i, (doc, pred_label, true_label, conf) in enumerate(zip(val_docs, y_pred, y_val, confidence)):
        is_correct = (pred_label == true_label)
        if is_correct:
            correct_count += 1
        
        result = {
            'index': i,
            'url': doc.get('url', ''),
            'site': doc.get('site', ''),
            'true_label': int(true_label),
            'true_category': category_names[true_label],
            'predicted_label': int(pred_label),
            'predicted_category': category_names[pred_label],
            'is_correct': bool(is_correct),
            'confidence': float(conf)
        }
        results.append(result)
    
    accuracy_pct = (correct_count / len(results)) * 100
    print(f"  Correct predictions: {correct_count}/{len(results)} ({accuracy_pct:.2f}%)")
    
    # ========== STEP 6: Save Results ==========
    print("\n[STEP 6] Save Results")
    print("-" * 80)
    
    # Save detailed results
    results_file = OUTPUT_DIR / 'val_predictions_with_truth.json'
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {results_file}")
    
    # Save summary
    summary = {
        'total_samples': len(results),
        'correct_predictions': correct_count,
        'accuracy': accuracy,
        'accuracy_percentage': accuracy_pct,
        'precision_macro': precision_macro,
        'recall_macro': recall_macro,
        'f1_macro': f1_macro,
        'f1_weighted': f1_weighted,
        'per_class_metrics': class_report
    }
    
    summary_file = OUTPUT_DIR / 'val_validation_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {summary_file}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)
    print(f"""
SUMMARY:
  Total samples: {len(results)}
  Correct: {correct_count}
  Accuracy: {accuracy:.4f} ({accuracy_pct:.2f}%)
  F1 (macro): {f1_macro:.4f}
  F1 (weighted): {f1_weighted:.4f}

Outputs:
  - val_predictions_with_truth.json (detailed predictions)
  - val_validation_summary.json (metrics summary)
  - val_confusion_matrix.png (visualization)
""")
    print("=" * 80)

if __name__ == "__main__":
    main()
