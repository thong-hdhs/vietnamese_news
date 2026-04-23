#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 3.3: BASE MODEL TRAINING WITH HPO & CALIBRATION
- GridSearchCV để tìm best hyperparameters cho Logistic Regression
- CalibratedClassifierCV để calibration probability (quan trọng cho semi-supervised)
- Đánh giá trên validation set
- Lưu base_model_calibrated.pkl cho self-training
"""

import json
import pickle
import time
from pathlib import Path
from datetime import datetime

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix
)

# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "2. TF-IDF_SVD" / "0. Data"

X_TRAIN_FILE = DATA_DIR / "X_train.pkl"
X_VAL_FILE = DATA_DIR / "X_val.pkl"
Y_TRAIN_FILE = DATA_DIR / "y_train.pkl"
Y_VAL_FILE = DATA_DIR / "y_val.pkl"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_MODEL_FILE = OUTPUT_DIR / "base_model_calibrated.pkl"
HPO_RESULTS_FILE = OUTPUT_DIR / "hpo_results.json"
CALIBRATION_REPORT_FILE = OUTPUT_DIR / "calibration_report.json"


def load_pickle(path: Path):
    """Load pickle file"""
    with path.open("rb") as f:
        return pickle.load(f)


def save_pickle(obj, path: Path):
    """Save pickle file"""
    with path.open("wb") as f:
        pickle.dump(obj, f)


def main():
    print("=" * 80)
    print("PHASE 3.3: BASE MODEL TRAINING WITH HPO & CALIBRATION")
    print("=" * 80)
    print(f"\nTimestamp: {datetime.now().isoformat()}")
    
    total_start = time.time()
    
    # ========== STEP 1: Load Data ==========
    print("\n[STEP 1] Load Data")
    print("-" * 80)
    
    print("Loading X_train, y_train, X_val, y_val...")
    X_train = load_pickle(X_TRAIN_FILE)
    y_train = load_pickle(Y_TRAIN_FILE)
    X_val = load_pickle(X_VAL_FILE)
    y_val = load_pickle(Y_VAL_FILE)
    
    print(f"  X_train shape: {X_train.shape}")
    print(f"  y_train shape: {y_train.shape}")
    print(f"  X_val shape: {X_val.shape}")
    print(f"  y_val shape: {y_val.shape}")
    print(f"  Unique labels in train: {np.unique(y_train)}")
    print(f"  Unique labels in val: {np.unique(y_val)}")
    
    # ========== STEP 2: GridSearchCV for HPO ==========
    print("\n[STEP 2] Hyperparameter Optimization (GridSearchCV)")
    print("-" * 80)
    
    # Define parameter grid
    # Note: sklearn 1.8+ deprecated 'penalty', use 'l1_ratio' instead
    # saga solver supports l1, l2, and elasticnet via l1_ratio
    param_grid = {
        'C': [0.01, 0.1, 1, 10, 100],
        'solver': ['saga'],
        'l1_ratio': [0, 0.5, 1],  # 0=l2, 1=l1, 0.5=elasticnet
        'max_iter': [1000],
        'random_state': [42]
    }
    
    print("Parameter grid:")
    for key, values in param_grid.items():
        print(f"  {key}: {values}")
    
    # Setup StratifiedKFold
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    print(f"\nCross-validation: StratifiedKFold(n_splits=5)")
    
    # Initialize base model
    base_lr = LogisticRegression(random_state=42, max_iter=1000)
    
    # Setup GridSearchCV with error handling
    grid_search = GridSearchCV(
        estimator=base_lr,
        param_grid=param_grid,
        cv=cv,
        scoring='f1_macro',
        n_jobs=-1,
        verbose=1,
        return_train_score=True,
        error_score='raise'  # Will raise errors for debugging
    )
    
    print("\nStarting GridSearchCV...")
    grid_start = time.time()
    grid_search.fit(X_train, y_train)
    grid_time = time.time() - grid_start
    
    print(f"\n✓ GridSearchCV completed in {grid_time:.2f} seconds")
    
    # Extract best parameters
    best_params = grid_search.best_params_
    best_score = grid_search.best_score_
    
    print(f"\nBest parameters:")
    for key, value in best_params.items():
        print(f"  {key}: {value}")
    print(f"\nBest cross-validation F1 (macro): {best_score:.4f}")
    
    # Save HPO results
    hpo_results = {
        'best_params': best_params,
        'best_cv_score': float(best_score),
        'cv_results': grid_search.cv_results_.tolist() if hasattr(grid_search.cv_results_, 'tolist') else None,
        'grid_search_time_seconds': grid_time,
        'n_splits': 5,
        'scoring': 'f1_macro'
    }
    
    with open(HPO_RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(hpo_results, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Saved HPO results to: {HPO_RESULTS_FILE}")
    
    # ========== STEP 3: Train Best Model with Calibration ==========
    print("\n[STEP 3] Train Best Model with Calibration")
    print("-" * 80)
    
    # Initialize best model
    best_lr = LogisticRegression(**best_params)
    
    print(f"Training LogisticRegression with best params...")
    train_start = time.time()
    
    # CalibratedClassifierCV with sigmoid method
    print("Applying CalibratedClassifierCV (method='sigmoid', cv=5)...")
    calibrated_model = CalibratedClassifierCV(
        estimator=best_lr,
        method='sigmoid',
        cv=5
    )
    
    calibrated_model.fit(X_train, y_train)
    train_time = time.time() - train_start
    
    print(f"✓ Training completed in {train_time:.2f} seconds")
    
    # ========== STEP 4: Evaluate on Validation Set ==========
    print("\n[STEP 4] Evaluate on Validation Set")
    print("-" * 80)
    
    # Predictions
    y_pred = calibrated_model.predict(X_val)
    y_pred_proba = calibrated_model.predict_proba(X_val)
    
    # Metrics
    accuracy = accuracy_score(y_val, y_pred)
    precision_macro = precision_score(y_val, y_pred, average='macro')
    recall_macro = recall_score(y_val, y_pred, average='macro')
    f1_macro = f1_score(y_val, y_pred, average='macro')
    f1_weighted = f1_score(y_val, y_pred, average='weighted')
    
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision (macro): {precision_macro:.4f}")
    print(f"Recall (macro): {recall_macro:.4f}")
    print(f"F1-score (macro): {f1_macro:.4f}")
    print(f"F1-score (weighted): {f1_weighted:.4f}")
    
    # Classification report
    print(f"\nClassification Report:")
    print(classification_report(y_val, y_pred, digits=4))
    
    # Confusion matrix
    cm = confusion_matrix(y_val, y_pred)
    print(f"Confusion Matrix:")
    print(cm)
    
    # Probability calibration check
    print(f"\nProbability Calibration Check:")
    print(f"  Max probability: {y_pred_proba.max():.4f}")
    print(f"  Min probability: {y_pred_proba.min():.4f}")
    print(f"  Mean probability: {y_pred_proba.mean():.4f}")
    
    # Check confidence distribution
    max_probs = y_pred_proba.max(axis=1)
    print(f"\nConfidence Distribution (max probability per sample):")
    print(f"  25th percentile: {np.percentile(max_probs, 25):.4f}")
    print(f"  50th percentile (median): {np.percentile(max_probs, 50):.4f}")
    print(f"  75th percentile: {np.percentile(max_probs, 75):.4f}")
    print(f"  90th percentile: {np.percentile(max_probs, 90):.4f}")
    
    # ========== STEP 5: Save Calibrated Model ==========
    print("\n[STEP 5] Save Calibrated Model")
    print("-" * 80)
    
    save_pickle(calibrated_model, BASE_MODEL_FILE)
    model_size = BASE_MODEL_FILE.stat().st_size / (1024 * 1024)
    print(f"✓ Saved calibrated model to: {BASE_MODEL_FILE}")
    print(f"  File size: {model_size:.2f} MB")
    
    # ========== STEP 6: Save Calibration Report ==========
    print("\n[STEP 6] Save Calibration Report")
    print("-" * 80)
    
    calibration_report = {
        'timestamp': datetime.now().isoformat(),
        'execution_time_seconds': time.time() - total_start,
        'grid_search_time_seconds': grid_time,
        'training_time_seconds': train_time,
        
        'best_params': best_params,
        'best_cv_f1_macro': float(best_score),
        
        'validation_metrics': {
            'accuracy': float(accuracy),
            'precision_macro': float(precision_macro),
            'recall_macro': float(recall_macro),
            'f1_macro': float(f1_macro),
            'f1_weighted': float(f1_weighted),
        },
        
        'calibration_method': 'sigmoid',
        'calibration_cv': 5,
        
        'probability_stats': {
            'max': float(y_pred_proba.max()),
            'min': float(y_pred_proba.min()),
            'mean': float(y_pred_proba.mean()),
        },
        
        'confidence_distribution': {
            'p25': float(np.percentile(max_probs, 25)),
            'p50': float(np.percentile(max_probs, 50)),
            'p75': float(np.percentile(max_probs, 75)),
            'p90': float(np.percentile(max_probs, 90)),
        },
        
        'model_file': str(BASE_MODEL_FILE),
        'model_size_mb': model_size,
    }
    
    with open(CALIBRATION_REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(calibration_report, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved calibration report to: {CALIBRATION_REPORT_FILE}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "=" * 80)
    print("PHASE 3.3 COMPLETE - BASE MODEL READY FOR SELF-TRAINING")
    print("=" * 80)
    
    print(f"""
✅ SUMMARY

Best Hyperparameters:
  C: {best_params['C']}
  solver: {best_params['solver']}
  l1_ratio: {best_params['l1_ratio']}
  max_iter: {best_params['max_iter']}

Cross-Validation Performance:
  Best CV F1 (macro): {best_score:.4f}

Validation Performance (Calibrated Model):
  Accuracy: {accuracy:.4f}
  F1-score (macro): {f1_macro:.4f}
  F1-score (weighted): {f1_weighted:.4f}

Calibration:
  Method: sigmoid (cv=5)
  Confidence (median): {np.percentile(max_probs, 50):.4f}

Outputs Saved:
  - base_model_calibrated.pkl ({model_size:.2f} MB)
  - hpo_results.json
  - calibration_report.json

Total Execution Time: {time.time() - total_start:.2f} seconds

Next Step:
  → Phase 3.4: Self-Training Loop
  → Use base_model_calibrated.pkl for pseudo-labeling unlabeled data
  → Threshold based on calibrated probabilities (recommend: > 0.90)
""")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
