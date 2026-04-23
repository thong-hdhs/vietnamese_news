#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 3.4: SELF-TRAINING LOOP WITH TOP-K STRATEGY
- Chiến lược: Top-K (K=20) per class với threshold > 0.9
- Mỗi vòng lặp: chọn 20 mẫu có xác suất cao nhất cho từng lớp
- Retrain sau mỗi vòng với tham số tối ưu từ HPO
- Theo dõi F1-score trên validation set
"""

import json
import pickle
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import numpy as np
from scipy.sparse import issparse, vstack, csr_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import f1_score, classification_report

# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "2. TF-IDF_SVD" / "0. Data"
BASE_MODEL_DIR = BASE_DIR.parent / "3. Base Model HPO" / "output"

X_TRAIN_FILE = DATA_DIR / "X_train.pkl"
Y_TRAIN_FILE = DATA_DIR / "y_train.pkl"
X_VAL_FILE = DATA_DIR / "X_val.pkl"
Y_VAL_FILE = DATA_DIR / "y_val.pkl"
X_UNLABELED_FILE = DATA_DIR / "X_unlabeled.pkl"

BASE_MODEL_FILE = BASE_MODEL_DIR / "base_model_calibrated.pkl"
HPO_RESULTS_FILE = BASE_MODEL_DIR / "hpo_results.json"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

FINAL_MODEL_FILE = OUTPUT_DIR / "final_model_calibrated.pkl"
SELF_TRAINING_LOG_FILE = OUTPUT_DIR / "self_training_log.json"
PSEUDO_LABELS_FILE = OUTPUT_DIR / "pseudo_labels.json"


def load_pickle(path: Path):
    """Load pickle file"""
    with path.open("rb") as f:
        return pickle.load(f)


def save_pickle(obj, path: Path):
    """Save pickle file"""
    with path.open("wb") as f:
        pickle.dump(obj, f)


def select_top_k_per_class(probs, threshold=0.9, k=20):
    """
    Chọn K mẫu có xác suất cao nhất cho từng lớp
    
    Args:
        probs: Array xác suất (n_samples, n_classes)
        threshold: Ngưỡng xác suất tối thiểu
        k: Số mẫu tối đa mỗi lớp
    
    Returns:
        selected_indices: List indices được chọn
        selected_labels: List labels tương ứng
    """
    n_samples, n_classes = probs.shape
    selected_indices = []
    selected_labels = []
    
    for class_idx in range(n_classes):
        # Lấy xác suất cho lớp này
        class_probs = probs[:, class_idx]
        
        # Lọc các mẫu có prob > threshold
        above_threshold = np.where(class_probs > threshold)[0]
        
        if len(above_threshold) == 0:
            continue
        
        # Sắp xếp theo xác suất giảm dần
        sorted_indices = above_threshold[np.argsort(class_probs[above_threshold])[::-1]]
        
        # Chọn top K
        top_k_indices = sorted_indices[:k]
        
        selected_indices.extend(top_k_indices)
        selected_labels.extend([class_idx] * len(top_k_indices))
    
    return selected_indices, selected_labels


def main():
    print("=" * 80)
    print("PHASE 3.4: SELF-TRAINING LOOP WITH TOP-K STRATEGY")
    print("=" * 80)
    print(f"\nTimestamp: {datetime.now().isoformat()}")
    
    total_start = time.time()
    
    # ========== STEP 1: Load Data & Model ==========
    print("\n[STEP 1] Load Data & Base Model")
    print("-" * 80)
    
    print("Loading data...")
    X_train = load_pickle(X_TRAIN_FILE)
    y_train = load_pickle(Y_TRAIN_FILE)
    X_val = load_pickle(X_VAL_FILE)
    y_val = load_pickle(Y_VAL_FILE)
    X_unlabeled = load_pickle(X_UNLABELED_FILE)
    
    print(f"  X_train: {X_train.shape}, y_train: {y_train.shape}")
    print(f"  X_val: {X_val.shape}, y_val: {y_val.shape}")
    print(f"  X_unlabeled: {X_unlabeled.shape}")
    
    print("\nLoading base model...")
    base_model = load_pickle(BASE_MODEL_FILE)
    print(f"  Base model loaded from: {BASE_MODEL_FILE}")
    
    print("\nLoading HPO results...")
    with open(HPO_RESULTS_FILE, 'r', encoding='utf-8') as f:
        hpo_results = json.load(f)
    best_params = hpo_results['best_params']
    print(f"  Best params: {best_params}")
    
    # ========== STEP 2: Initialize Dynamic Datasets ==========
    print("\n[STEP 2] Initialize Dynamic Datasets")
    print("-" * 80)
    
    # Current Training Set (ban đầu là train gốc)
    current_X_train = X_train.copy() if issparse(X_train) else X_train.copy()
    current_y_train = y_train.copy()
    
    # Current Unlabeled Pool (ban đầu là toàn bộ unlabeled)
    current_X_pool = X_unlabeled.copy() if issparse(X_unlabeled) else X_unlabeled.copy()
    pool_indices = np.arange(X_unlabeled.shape[0])  # Track original indices
    
    # Validation Set (không đổi)
    current_X_val = X_val
    current_y_val = y_val
    
    print(f"  Initial Train: {current_X_train.shape[0]} samples")
    print(f"  Initial Pool: {current_X_pool.shape[0]} samples")
    print(f"  Validation: {current_X_val.shape[0]} samples")
    
    # ========== STEP 3: Self-Training Loop ==========
    print("\n[STEP 3] Self-Training Loop")
    print("-" * 80)
    
    # Parameters
    K_PER_CLASS = 20
    CONFIDENCE_THRESHOLD = 0.9
    MAX_ITERATIONS = 15
    F1_DROP_THRESHOLD = 0.01  # 1% drop
    
    # Tracking
    iteration_log = []
    best_val_f1 = 0.0
    best_model = None
    pseudo_labels_all = []  # Track all pseudo labels assigned
    
    print(f"\nConfiguration:")
    print(f"  K per class: {K_PER_CLASS}")
    print(f"  Confidence threshold: {CONFIDENCE_THRESHOLD}")
    print(f"  Max iterations: {MAX_ITERATIONS}")
    print(f"  F1 drop threshold: {F1_DROP_THRESHOLD}")
    
    # Evaluate base model first
    print(f"\n{'='*80}")
    print(f"Iteration 0: Base Model (Initial)")
    print(f"{'='*80}")
    
    y_val_pred = base_model.predict(current_X_val)
    val_f1 = f1_score(current_y_val, y_val_pred, average='macro')
    best_val_f1 = val_f1
    best_model = base_model
    
    iteration_log.append({
        'iteration': 0,
        'pseudo_labels_added': 0,
        'pool_size': current_X_pool.shape[0],
        'train_size': current_X_train.shape[0],
        'val_f1_macro': float(val_f1),
        'val_f1_drop': 0.0,
        'stopped_reason': 'initial'
    })
    
    print(f"  Validation F1 (macro): {val_f1:.4f}")
    print(f"  Pool size: {current_X_pool.shape[0]}")
    print(f"  Train size: {current_X_train.shape[0]}")
    
    # Self-training loop
    for iteration in range(1, MAX_ITERATIONS + 1):
        print(f"\n{'='*80}")
        print(f"Iteration {iteration}")
        print(f"{'='*80}")
        
        # Bước A: Dự đoán xác suất trên Pool
        print(f"  [A] Predicting probabilities on Pool...")
        pool_probs = base_model.predict_proba(current_X_pool)
        print(f"      Pool shape: {current_X_pool.shape}")
        print(f"      Probs shape: {pool_probs.shape}")
        
        # Bước B: Chọn Top-K per class
        print(f"  [B] Selecting Top-K per class...")
        selected_indices, selected_labels = select_top_k_per_class(
            pool_probs, 
            threshold=CONFIDENCE_THRESHOLD, 
            k=K_PER_CLASS
        )
        
        num_selected = len(selected_indices)
        print(f"      Selected: {num_selected} samples")
        
        # Distribution by class
        label_counts = defaultdict(int)
        for label in selected_labels:
            label_counts[label] += 1
        print(f"      Distribution: {dict(label_counts)}")
        
        # Stopping condition: No samples selected
        if num_selected == 0:
            print(f"  [STOP] No samples selected with threshold > {CONFIDENCE_THRESHOLD}")
            iteration_log[-1]['stopped_reason'] = 'no_samples_selected'
            break
        
        # Bước C: Cập nhật datasets
        print(f"  [C] Updating datasets...")
        
        # Lấy samples được chọn từ Pool
        selected_X = current_X_pool[selected_indices]
        selected_y = np.array(selected_labels, dtype=np.int32)
        
        # Track original indices for pseudo labels
        original_indices = pool_indices[selected_indices]
        for idx, (orig_idx, pred_label) in enumerate(zip(original_indices, selected_labels)):
            # Get the probability for the predicted class
            confidence = float(pool_probs[selected_indices[idx], pred_label])
            pseudo_labels_all.append({
                'original_index': int(orig_idx),
                'predicted_label': int(pred_label),
                'iteration': iteration,
                'confidence': confidence
            })
        
        # Thêm vào Train
        if issparse(current_X_train):
            current_X_train = vstack([current_X_train, selected_X])
        else:
            current_X_train = np.vstack([current_X_train, selected_X])
        current_y_train = np.concatenate([current_y_train, selected_y])
        
        # Xóa từ Pool
        mask = np.ones(current_X_pool.shape[0], dtype=bool)
        mask[selected_indices] = False
        current_X_pool = current_X_pool[mask]
        pool_indices = pool_indices[mask]
        
        print(f"      Added to Train: {num_selected} samples")
        print(f"      Removed from Pool: {num_selected} samples")
        print(f"      New Train size: {current_X_train.shape[0]}")
        print(f"      New Pool size: {current_X_pool.shape[0]}")
        
        # Bước D: Retrain với tham số tối ưu
        print(f"  [D] Retraining model...")
        
        # Create new model with best params
        new_lr = LogisticRegression(**best_params)
        new_model = CalibratedClassifierCV(
            estimator=new_lr,
            method='sigmoid',
            cv=5
        )
        
        retrain_start = time.time()
        new_model.fit(current_X_train, current_y_train)
        retrain_time = time.time() - retrain_start
        
        print(f"      Retraining completed in {retrain_time:.2f}s")
        
        # Evaluate on validation
        y_val_pred_new = new_model.predict(current_X_val)
        val_f1_new = f1_score(current_y_val, y_val_pred_new, average='macro')
        f1_drop = best_val_f1 - val_f1_new
        
        print(f"      Validation F1 (macro): {val_f1_new:.4f}")
        print(f"      F1 change: {f1_drop:+.4f}")
        
        # Update base model for next iteration
        base_model = new_model
        
        # Track best model
        if val_f1_new > best_val_f1:
            best_val_f1 = val_f1_new
            best_model = new_model
            print(f"      ✓ New best model! F1: {best_val_f1:.4f}")
        
        # Stopping condition: F1 drop > 1%
        if f1_drop > F1_DROP_THRESHOLD:
            print(f"  [STOP] F1 dropped by {f1_drop:.4f} > {F1_DROP_THRESHOLD}")
            iteration_log.append({
                'iteration': iteration,
                'pseudo_labels_added': num_selected,
                'pool_size': current_X_pool.shape[0],
                'train_size': current_X_train.shape[0],
                'val_f1_macro': float(val_f1_new),
                'val_f1_drop': float(f1_drop),
                'stopped_reason': 'f1_drop',
                'retrain_time_seconds': retrain_time
            })
            break
        
        # Log iteration
        iteration_log.append({
            'iteration': iteration,
            'pseudo_labels_added': num_selected,
            'pool_size': current_X_pool.shape[0],
            'train_size': current_X_train.shape[0],
            'val_f1_macro': float(val_f1_new),
            'val_f1_drop': float(f1_drop),
            'retrain_time_seconds': retrain_time,
            'stopped_reason': 'max_iterations_reached'
        })
        
        # Stopping condition: Pool empty
        if current_X_pool.shape[0] == 0:
            print(f"  [STOP] Pool is empty")
            iteration_log[-1]['stopped_reason'] = 'pool_empty'
            break
    
    # ========== STEP 4: Save Results ==========
    print("\n[STEP 4] Save Results")
    print("-" * 80)
    
    # Save final model
    save_pickle(best_model, FINAL_MODEL_FILE)
    model_size = FINAL_MODEL_FILE.stat().st_size / (1024 * 1024)
    print(f"  ✓ Saved final model to: {FINAL_MODEL_FILE}")
    print(f"    File size: {model_size:.2f} MB")
    
    # Save iteration log
    with open(SELF_TRAINING_LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(iteration_log, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Saved iteration log to: {SELF_TRAINING_LOG_FILE}")
    
    # Save pseudo labels
    with open(PSEUDO_LABELS_FILE, 'w', encoding='utf-8') as f:
        json.dump(pseudo_labels_all, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Saved pseudo labels to: {PSEUDO_LABELS_FILE}")
    print(f"    Total pseudo labels: {len(pseudo_labels_all)}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "=" * 80)
    print("SELF-TRAINING LOOP COMPLETE")
    print("=" * 80)
    
    final_iteration = iteration_log[-1]
    total_pseudo_labels = sum(log['pseudo_labels_added'] for log in iteration_log[1:])
    
    print(f"""
✅ SUMMARY

Configuration:
  K per class: {K_PER_CLASS}
  Confidence threshold: {CONFIDENCE_THRESHOLD}
  Max iterations: {MAX_ITERATIONS}

Iterations completed: {final_iteration['iteration']}
Stopped reason: {final_iteration['stopped_reason']}

Pseudo Labels:
  Total added: {total_pseudo_labels}
  Final pool size: {final_iteration['pool_size']}
  Final train size: {final_iteration['train_size']}

Performance:
  Initial F1 (macro): {iteration_log[0]['val_f1_macro']:.4f}
  Best F1 (macro): {best_val_f1:.4f}
  Final F1 (macro): {final_iteration['val_f1_macro']:.4f}
  F1 improvement: {best_val_f1 - iteration_log[0]['val_f1_macro']:+.4f}

Iteration Details:
""")
    
    for log in iteration_log:
        print(f"  Iteration {log['iteration']}: "
              f"+{log['pseudo_labels_added']} labels, "
              f"Pool={log['pool_size']}, "
              f"F1={log['val_f1_macro']:.4f} "
              f"({log['val_f1_drop']:+.4f})")
    
    print(f"""
Outputs Saved:
  - final_model_calibrated.pkl ({model_size:.2f} MB)
  - self_training_log.json
  - pseudo_labels.json ({len(pseudo_labels_all)} labels)

Total Execution Time: {time.time() - total_start:.2f} seconds

Next Step:
  → Phase 3.5: Final Evaluation on Full Dataset
  → Use final_model_calibrated.pkl to predict all unlabeled data
  → Generate final predictions report
""")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
