#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDATION SCRIPT FOR TF-IDF_SVD OUTPUT (PHASE 3.2)
Kiểm tra tính đúng đắn của output trước khi bước sang model training
"""

import json
import pickle
import numpy as np
from pathlib import Path
from scipy.sparse import issparse, csr_matrix
from collections import Counter

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "0. Data"

# File paths
TRAIN_FILE = DATA_DIR.parent.parent / "1. Data Split" / "0. data" / "train_data.json"
VAL_FILE = DATA_DIR.parent.parent / "1. Data Split" / "0. data" / "val_data.json"
UNLABELED_FILE = DATA_DIR.parent.parent / "1. Data Split" / "0. data" / "unlabeled_data.json"

X_TRAIN_FILE = DATA_DIR / "X_train.pkl"
X_VAL_FILE = DATA_DIR / "X_val.pkl"
X_UNLABELED_FILE = DATA_DIR / "X_unlabeled.pkl"
Y_TRAIN_FILE = DATA_DIR / "y_train.pkl"
Y_VAL_FILE = DATA_DIR / "y_val.pkl"
TFIDF_VECTORIZER_FILE = DATA_DIR / "tfidf_vectorizer.pkl"
SVD_MODEL_FILE = DATA_DIR / "svd_model.pkl"
STATS_FILE = DATA_DIR / "feature_stats.json"
TUNING_RESULTS_FILE = DATA_DIR / "tfidf_svd_tuning_results.json"


def read_jsonl(path: Path):
    """Read JSONL file"""
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_pickle(path: Path):
    """Load pickle file with error handling"""
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception as e:
        print(f"  [ERROR] Failed to load {path.name}: {e}")
        return None


def check_file_exists(path: Path, critical=True):
    """Check if file exists"""
    if path.exists():
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  ✓ {path.name}: {size_mb:.2f} MB")
        return True
    else:
        print(f"  ✗ {path.name}: NOT FOUND")
        if critical:
            raise FileNotFoundError(f"Critical file missing: {path}")
        return False


def check_matrix_properties(X, name):
    """Check sparse/dense matrix properties"""
    checks = {
        'is_sparse': issparse(X),
        'shape': X.shape if hasattr(X, 'shape') else None,
        'dtype': X.dtype if hasattr(X, 'dtype') else None,
        'has_nan': False,
        'has_inf': False,
        'nnz': X.nnz if issparse(X) else np.count_nonzero(X),
    }
    
    # Check for NaN/Inf
    if issparse(X):
        checks['has_nan'] = np.isnan(X.data).sum() > 0
        checks['has_inf'] = np.isinf(X.data).sum() > 0
    else:
        checks['has_nan'] = np.isnan(X).sum() > 0
        checks['has_inf'] = np.isinf(X).sum() > 0
    
    return checks


def check_labels(y, name):
    """Check label array properties"""
    checks = {
        'shape': y.shape if hasattr(y, 'shape') else None,
        'dtype': y.dtype if hasattr(y, 'dtype') else None,
        'min': int(y.min()) if len(y) > 0 else None,
        'max': int(y.max()) if len(y) > 0 else None,
        'unique_values': sorted(set(y.tolist())) if len(y) > 0 else [],
        'label_distribution': dict(Counter(y.tolist())) if len(y) > 0 else {},
    }
    
    # Check for invalid labels
    valid_range = (checks['min'] >= -1) and (checks['max'] <= 6)
    checks['labels_in_valid_range'] = valid_range
    
    return checks


def compare_with_input(X_shape, y_shape, input_rows, name):
    """Compare output with input data"""
    input_count = len(input_rows)
    output_count = X_shape[0]
    
    comparison = {
        'input_count': input_count,
        'output_count': output_count,
        'match': input_count == output_count,
        'lost_samples': input_count - output_count,
    }
    
    return comparison


def validate_transformer_consistency(tfidf, svd, X_train, X_val, X_unlabeled):
    """Check if transformers were applied consistently"""
    checks = {
        'tfidf_fitted': hasattr(tfidf, 'vocabulary_'),
        'svd_fitted': hasattr(svd, 'components_'),
        'tfidf_vocab_size': len(tfidf.vocabulary_) if hasattr(tfidf, 'vocabulary_') else None,
        'svd_n_components': svd.n_components if hasattr(svd, 'n_components_') else None,
        'svd_explained_variance': svd.explained_variance_ratio_.sum() if hasattr(svd, 'explained_variance_ratio_') else None,
    }
    
    # Check feature dimensions
    checks['feature_dim_train'] = X_train.shape[1]
    checks['feature_dim_val'] = X_val.shape[1]
    checks['feature_dim_unlabeled'] = X_unlabeled.shape[1]
    checks['feature_dims_match'] = (checks['feature_dim_train'] == checks['feature_dim_val'] == checks['feature_dim_unlabeled'])
    
    return checks


def main():
    print("=" * 80)
    print("VALIDATION: TF-IDF_SVD OUTPUT (PHASE 3.2)")
    print("=" * 80)
    
    all_passed = True
    warnings = []
    
    # ========== STEP 1: Check file existence ==========
    print("\n[STEP 1] FILE EXISTENCE CHECK")
    print("-" * 80)
    
    check_file_exists(X_TRAIN_FILE, critical=True)
    check_file_exists(X_VAL_FILE, critical=True)
    check_file_exists(X_UNLABELED_FILE, critical=True)
    check_file_exists(Y_TRAIN_FILE, critical=True)
    check_file_exists(Y_VAL_FILE, critical=True)
    check_file_exists(TFIDF_VECTORIZER_FILE, critical=True)
    check_file_exists(SVD_MODEL_FILE, critical=True)
    check_file_exists(STATS_FILE, critical=True)
    check_file_exists(TUNING_RESULTS_FILE, critical=False)
    
    # ========== STEP 2: Load input data for comparison ==========
    print("\n[STEP 2] LOAD INPUT DATA (for comparison)")
    print("-" * 80)
    
    train_rows = read_jsonl(TRAIN_FILE)
    val_rows = read_jsonl(VAL_FILE)
    unlabeled_rows = read_jsonl(UNLABELED_FILE)
    
    print(f"  ✓ train_data.json: {len(train_rows)} rows")
    print(f"  ✓ val_data.json: {len(val_rows)} rows")
    print(f"  ✓ unlabeled_data.json: {len(unlabeled_rows)} rows")
    
    # ========== STEP 3: Load output files ==========
    print("\n[STEP 3] LOAD OUTPUT FILES")
    print("-" * 80)
    
    X_train = load_pickle(X_TRAIN_FILE)
    X_val = load_pickle(X_VAL_FILE)
    X_unlabeled = load_pickle(X_UNLABELED_FILE)
    y_train = load_pickle(Y_TRAIN_FILE)
    y_val = load_pickle(Y_VAL_FILE)
    tfidf = load_pickle(TFIDF_VECTORIZER_FILE)
    svd = load_pickle(SVD_MODEL_FILE)
    
    with open(STATS_FILE, 'r', encoding='utf-8') as f:
        stats = json.load(f)
    
    if TUNING_RESULTS_FILE.exists():
        with open(TUNING_RESULTS_FILE, 'r', encoding='utf-8') as f:
            tuning_results = json.load(f)
    else:
        tuning_results = None
    
    # ========== STEP 4: Validate matrix properties ==========
    print("\n[STEP 4] MATRIX PROPERTIES VALIDATION")
    print("-" * 80)
    
    X_train_checks = check_matrix_properties(X_train, "X_train")
    X_val_checks = check_matrix_properties(X_val, "X_val")
    X_unlabeled_checks = check_matrix_properties(X_unlabeled, "X_unlabeled")
    
    for name, checks in [("X_train", X_train_checks), ("X_val", X_val_checks), ("X_unlabeled", X_unlabeled_checks)]:
        print(f"\n  {name}:")
        print(f"    - Shape: {checks['shape']}")
        print(f"    - Sparse: {checks['is_sparse']}")
        print(f"    - Dtype: {checks['dtype']}")
        print(f"    - Non-zero: {checks['nnz']:,}")
        print(f"    - Has NaN: {checks['has_nan']}")
        print(f"    - Has Inf: {checks['has_inf']}")
        
        if checks['has_nan'] or checks['has_inf']:
            all_passed = False
            warnings.append(f"{name} contains NaN or Inf values")
    
    # ========== STEP 5: Validate labels ==========
    print("\n[STEP 5] LABEL VALIDATION")
    print("-" * 80)
    
    y_train_checks = check_labels(y_train, "y_train")
    y_val_checks = check_labels(y_val, "y_val")
    
    for name, checks in [("y_train", y_train_checks), ("y_val", y_val_checks)]:
        print(f"\n  {name}:")
        print(f"    - Shape: {checks['shape']}")
        print(f"    - Dtype: {checks['dtype']}")
        print(f"    - Range: [{checks['min']}, {checks['max']}]")
        print(f"    - Unique values: {checks['unique_values']}")
        print(f"    - Labels in valid range (-1 to 6): {checks['labels_in_valid_range']}")
        print(f"    - Distribution: {checks['label_distribution']}")
        
        if not checks['labels_in_valid_range']:
            all_passed = False
            warnings.append(f"{name} contains invalid labels")
    
    # ========== STEP 6: Compare with input ==========
    print("\n[STEP 6] INPUT-OUTPUT CONSISTENCY")
    print("-" * 80)
    
    train_comparison = compare_with_input(X_train.shape, y_train.shape, train_rows, "train")
    val_comparison = compare_with_input(X_val.shape, y_val.shape, val_rows, "val")
    unlabeled_comparison = compare_with_input(X_unlabeled.shape, None, unlabeled_rows, "unlabeled")
    
    for name, comp in [("train", train_comparison), ("val", val_comparison), ("unlabeled", unlabeled_comparison)]:
        print(f"\n  {name}:")
        print(f"    - Input count: {comp['input_count']}")
        print(f"    - Output count: {comp['output_count']}")
        print(f"    - Match: {comp['match']}")
        print(f"    - Lost samples: {comp['lost_samples']}")
        
        if not comp['match']:
            all_passed = False
            warnings.append(f"{name} lost samples during processing")
    
    # ========== STEP 7: Validate transformer consistency ==========
    print("\n[STEP 7] TRANSFORMER CONSISTENCY")
    print("-" * 80)
    
    transformer_checks = validate_transformer_consistency(tfidf, svd, X_train, X_val, X_unlabeled)
    
    print(f"  TF-IDF fitted: {transformer_checks['tfidf_fitted']}")
    print(f"  TF-IDF vocab size: {transformer_checks['tfidf_vocab_size']}")
    print(f"  SVD fitted: {transformer_checks['svd_fitted']}")
    print(f"  SVD n_components: {transformer_checks['svd_n_components']}")
    print(f"  SVD explained variance: {transformer_checks['svd_explained_variance']:.4f}")
    print(f"  Feature dimensions match: {transformer_checks['feature_dims_match']}")
    print(f"    - Train: {transformer_checks['feature_dim_train']}")
    print(f"    - Val: {transformer_checks['feature_dim_val']}")
    print(f"    - Unlabeled: {transformer_checks['feature_dim_unlabeled']}")
    
    if not transformer_checks['feature_dims_match']:
        all_passed = False
        warnings.append("Feature dimensions do not match across datasets")
    
    # ========== STEP 8: Check stats file consistency ==========
    print("\n[STEP 8] STATS FILE CONSISTENCY")
    print("-" * 80)
    
    print(f"  Config from stats.json:")
    print(f"    - TF-IDF max_features: {stats['config']['tfidf_max_features']}")
    print(f"    - SVD n_components: {stats['config']['svd_n_components']}")
    print(f"    - Site onehot dim: {stats['config']['site_onehot_dim']}")
    
    print(f"  Output shapes from stats.json:")
    print(f"    - X_train: {stats['output_shapes']['X_train']}")
    print(f"    - X_val: {stats['output_shapes']['X_val']}")
    print(f"    - X_unlabeled: {stats['output_shapes']['X_unlabeled']}")
    
    # Compare with actual shapes
    stats_match = (
        stats['output_shapes']['X_train'] == list(X_train.shape) and
        stats['output_shapes']['X_val'] == list(X_val.shape) and
        stats['output_shapes']['X_unlabeled'] == list(X_unlabeled.shape)
    )
    print(f"  Stats match actual shapes: {stats_match}")
    
    if not stats_match:
        all_passed = False
        warnings.append("Stats file does not match actual output shapes")
    
    # ========== STEP 9: Check tuning results ==========
    if tuning_results:
        print("\n[STEP 9] TUNING RESULTS")
        print("-" * 80)
        
        best = tuning_results['best']
        print(f"  Best config:")
        print(f"    - TF-IDF max_features: {best['tfidf_max_features']}")
        print(f"    - SVD n_components: {best['svd_n_components']}")
        print(f"    - Macro F1: {best['macro_f1']:.4f}")
        print(f"    - Explained variance: {best['explained_variance_sum']:.4f}")
        print(f"    - Execution time: {best['elapsed_sec']:.2f}s")
        
        # Check if current config matches best
        current_matches_best = (
            stats['config']['tfidf_max_features'] == best['tfidf_max_features'] and
            stats['config']['svd_n_components'] == best['svd_n_components']
        )
        print(f"  Current config matches best: {current_matches_best}")
        
        if not current_matches_best:
            warnings.append("Current config does not match best tuning result")
    
    # ========== STEP 10: Feature dimension breakdown ==========
    print("\n[STEP 10] FEATURE DIMENSION BREAKDOWN")
    print("-" * 80)
    
    expected_svd = stats['config']['svd_n_components']
    expected_site = stats['config']['site_onehot_dim']
    expected_total = expected_svd + expected_site
    
    actual_total = X_train.shape[1]
    
    print(f"  Expected: {expected_svd} (SVD) + {expected_site} (site) = {expected_total}")
    print(f"  Actual: {actual_total}")
    print(f"  Match: {expected_total == actual_total}")
    
    if expected_total != actual_total:
        all_passed = False
        warnings.append(f"Feature dimension mismatch: expected {expected_total}, got {actual_total}")
    
    # ========== STEP 11: SVD explained variance check ==========
    print("\n[STEP 11] SVD EXPLAINED VARIANCE")
    print("-" * 80)
    
    explained_var = transformer_checks['svd_explained_variance']
    print(f"  Explained variance ratio: {explained_var:.4f} ({explained_var*100:.2f}%)")
    
    if explained_var < 0.5:
        warnings.append(f"SVD explained variance is low ({explained_var:.2%}), consider increasing components")
    elif explained_var > 0.8:
        print(f"  ✓ Good: Explained variance > 80%")
    else:
        print(f"  ⚠ Acceptable: Explained variance between 50-80%")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    
    if all_passed and len(warnings) == 0:
        print("\n✅ ALL CHECKS PASSED - Output is ready for next phase")
        print("\nRecommendations:")
        print("  → Proceed to model training (Phase 3.3)")
        print("  → Use X_train, y_train for training")
        print("  → Use X_val, y_val for validation")
        print("  → Use X_unlabeled for semi-supervised learning")
    else:
        print("\n⚠️  VALIDATION ISSUES FOUND")
        print(f"\nTotal warnings: {len(warnings)}")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")
        
        if not all_passed:
            print("\n❌ CRITICAL ERRORS - Do not proceed to next phase")
            print("   Please fix the issues above before continuing")
        else:
            print("\n⚠️  WARNINGS ONLY - Can proceed with caution")
    
    print("\n" + "=" * 80)
    
    return all_passed, len(warnings) == 0


if __name__ == "__main__":
    passed, no_warnings = main()
    exit(0 if (passed and no_warnings) else 1)
