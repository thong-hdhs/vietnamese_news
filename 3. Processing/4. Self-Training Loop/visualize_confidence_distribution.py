#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIDENCE DISTRIBUTION VISUALIZATION
- Visualize confidence distribution across self-training iterations
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Load pseudo_labels.json
pseudo_labels_path = Path("d:\\vietnamese_news\\3. Processing\\4. Self-Training Loop\\output\\pseudo_labels.json")
with open(pseudo_labels_path, 'r', encoding='utf-8') as f:
    pseudo_labels = json.load(f)

# Group confidence scores by iteration
iteration_confidences = {}
for label in pseudo_labels:
    iteration = label['iteration']
    confidence = label['confidence']
    
    if iteration not in iteration_confidences:
        iteration_confidences[iteration] = []
    iteration_confidences[iteration].append(confidence)

# Sort iterations
sorted_iterations = sorted(iteration_confidences.keys())

# Create figure
fig, axes = plt.subplots(4, 4, figsize=(20, 16))
fig.suptitle('Confidence Distribution Across Self-Training Iterations', fontsize=16, fontweight='bold')

# Plot histogram for each iteration
for idx, iteration in enumerate(sorted_iterations):
    row = idx // 4
    col = idx % 4
    
    if idx >= 16:
        break
    
    confidences = iteration_confidences[iteration]
    ax = axes[row, col]
    
    # Plot histogram
    ax.hist(confidences, bins=20, edgecolor='black', alpha=0.7, color='steelblue')
    
    # Add statistics
    mean_conf = np.mean(confidences)
    median_conf = np.median(confidences)
    min_conf = np.min(confidences)
    max_conf = np.max(confidences)
    
    ax.set_title(f'Iteration {iteration} (n={len(confidences)})', fontsize=10, fontweight='bold')
    ax.set_xlabel('Confidence', fontsize=8)
    ax.set_ylabel('Count', fontsize=8)
    ax.grid(True, alpha=0.3)
    
    # Add stats text
    stats_text = f'Mean: {mean_conf:.3f}\nMedian: {median_conf:.3f}\nMin: {min_conf:.3f}\nMax: {max_conf:.3f}'
    ax.text(0.95, 0.95, stats_text, transform=ax.transAxes, fontsize=7,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# Hide empty subplots
for idx in range(len(sorted_iterations), 16):
    row = idx // 4
    col = idx % 4
    axes[row, col].axis('off')

plt.tight_layout()
plt.savefig('d:\\vietnamese_news\\3. Processing\\4. Self-Training Loop\\output\\confidence_distribution.png', dpi=300, bbox_inches='tight')
print("Saved: confidence_distribution.png")

# Also create a boxplot for comparison
fig2, ax2 = plt.subplots(figsize=(14, 8))
fig2.suptitle('Confidence Boxplot Across Iterations', fontsize=16, fontweight='bold')

# Prepare data for boxplot
data_to_plot = [iteration_confidences[iter] for iter in sorted_iterations]
iteration_labels = [f'Iter {iter}' for iter in sorted_iterations]

bp = ax2.boxplot(data_to_plot, labels=iteration_labels, patch_artist=True)

# Color the boxes
for patch in bp['boxes']:
    patch.set_facecolor('lightblue')

ax2.set_xlabel('Iteration', fontsize=12)
ax2.set_ylabel('Confidence', fontsize=12)
ax2.grid(True, alpha=0.3, axis='y')
ax2.set_ylim([0.5, 1.0])

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('d:\\vietnamese_news\\3. Processing\\4. Self-Training Loop\\output\\confidence_boxplot.png', dpi=300, bbox_inches='tight')
print("Saved: confidence_boxplot.png")

# Print summary statistics
print("\n" + "=" * 80)
print("CONFIDENCE STATISTICS SUMMARY")
print("=" * 80)
for iteration in sorted_iterations:
    confidences = iteration_confidences[iteration]
    print(f"\nIteration {iteration}:")
    print(f"  Count: {len(confidences)}")
    print(f"  Mean: {np.mean(confidences):.4f}")
    print(f"  Median: {np.median(confidences):.4f}")
    print(f"  Std: {np.std(confidences):.4f}")
    print(f"  Min: {np.min(confidences):.4f}")
    print(f"  Max: {np.max(confidences):.4f}")
