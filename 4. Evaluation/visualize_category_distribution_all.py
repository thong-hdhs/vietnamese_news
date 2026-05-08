#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CATEGORY DISTRIBUTION VISUALIZATION FOR ALL 13001 DOCUMENTS
- Combine train (2674), val (472), and unlabeled with predicted labels (9855)
- Total: 13001 documents
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

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

# Load train data (JSONL format)
train_path = Path("d:\\vietnamese_news\\3. Processing\\1. Data Split\\0. data\\train_data.json")
train_data = []
with open(train_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            train_data.append(json.loads(line))

# Load val data (JSONL format)
val_path = Path("d:\\vietnamese_news\\3. Processing\\1. Data Split\\0. data\\val_data.json")
val_data = []
with open(val_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            val_data.append(json.loads(line))

# Load final predictions (unlabeled with predicted labels)
predictions_path = Path("d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\final_predictions.json")
with open(predictions_path, 'r', encoding='utf-8') as f:
    predictions = json.load(f)

# Count categories for each dataset
train_counts = {i: 0 for i in range(7)}
val_counts = {i: 0 for i in range(7)}
unlabeled_counts = {i: 0 for i in range(7)}

# Count train categories
for doc in train_data:
    cat_encoded = doc.get('category_encoded', -1)
    if cat_encoded in train_counts:
        train_counts[cat_encoded] += 1

# Count val categories
for doc in val_data:
    cat_encoded = doc.get('category_encoded', -1)
    if cat_encoded in val_counts:
        val_counts[cat_encoded] += 1

# Count unlabeled predicted categories
for pred in predictions:
    pred_label = pred.get('predicted_label', -1)
    if pred_label in unlabeled_counts:
        unlabeled_counts[pred_label] += 1

# Calculate total counts
total_counts = {i: train_counts[i] + val_counts[i] + unlabeled_counts[i] for i in range(7)}

# Prepare data for plotting
categories = [category_names[i] for i in range(7)]
train_values = [train_counts[i] for i in range(7)]
val_values = [val_counts[i] for i in range(7)]
unlabeled_values = [unlabeled_counts[i] for i in range(7)]
total_values = [total_counts[i] for i in range(7)]

# Create figure with subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
fig.suptitle('Category Distribution Analysis (Total: 13,001 Documents)', fontsize=16, fontweight='bold')

# Stacked bar chart
x = np.arange(len(categories))
width = 0.25

ax1.bar(x - width, train_values, width, label='Train (2,674)', color='#1dd1a1', edgecolor='black')
ax1.bar(x, val_values, width, label='Val (472)', color='#5f27cd', edgecolor='black')
ax1.bar(x + width, unlabeled_values, width, label='Unlabeled (9,855)', color='#00d2d3', edgecolor='black')

ax1.set_xlabel('Category', fontsize=12)
ax1.set_ylabel('Count', fontsize=12)
ax1.set_title('Category Distribution by Dataset', fontsize=14, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(categories, rotation=45, ha='right')
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3, axis='y')

# Add count labels on bars
for i, (t, v, u) in enumerate(zip(train_values, val_values, unlabeled_values)):
    if t > 0:
        ax1.text(i - width, t, str(t), ha='center', va='bottom', fontsize=8)
    if v > 0:
        ax1.text(i, v, str(v), ha='center', va='bottom', fontsize=8)
    if u > 0:
        ax1.text(i + width, u, str(u), ha='center', va='bottom', fontsize=8)

# Pie chart for total distribution
colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd', '#00d2d3', '#ff9ff3']
wedges, texts, autotexts = ax2.pie(total_values, labels=categories, autopct='%1.1f%%', 
                                     colors=colors, startangle=90, textprops={'fontsize': 10})
ax2.set_title('Total Category Distribution', fontsize=14, fontweight='bold')

# Make percentage text bold
for autotext in autotexts:
    autotext.set_fontsize(10)
    autotext.set_fontweight('bold')

plt.tight_layout()
plt.savefig('d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\category_distribution_all.png', dpi=300, bbox_inches='tight')
print("Saved: category_distribution_all.png")

# Print statistics
print("\n" + "=" * 80)
print("CATEGORY DISTRIBUTION STATISTICS (TOTAL: 13,001)")
print("=" * 80)
print(f"\n{'Category':<15} {'Train':<8} {'Val':<8} {'Unlabeled':<12} {'Total':<8} {'Percentage':<12}")
print("-" * 80)

for i in range(7):
    total = total_counts[i]
    pct = (total / 13001) * 100
    print(f"{category_names[i]:<15} {train_counts[i]:<8} {val_counts[i]:<8} {unlabeled_counts[i]:<12} {total:<8} {pct:>10.2f}%")

print("-" * 80)
print(f"{'TOTAL':<15} {sum(train_values):<8} {sum(val_values):<8} {sum(unlabeled_values):<12} {sum(total_values):<8} {'100.00%':>12}")
print("=" * 80)
