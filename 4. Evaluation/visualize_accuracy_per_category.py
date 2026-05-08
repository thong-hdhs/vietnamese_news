#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ACCURACY PER CATEGORY VISUALIZATION
- Visualize percentage of correct predictions per category
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

# Load validation results
results_path = Path("d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\val_predictions_with_truth.json")
with open(results_path, 'r', encoding='utf-8') as f:
    results = json.load(f)

# Calculate accuracy per category
category_stats = {i: {'correct': 0, 'total': 0} for i in range(7)}

for result in results:
    true_label = result['true_label']
    is_correct = result['is_correct']
    
    category_stats[true_label]['total'] += 1
    if is_correct:
        category_stats[true_label]['correct'] += 1

# Calculate percentages
categories = []
accuracies = []
correct_counts = []
total_counts = []

for i in range(7):
    cat_name = category_names[i]
    correct = category_stats[i]['correct']
    total = category_stats[i]['total']
    accuracy = (correct / total * 100) if total > 0 else 0
    
    categories.append(cat_name)
    accuracies.append(accuracy)
    correct_counts.append(correct)
    total_counts.append(total)

# Create figure
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Accuracy Analysis: Predicted vs True Labels', fontsize=16, fontweight='bold')

# Bar chart
colors = ['#ff6b6b' if acc < 90 else '#1dd1a1' if acc >= 95 else '#feca57' for acc in accuracies]
bars = ax1.bar(categories, accuracies, color=colors, edgecolor='black', alpha=0.8)
ax1.set_xlabel('Category', fontsize=12)
ax1.set_ylabel('Accuracy (%)', fontsize=12)
ax1.set_title('Accuracy per Category', fontsize=14, fontweight='bold')
ax1.set_ylim([0, 100])
ax1.grid(True, alpha=0.3, axis='y')

# Add accuracy labels on bars
for bar, acc, correct, total in zip(bars, accuracies, correct_counts, total_counts):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{acc:.1f}%\n({correct}/{total})',
             ha='center', va='bottom', fontsize=10, fontweight='bold')

# Add threshold line
ax1.axhline(y=90, color='red', linestyle='--', alpha=0.5, label='90% threshold')
ax1.legend(fontsize=10)

# Pie chart for overall accuracy
total_correct = sum(correct_counts)
total_samples = sum(total_counts)
overall_accuracy = (total_correct / total_samples) * 100

pie_data = [total_correct, total_samples - total_correct]
pie_labels = [f'Correct ({total_correct})', f'Incorrect ({total_samples - total_correct})']
pie_colors = ['#1dd1a1', '#ff6b6b']

wedges, texts, autotexts = ax2.pie(pie_data, labels=pie_labels, autopct='%1.1f%%',
                                     colors=pie_colors, startangle=90, textprops={'fontsize': 12})
ax2.set_title(f'Overall Accuracy: {overall_accuracy:.2f}%', fontsize=14, fontweight='bold')

# Make percentage text bold
for autotext in autotexts:
    autotext.set_fontsize(12)
    autotext.set_fontweight('bold')

plt.tight_layout()
plt.savefig('d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\accuracy_per_category.png', dpi=300, bbox_inches='tight')
print("Saved: accuracy_per_category.png")

# Print statistics
print("\n" + "=" * 80)
print("ACCURACY PER CATEGORY")
print("=" * 80)
print(f"\n{'Category':<15} {'Correct':<10} {'Total':<10} {'Accuracy':<15}")
print("-" * 50)

for i in range(7):
    print(f"{category_names[i]:<15} {correct_counts[i]:<10} {total_counts[i]:<10} {accuracies[i]:>10.2f}%")

print("-" * 50)
print(f"{'OVERALL':<15} {total_correct:<10} {total_samples:<10} {overall_accuracy:>10.2f}%")
print("=" * 80)
