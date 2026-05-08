#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIDENCE BIN DISTRIBUTION VISUALIZATION
- Visualize confidence distribution by bins: <0.5, 0.5-0.6, 0.6-0.7, 0.7-0.8, 0.8-0.9, >=0.9
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Load final_predictions.json
final_predictions_path = Path("d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\final_predictions.json")
with open(final_predictions_path, 'r', encoding='utf-8') as f:
    predictions = json.load(f)

# Define confidence bins
bins = [
    (0.0, 0.5, "< 0.5"),
    (0.5, 0.6, "0.5 - 0.6"),
    (0.6, 0.7, "0.6 - 0.7"),
    (0.7, 0.8, "0.7 - 0.8"),
    (0.8, 0.9, "0.8 - 0.9"),
    (0.9, 1.0, ">= 0.9")
]

# Count articles in each bin
bin_counts = {label: 0 for _, _, label in bins}
total = len(predictions)

for article in predictions:
    conf = article['confidence']
    for min_val, max_val, label in bins:
        if min_val <= conf < max_val:
            bin_counts[label] += 1
            break

# Prepare data for plotting
labels = [label for _, _, label in bins]
counts = [bin_counts[label] for label in labels]
percentages = [(count / total) * 100 for count in counts]

# Create figure with subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle('Confidence Distribution Analysis', fontsize=16, fontweight='bold')

# Bar chart
bars = ax1.bar(labels, counts, color=['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd', '#00d2d3'], edgecolor='black', alpha=0.8)
ax1.set_xlabel('Confidence Range', fontsize=12)
ax1.set_ylabel('Count', fontsize=12)
ax1.set_title('Count by Confidence Range', fontsize=14, fontweight='bold')
ax1.grid(True, alpha=0.3, axis='y')

# Add count labels on bars
for bar, count, pct in zip(bars, counts, percentages):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{count}\n({pct:.1f}%)',
             ha='center', va='bottom', fontsize=10, fontweight='bold')

# Pie chart
colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd', '#00d2d3']
explode = [0.05 if pct < 10 else 0 for pct in percentages]
wedges, texts, autotexts = ax2.pie(percentages, labels=labels, autopct='%1.1f%%', 
                                     colors=colors, explode=explode, startangle=90,
                                     textprops={'fontsize': 10})
ax2.set_title('Percentage Distribution', fontsize=14, fontweight='bold')

# Make percentage text bold
for autotext in autotexts:
    autotext.set_fontsize(10)
    autotext.set_fontweight('bold')

plt.tight_layout()
plt.savefig('d:\\vietnamese_news\\3. Processing\\4. Self-Training Loop\\output\\confidence_bins_distribution.png', dpi=300, bbox_inches='tight')
print("Saved: confidence_bins_distribution.png")

# Print statistics
print("\n" + "=" * 80)
print("CONFIDENCE BIN STATISTICS")
print("=" * 80)
print(f"\nTotal articles: {total}\n")

for label, count, pct in zip(labels, counts, percentages):
    print(f"{label:15s}: {count:5d} ({pct:5.2f}%)")

print("\n" + "=" * 80)
