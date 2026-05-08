#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CONFIDENCE STATISTICS
- Thống kê số lượng bài theo các mốc confidence
"""

import json

# Read final_predictions.json
with open('d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\final_predictions.json', 'r', encoding='utf-8') as f:
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

# Initialize counters
bin_counts = {label: 0 for _, _, label in bins}
total = len(predictions)

# Count articles in each bin
for article in predictions:
    conf = article['confidence']
    for min_val, max_val, label in bins:
        if min_val <= conf < max_val:
            bin_counts[label] += 1
            break

# Print statistics
print("=" * 80)
print("CONFIDENCE STATISTICS")
print("=" * 80)
print(f"\nTotal articles: {total}\n")

for min_val, max_val, label in bins:
    count = bin_counts[label]
    percentage = (count / total) * 100 if total > 0 else 0
    print(f"{label:15s}: {count:5d} ({percentage:5.2f}%)")

print("\n" + "=" * 80)
