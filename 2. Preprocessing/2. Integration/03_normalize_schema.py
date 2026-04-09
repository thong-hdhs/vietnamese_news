#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TEST: Normalize schema - Test on ALL JSON files
- Convert publish_date to ISO 8601 format
- Map categories (11 → 7)
- Test on all JSON files
"""

import re
import json
import glob
from datetime import datetime

# Category mapping
CATEGORY_MAPPING = {
    'Kinh doanh': 'Kinh tế',
    'Khoa học - Công nghệ': 'Công nghệ',
    'Văn hóa giải trí': 'Giải trí',
    'Khoa học & CN': 'Công nghệ'
}

def parse_datetime_to_iso8601(date_str):
    """
    Convert Vietnamese datetime format to ISO 8601
    Input: "Thứ ba, 7/4/2026, 15:32 (GMT+7)"
    Output: "2026-04-07T15:32:00+07:00"
    """
    if not date_str:
        return None
    
    try:
        # Extract day/month/year, hour:minute, timezone
        pattern = r'(\d{1,2})/(\d{1,2})/(\d{4}),\s+(\d{1,2}):(\d{2}).*GMT([\+\-]\d{1,2})'
        match = re.search(pattern, date_str)
        
        if not match:
            return date_str
        
        day, month, year, hour, minute, tz_hour = match.groups()
        
        # Format timezone: +7 → +07:00
        tz_hour_int = int(tz_hour)
        tz_formatted = f"{tz_hour_int:+03d}:00"
        
        # Format to ISO 8601
        iso_date = f"{year}-{int(month):02d}-{int(day):02d}T{int(hour):02d}:{minute}:00{tz_formatted}"
        
        return iso_date
    
    except Exception as e:
        return date_str

def normalize_category(category):
    """Map category to normalized version"""
    if not isinstance(category, str):
        return category
    if category in CATEGORY_MAPPING:
        return CATEGORY_MAPPING[category]
    return category

print("=" * 80)
print("TEST: NORMALIZE SCHEMA - ALL JSON FILES")
print("=" * 80)

# Load all JSON files
json_files = glob.glob("../test/*.json")
if not json_files:
    print("[ERROR] No JSON files found")
    exit(1)

print(f"\nFound {len(json_files)} JSON files:\n")

all_samples = []
total_processed = 0

for json_file in sorted(json_files):
    filename = json_file.split('\\')[-1]
    print(f"Processing: {filename}")
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  [SKIP] Cannot load: {e}")
        continue
    
    if isinstance(data, dict):
        documents = list(data.values())
    else:
        documents = data
    
    print(f"  Loaded {len(documents)} documents, processing first 50...")
    
    # Process first 50 from each file
    samples = []
    for idx, doc in enumerate(documents[:50], 1):
        old_date = doc.get('publish_date', '')
        old_category = doc.get('category', '')
        
        # Handle non-string values
        if not isinstance(old_date, str):
            old_date = str(old_date) if old_date else ''
        if not isinstance(old_category, str):
            old_category = ''
        
        new_date = parse_datetime_to_iso8601(old_date)
        new_category = normalize_category(old_category)
        
        sample = {
            'file': filename,
            'index': idx,
            'site': doc.get('site'),
            'datetime': {
                'before': old_date[:40] if old_date else '',
                'after': new_date[:40] if new_date else '',
                'changed': old_date != new_date
            },
            'category': {
                'before': old_category,
                'after': new_category,
                'changed': old_category != new_category
            }
        }
        
        samples.append(sample)
    
    all_samples.extend(samples)
    total_processed += len(samples)
    
    # Count changes per file
    date_changes = sum(1 for s in samples if s['datetime']['changed'])
    category_changes = sum(1 for s in samples if s['category']['changed'])
    print(f"  ✓ DateTime changes: {date_changes}/{len(samples)} ({date_changes/len(samples)*100:.1f}%)")
    print(f"  ✓ Category changes: {category_changes}/{len(samples)} ({category_changes/len(samples)*100:.1f}%)")
    print()

# Overall statistics
print("=" * 80)
print("[OVERALL SUMMARY]")
print("=" * 80)

date_changes_total = sum(1 for s in all_samples if s['datetime']['changed'])
category_changes_total = sum(1 for s in all_samples if s['category']['changed'])

print(f"\nTotal samples processed: {len(all_samples)}")
print(f"Total DateTime changes: {date_changes_total}/{len(all_samples)} ({date_changes_total/len(all_samples)*100:.1f}%)")
print(f"Total Category changes: {category_changes_total}/{len(all_samples)} ({category_changes_total/len(all_samples)*100:.1f}%)")

# Show category mapping examples from samples
if category_changes_total > 0:
    print(f"\n[CATEGORY MAPPINGS FOUND]")
    mapping_stats = {}
    for sample in all_samples:
        if sample['category']['changed']:
            key = f"{sample['category']['before']} → {sample['category']['after']}"
            mapping_stats[key] = mapping_stats.get(key, 0) + 1
    
    for mapping, count in sorted(mapping_stats.items()):
        print(f"  {mapping}: {count} samples")
else:
    print(f"\n[NO CATEGORY MAPPINGS FOUND IN TEST DATA]")
    print(f"  This is expected - category mappings will be applied on live MongoDB data")

# Show datetime examples
print(f"\n[DATETIME CONVERSION EXAMPLES]")
for idx, sample in enumerate(all_samples[:5], 1):
    if sample['datetime']['changed']:
        print(f"  {idx}. {sample['datetime']['before']}")
        print(f"     → {sample['datetime']['after']}")

# Export to JSON
output_file = "../test/test_normalize_schema_all_samples.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_samples, f, ensure_ascii=False, indent=2)

print(f"\n[OK] Exported {len(all_samples)} samples to: {output_file}")

print("\n" + "=" * 80)
print("✅ TEST COMPLETE - READY FOR LIVE DATABASE EXECUTION")
print("=" * 80)
print("\n[NEXT STEP] Run 06_normalize_schema_production.py on live MongoDB")
