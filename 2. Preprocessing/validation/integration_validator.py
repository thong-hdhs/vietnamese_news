#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VALIDATION: Integration Phase (Phases 3.1 - 3.7)
- Field consolidation (14 → 9 fields)
- DateTime normalization (Vietnamese → ISO 8601)
- Category consolidation (11 → 7)
- Label removal (TITLE:, CONTENT:, TAGS:, etc.)
- Newline normalization (multiple → single)
- Data type consistency
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import logging
import os
from datetime import datetime
from collections import defaultdict
import re

# Configure logging
log_dir = "../logs/validation"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/03_integration_validator.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

MONGO_URI = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'

EXPECTED_FIELDS = [
    '_id', 'site', 'category', 'author', 'publish_date',
    'article_id', 'url', 'full_text', 'metadata_text'
]

UNEXPECTED_FIELDS = [
    'estimated_read_time_minutes', 'images', 'og_tags',
    'paragraph_count', 'word_count'
]

EXPECTED_CATEGORIES = [
    'Kinh tế', 'Giải trí', 'Thể thao', 'Giáo dục',
    'Thời sự', 'Sức khỏe', 'Công nghệ'
]

LABEL_PATTERNS = [
    r'^TITLE:\s*',
    r'CONTENT:\s*\n',
    r'SUBHEADINGS:\s*',
    r'TAGS:\s*',
    r'KEYWORDS:\s*',
    r'DESCRIPTION:\s*'
]

DATETIME_ISO8601 = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$')

def connect_mongodb():
    """Connect to MongoDB"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logger.info("✓ Connected to MongoDB")
        return client
    except PyMongoError as e:
        logger.error(f"Cannot connect to MongoDB: {e}")
        return None

def check_field_structure(collection, sample_size=100):
    """Check if collection has exactly 9 expected fields"""
    logger.info("\n" + "=" * 80)
    logger.info("[1] FIELD STRUCTURE VALIDATION")
    logger.info("=" * 80)
    
    issues = []
    samples = list(collection.find().limit(sample_size))
    
    if not samples:
        logger.error("No documents found")
        return []
    
    logger.info(f"Checking {len(samples)} documents for field structure...")
    
    for idx, doc in enumerate(samples, 1):
        doc_fields = set(doc.keys())
        
        # Check unexpected fields
        found_unexpected = doc_fields & set(UNEXPECTED_FIELDS)
        if found_unexpected:
            issues.append({
                'type': 'UNEXPECTED_FIELDS',
                'doc_id': str(doc.get('_id')),
                'fields': list(found_unexpected)
            })
        
        # Check missing expected fields
        missing = set(EXPECTED_FIELDS) - doc_fields
        if missing:
            issues.append({
                'type': 'MISSING_FIELDS',
                'doc_id': str(doc.get('_id')),
                'fields': list(missing)
            })
    
    if issues:
        logger.warning(f"❌ Found {len(issues)} documents with field issues")
        for issue in issues[:5]:  # Show first 5
            logger.warning(f"  - {issue['type']}: {issue['fields']}")
    else:
        logger.info(f"✅ All {len(samples)} documents have correct field structure (9 fields)")
    
    return issues

def check_datetime_format(collection, sample_size=100):
    """Check if datetime is in ISO 8601 format"""
    logger.info("\n" + "=" * 80)
    logger.info("[2] DATETIME FORMAT VALIDATION")
    logger.info("=" * 80)
    
    issues = []
    samples = list(collection.find().limit(sample_size))
    
    logger.info(f"Checking {len(samples)} documents for datetime format...")
    
    format_counts = defaultdict(int)
    
    for idx, doc in enumerate(samples, 1):
        publish_date = doc.get('publish_date')
        
        if publish_date is None:
            format_counts['NULL'] += 1
        elif isinstance(publish_date, str):
            if DATETIME_ISO8601.match(publish_date):
                format_counts['ISO8601'] += 1
            else:
                format_counts['INVALID'] += 1
                issues.append({
                    'type': 'INVALID_DATETIME',
                    'doc_id': str(doc.get('_id')),
                    'value': publish_date
                })
        else:
            format_counts['UNEXPECTED_TYPE'] += 1
            issues.append({
                'type': 'UNEXPECTED_TYPE',
                'doc_id': str(doc.get('_id')),
                'type_name': type(publish_date).__name__
            })
    
    logger.info(f"DateTime format distribution:")
    for fmt, count in sorted(format_counts.items(), key=lambda x: -x[1]):
        pct = (count / len(samples)) * 100
        logger.info(f"  - {fmt}: {count} ({pct:.1f}%)")
    
    if format_counts.get('ISO8601', 0) / len(samples) >= 0.95:
        logger.info(f"✅ DateTime format is valid (≥95% ISO 8601)")
    else:
        logger.warning(f"❌ DateTime format validation FAILED")
    
    return issues

def check_category_consolidation(collection, sample_size=1000):
    """Check if categories are consolidated to 7 categories"""
    logger.info("\n" + "=" * 80)
    logger.info("[3] CATEGORY CONSOLIDATION VALIDATION")
    logger.info("=" * 80)
    
    logger.info(f"Checking {sample_size} documents for category values...")
    
    categories = defaultdict(int)
    nulls = 0
    
    samples = list(collection.find({}, {'category': 1}).limit(sample_size))
    
    for doc in samples:
        cat = doc.get('category')
        if cat is None:
            nulls += 1
        else:
            categories[cat] += 1
    
    logger.info(f"\nCategory distribution ({len(categories)} unique):")
    
    issues = []
    invalid_categories = []
    
    for cat in sorted(categories.keys(), key=lambda x: -categories[x]):
        count = categories[cat]
        pct = (count / len(samples)) * 100
        logger.info(f"  - {cat}: {count} ({pct:.1f}%)")
        
        # Check if category is in expected list
        if cat not in EXPECTED_CATEGORIES:
            invalid_categories.append({
                'category': cat,
                'count': count
            })
    
    logger.info(f"  - (NULL/Unlabeled): {nulls} ({(nulls/len(samples))*100:.1f}%)")
    
    # All labeled categories should be in EXPECTED_CATEGORIES
    if all(cat in EXPECTED_CATEGORIES for cat in categories.keys()):
        logger.info(f"✅ All {len(categories)} labeled categories are in expected categories")
        logger.info(f"✅ Category consolidation is correct (unlabeled: {(nulls/len(samples))*100:.1f}%)")
    else:
        logger.warning(f"❌ Found {len(invalid_categories)} invalid categories")
        for cat_info in invalid_categories:
            logger.warning(f"   - {cat_info['category']}: {cat_info['count']} docs")
        issues = invalid_categories
    
    return issues

def check_label_removal(collection, sample_size=50):
    """Check if TITLE:, CONTENT:, TAGS: labels are removed from text"""
    logger.info("\n" + "=" * 80)
    logger.info("[4] LABEL REMOVAL VALIDATION")
    logger.info("=" * 80)
    
    issues = []
    samples = list(collection.find({'full_text': {'$ne': None}}).limit(sample_size))
    
    logger.info(f"Checking {len(samples)} documents for text labels...")
    
    labels_found_count = 0
    
    for doc in samples:
        full_text = doc.get('full_text', '')
        metadata_text = doc.get('metadata_text', '')
        
        combined_text = full_text + '\n' + metadata_text
        
        # Check for labels
        for pattern in LABEL_PATTERNS:
            if re.search(pattern, combined_text, re.MULTILINE):
                labels_found_count += 1
                issues.append({
                    'type': 'LABEL_FOUND',
                    'doc_id': str(doc.get('_id')),
                    'pattern': pattern,
                    'sample': combined_text[:100]
                })
                break
    
    if labels_found_count == 0:
        logger.info(f"✅ All {len(samples)} documents are free of labels (TITLE:, CONTENT:, etc.)")
    else:
        logger.warning(f"❌ Found {labels_found_count} documents with labels")
        for issue in issues[:3]:  # Show first 3
            logger.warning(f"  - {issue['pattern']}: {issue['sample'][:50]}...")
    
    return issues

def check_newline_normalization(collection, sample_size=50):
    """Check if multiple newlines are collapsed to single newline"""
    logger.info("\n" + "=" * 80)
    logger.info("[5] NEWLINE NORMALIZATION VALIDATION")
    logger.info("=" * 80)
    
    issues = []
    samples = list(collection.find({'full_text': {'$ne': None}}).limit(sample_size))
    
    logger.info(f"Checking {len(samples)} documents for newline normalization...")
    
    multiple_newlines_count = 0
    
    for doc in samples:
        full_text = doc.get('full_text', '')
        metadata_text = doc.get('metadata_text', '')
        
        combined_text = full_text + '\n' + metadata_text
        
        # Check for multiple consecutive newlines
        if '\n\n' in combined_text or '\n\n\n' in combined_text:
            multiple_newlines_count += 1
            issues.append({
                'type': 'MULTIPLE_NEWLINES',
                'doc_id': str(doc.get('_id')),
                'pattern': '\\n\\n' if '\n\n' in combined_text else '\\n\\n\\n'
            })
    
    if multiple_newlines_count == 0:
        logger.info(f"✅ All {len(samples)} documents have single newlines only")
    else:
        logger.warning(f"❌ Found {multiple_newlines_count} documents with multiple consecutive newlines")
        for issue in issues[:3]:
            logger.warning(f"  - {issue['doc_id']}: {issue['pattern']}")
    
    return issues

def check_data_type_consistency(collection, sample_size=50):
    """Check data type consistency across fields"""
    logger.info("\n" + "=" * 80)
    logger.info("[6] DATA TYPE CONSISTENCY VALIDATION")
    logger.info("=" * 80)
    
    issues = []
    samples = list(collection.find().limit(sample_size))
    
    logger.info(f"Checking {len(samples)} documents for type consistency...")
    
    type_checks = {
        '_id': (ObjectId, 'ObjectId'),
        'site': (str, 'string'),
        'category': (str, 'string or None'),
        'author': (str, 'string'),
        'publish_date': (str, 'string'),
        'article_id': (str, 'string'),
        'url': (str, 'string'),
        'full_text': (str, 'string'),
        'metadata_text': (str, 'string')
    }
    
    type_issues = defaultdict(list)
    
    for doc in samples:
        for field, (expected_type, desc) in type_checks.items():
            value = doc.get(field)
            
            if value is None:
                continue
            
            if not isinstance(value, expected_type):
                type_issues[field].append({
                    'doc_id': str(doc.get('_id')),
                    'expected': desc,
                    'actual': type(value).__name__
                })
    
    logger.info(f"Type consistency check:")
    for field in EXPECTED_FIELDS:
        if type_issues.get(field):
            logger.warning(f"  ❌ {field}: {len(type_issues[field])} inconsistencies")
            for issue in type_issues[field][:2]:
                logger.warning(f"     - Expected: {issue['expected']}, Got: {issue['actual']}")
        else:
            logger.info(f"  ✅ {field}: Consistent")
    
    return type_issues

def generate_summary(all_issues):
    """Generate validation summary"""
    logger.info("\n" + "=" * 80)
    logger.info("[VALIDATION SUMMARY]")
    logger.info("=" * 80)
    
    # Count issues from all validation steps
    total_issues = 0
    for key, value in all_issues.items():
        if isinstance(value, list):
            total_issues += len(value)
        elif isinstance(value, dict):
            # For type_issues dict, count all issues
            for field_issues in value.values():
                if isinstance(field_issues, list):
                    total_issues += len(field_issues)
    
    if total_issues == 0:
        logger.info("\n✅ ALL INTEGRATION PHASE VALIDATIONS PASSED")
        logger.info("\nIntegration Phase Status: READY FOR TRANSFORMATION")
        return True
    else:
        logger.warning(f"\n❌ VALIDATION FOUND {total_issues} ISSUES")
        logger.warning("\nIntegration Phase Status: NEEDS FIXES")
        return False

def main():
    logger.info("=" * 80)
    logger.info("INTEGRATION PHASE VALIDATION")
    logger.info("=" * 80)
    
    client = connect_mongodb()
    if not client:
        logger.error("Cannot proceed without MongoDB connection")
        return
    
    db = client['vietnamese_news']
    collection = db['news_data_preprocessing']
    
    total_docs = collection.count_documents({})
    logger.info(f"\nTotal documents in collection: {total_docs}")
    
    # Run all validations
    all_issues = {
        'field_structure': check_field_structure(collection),
        'datetime_format': check_datetime_format(collection),
        'category_consolidation': check_category_consolidation(collection),
        'label_removal': check_label_removal(collection),
        'newline_normalization': check_newline_normalization(collection),
        'data_type_consistency': check_data_type_consistency(collection)
    }
    
    # Generate summary
    passed = generate_summary(all_issues)
    
    logger.info(f"\nTimestamp: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    client.close()

if __name__ == "__main__":
    main()
