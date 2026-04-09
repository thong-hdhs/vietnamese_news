"""
CLEANING VALIDATOR - Xác thực chất lượng dữ liệu sau các bước cleaning

Kiểm tra:
1. NULL/empty values trong critical fields
2. Text fields được clean properly (whitespace, HTML entities, Unicode)
3. Word counts reasonable
4. Không có duplicates
5. Data quality metrics
"""

import logging
import unicodedata
import re
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime
from collections import Counter

# Setup directories
log_dir = Path('../logs/validation')
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'cleaning_validation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def connect_mongodb(connection_string):
    """Connect to MongoDB"""
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.server_info()
        logger.info("[OK] Connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"[ERROR] Cannot connect to MongoDB: {e}")
        raise


def count_words(text):
    """Count words in text"""
    if not text or not isinstance(text, str):
        return 0
    return len(text.strip().split())


def check_html_entities(text):
    """Check if text contains HTML entities"""
    if not text or not isinstance(text, str):
        return False
    # Check for common HTML entities
    pattern = r'&[a-zA-Z0-9]+;'
    return bool(re.search(pattern, text))


def check_excessive_whitespace(text):
    """Check if text has excessive whitespace (2+ consecutive spaces)"""
    if not text or not isinstance(text, str):
        return False
    return bool(re.search(r'  +', text))


def check_control_chars(text):
    """Check if text contains control characters"""
    if not text or not isinstance(text, str):
        return False
    for char in text:
        if unicodedata.category(char)[0] == 'C':
            return True
    return False


def validate_cleaning(db, collection_name):
    """
    Validate data after cleaning steps
    Returns: stats dict with validation results
    """
    logger.info("\n" + "="*80)
    logger.info("CLEANING VALIDATION")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    logger.info(f"\n[INFO] Total documents: {total_docs}")
    
    stats = {
        'total_documents': total_docs,
        'critical_fields': ['title', 'article_content', 'site'],
        'text_fields': [],
        'null_checks': {},
        'empty_checks': {},
        'whitespace_issues': {},
        'html_entity_issues': {},
        'control_char_issues': {},
        'word_count_stats': {},
        'duplicate_count': 0,
        'issues_found': 0,
        'critical_issues': 0,
        'warnings': []
    }
    
    # Fetch all documents
    logger.info(f"\nFetching all {total_docs} documents...")
    documents = list(collection.find())
    logger.info(f"[OK] Fetched {len(documents)} documents\n")
    
    # Detect text fields
    text_fields = set()
    for doc in documents:
        for field, value in doc.items():
            if field not in ['_id', 'publish_date', 'updated_at', 'created_at']:
                if isinstance(value, str) or value is None:
                    text_fields.add(field)
    
    stats['text_fields'] = sorted(list(text_fields))
    logger.info(f"[INFO] Detected text fields: {stats['text_fields']}\n")
    
    # Initialize checks
    for field in stats['text_fields']:
        stats['null_checks'][field] = 0
        stats['empty_checks'][field] = 0
        stats['whitespace_issues'][field] = 0
        stats['html_entity_issues'][field] = 0
        stats['control_char_issues'][field] = 0
    
    for field in stats['critical_fields']:
        stats['word_count_stats'][field] = {'min': float('inf'), 'max': 0, 'avg': 0, 'sum': 0, 'count': 0}
    
    # Validate documents
    logger.info("Validating documents...")
    seen_titles = {}
    
    for idx, doc in enumerate(documents):
        doc_id = doc.get('_id')
        
        # Check each text field
        for field in stats['text_fields']:
            value = doc.get(field)
            
            # NULL check
            if value is None:
                stats['null_checks'][field] += 1
            
            # Empty string check
            elif isinstance(value, str) and not value.strip():
                stats['empty_checks'][field] += 1
            
            # Only validate if value is non-empty string
            elif isinstance(value, str) and value.strip():
                # Whitespace check
                if check_excessive_whitespace(value):
                    stats['whitespace_issues'][field] += 1
                
                # HTML entity check
                if check_html_entities(value):
                    stats['html_entity_issues'][field] += 1
                
                # Control char check
                if check_control_chars(value):
                    stats['control_char_issues'][field] += 1
        
        # Word count for critical fields
        for field in stats['critical_fields']:
            value = doc.get(field)
            if isinstance(value, str) and value.strip():
                wc = count_words(value)
                stats['word_count_stats'][field]['count'] += 1
                stats['word_count_stats'][field]['sum'] += wc
                stats['word_count_stats'][field]['min'] = min(stats['word_count_stats'][field]['min'], wc)
                stats['word_count_stats'][field]['max'] = max(stats['word_count_stats'][field]['max'], wc)
        
        # Duplicate check (by title + site)
        title = doc.get('title', '').strip().lower()
        site = doc.get('site', '').strip().lower()
        hash_key = f"{title}|{site}"
        
        if hash_key and hash_key in seen_titles:
            stats['duplicate_count'] += 1
        else:
            if hash_key:
                seen_titles[hash_key] = doc_id
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Validated {idx + 1}/{len(documents)} documents...")
    
    # Calculate averages
    for field in stats['critical_fields']:
        if stats['word_count_stats'][field]['count'] > 0:
            stats['word_count_stats'][field]['avg'] = stats['word_count_stats'][field]['sum'] / stats['word_count_stats'][field]['count']
    
    logger.info(f"[OK] Validation complete!\n")
    
    # Count issues
    for field in stats['text_fields']:
        field_issues = (stats['null_checks'].get(field, 0) + 
                       stats['empty_checks'].get(field, 0) +
                       stats['whitespace_issues'].get(field, 0) +
                       stats['html_entity_issues'].get(field, 0) +
                       stats['control_char_issues'].get(field, 0))
        
        stats['issues_found'] += field_issues
        
        # Critical fields
        if field in stats['critical_fields']:
            if stats['null_checks'].get(field, 0) > 0:
                stats['critical_issues'] += stats['null_checks'][field]
                stats['warnings'].append(f"CRITICAL: {field} has {stats['null_checks'][field]} NULL values")
            if stats['empty_checks'].get(field, 0) > 0:
                stats['critical_issues'] += stats['empty_checks'][field]
                stats['warnings'].append(f"CRITICAL: {field} has {stats['empty_checks'][field]} empty strings")
    
    return stats


def print_detailed_report(stats):
    """Print comprehensive validation report"""
    logger.info("\n" + "="*80)
    logger.info("[VALIDATION REPORT]")
    logger.info("="*80)
    
    logger.info(f"\n[OVERALL STATISTICS]")
    logger.info(f"   Total documents: {stats['total_documents']}")
    logger.info(f"   Total issues found: {stats['issues_found']}")
    logger.info(f"   Critical issues: {stats['critical_issues']}")
    
    logger.info(f"\n[NULL VALUE CHECKS (should all be 0)]")
    for field in stats['text_fields']:
        count = stats['null_checks'][field]
        if count > 0:
            logger.warning(f"   {field}: {count} NULLs - ISSUE!")
        else:
            logger.info(f"   {field}: 0 - OK")
    
    logger.info(f"\n[EMPTY STRING CHECKS (should all be 0)]")
    for field in stats['text_fields']:
        count = stats['empty_checks'][field]
        if count > 0:
            logger.warning(f"   {field}: {count} empty - ISSUE!")
        else:
            logger.info(f"   {field}: 0 - OK")
    
    logger.info(f"\n[WHITESPACE ISSUES (should be minimal)]")
    for field in stats['text_fields']:
        count = stats['whitespace_issues'][field]
        pct = count / stats['total_documents'] * 100 if stats['total_documents'] > 0 else 0
        if count > 0:
            logger.warning(f"   {field}: {count} ({pct:.2f}%) - Has excessive spaces")
        else:
            logger.info(f"   {field}: 0 - OK")
    
    logger.info(f"\n[HTML ENTITY ISSUES (should be 0)]")
    for field in stats['text_fields']:
        count = stats['html_entity_issues'][field]
        if count > 0:
            logger.warning(f"   {field}: {count} - ISSUE! Not decoded properly")
        else:
            logger.info(f"   {field}: 0 - OK")
    
    logger.info(f"\n[CONTROL CHARACTER ISSUES (should be 0)]")
    for field in stats['text_fields']:
        count = stats['control_char_issues'][field]
        if count > 0:
            logger.warning(f"   {field}: {count} - ISSUE! Contains control chars")
        else:
            logger.info(f"   {field}: 0 - OK")
    
    logger.info(f"\n[WORD COUNT STATISTICS (Critical Fields)]")
    for field in stats['critical_fields']:
        wc_stats = stats['word_count_stats'][field]
        if wc_stats['count'] > 0:
            logger.info(f"   {field}:")
            logger.info(f"      Count: {wc_stats['count']}")
            logger.info(f"      Min: {wc_stats['min']}")
            logger.info(f"      Max: {wc_stats['max']}")
            logger.info(f"      Avg: {wc_stats['avg']:.1f}")
            
            if wc_stats['min'] < 5:
                logger.warning(f"      WARNING: Min word count too low!")
        else:
            logger.warning(f"   {field}: No valid data!")
    
    logger.info(f"\n[DUPLICATE CHECK]")
    logger.info(f"   Duplicates found: {stats['duplicate_count']}")
    if stats['duplicate_count'] > 0:
        logger.warning(f"   NOTE: Duplicates checked but not fully removed at this stage")
    
    logger.info(f"\n[CRITICAL WARNINGS]")
    if stats['warnings']:
        for warning in stats['warnings']:
            logger.warning(f"   {warning}")
    else:
        logger.info(f"   No critical warnings - Data looks good!")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("CLEANING VALIDATION REPORT")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Validate cleaning
        logger.info("\nValidating cleaned data...")
        stats = validate_cleaning(db, collection_name)
        
        # 3. Print detailed report
        print_detailed_report(stats)
        
        # 4. Close connection
        client.close()
        logger.info("[OK] Database connection closed")
        
        # 5. Final status
        logger.info("\n" + "="*80)
        if stats['critical_issues'] == 0:
            logger.info("[OK] VALIDATION PASSED - Data is clean and ready!")
        else:
            logger.warning(f"[WARNING] Found {stats['critical_issues']} critical issues")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        
    except Exception as e:
        logger.error(f"[ERROR] Validation failed: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
