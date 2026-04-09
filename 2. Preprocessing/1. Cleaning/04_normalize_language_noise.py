"""
BƯỚC 4: CHUẨN HÓA & GIẢM NHIỄU NGÔN NGỮ

Lỗi Unicode → fix (NFC normalization)
Khoảng trắng thừa → trim & collapse
Phần tử HTML → loại bỏ (decode entities)
Ký tự đặc biệt → remove (control chars)

Input: Collection 'news_data_preprocessing' từ MongoDB (sau bước 3)
Output: Updated database (cleaned fields) + Detailed logs
Logs: normalize_language_noise.log
"""

import logging
import unicodedata
import html
import re
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime

# Setup directories
log_dir = Path('../logs/cleaning')
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'normalize_language_noise.log'),
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


def normalize_unicode(text):
    """Normalize Unicode using NFC (Composed form)"""
    if not text or not isinstance(text, str):
        return text
    
    # Convert to NFC (Composed form)
    text = unicodedata.normalize('NFC', text)
    
    # Remove control characters (keep only valid Unicode)
    text = ''.join(c for c in text if unicodedata.category(c)[0] != 'C' or c in '\n\r\t')
    
    return text


def decode_html_entities(text):
    """Decode HTML entities"""
    if not text or not isinstance(text, str):
        return text
    
    # Decode HTML entities
    text = html.unescape(text)
    
    return text


def clean_whitespace(text):
    """Clean extra whitespace: collapse spaces/tabs but preserve meaningful newlines"""
    if not text or not isinstance(text, str):
        return text
    
    # First: Replace tabs with spaces
    text = text.replace('\t', ' ')
    
    # Second: Collapse multiple spaces into one (but preserve newlines for now)
    text = re.sub(r' +', ' ', text)
    
    # Third: Collapse multiple newlines into max 2 newlines (for paragraph breaks)
    text = re.sub(r'\n\n\n+', '\n\n', text)
    
    # Fourth: Clean spaces around newlines (trim spaces before/after newlines)
    text = re.sub(r' +\n', '\n', text)  # Remove trailing spaces before newline
    text = re.sub(r'\n +', '\n', text)  # Remove leading spaces after newline
    
    # Fifth: Trim leading/trailing whitespace from whole text
    text = text.strip()
    
    return text


def clean_special_chars(text):
    """Remove problematic special characters but keep Vietnamese diacritics"""
    if not text or not isinstance(text, str):
        return text
    
    # Remove zero-width characters, RTL marks, etc.
    text = text.replace('\u200b', '')  # Zero-width space
    text = text.replace('\u200c', '')  # Zero-width non-joiner
    text = text.replace('\u200d', '')  # Zero-width joiner
    text = text.replace('\ufeff', '')  # BOM
    
    return text


def normalize_text(text):
    """Apply all normalizations to text"""
    if not text or not isinstance(text, str):
        return text
    
    # Apply normalizations in order
    text = normalize_unicode(text)
    text = decode_html_entities(text)
    text = clean_whitespace(text)
    text = clean_special_chars(text)
    
    return text


def get_text_fields(sample_doc):
    """Detect all text/string fields from sample document"""
    text_fields = []
    
    for field, value in sample_doc.items():
        # Skip technical fields
        if field in ['_id', 'publish_date', 'updated_at', 'created_at']:
            continue
        
        # Include if value is string or None (potential text field)
        if isinstance(value, str) or value is None:
            text_fields.append(field)
    
    return sorted(text_fields)


def normalize_documents(db, collection_name):
    """
    Normalize all documents in collection
    Returns: stats dict with cleanup information
    """
    logger.info("\n" + "="*80)
    logger.info("STEP 4: NORMALIZE LANGUAGE NOISE")
    logger.info("="*80)
    logger.info("Operations: Unicode normalization, HTML entity decode, whitespace clean\n")
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    logger.info(f"[INFO] Total documents: {total_docs}")
    
    # Fetch first document to detect text fields
    logger.info(f"\nFetching documents...")
    documents = list(collection.find())
    logger.info(f"[OK] Fetched {len(documents)} documents\n")
    
    # Detect text fields from first document
    text_fields = get_text_fields(documents[0]) if documents else []
    logger.info(f"[INFO] Detected text fields: {text_fields}\n")
    
    stats = {
        'total_documents': total_docs,
        'fields_to_clean': text_fields,
        'documents_modified': 0,
        'fields_cleaned': {field: 0 for field in text_fields},
        'sample_changes': []
    }
    
    # Process documents
    logger.info("Normalizing documents...")
    update_operations = []
    
    for idx, doc in enumerate(documents):
        doc_id = doc['_id']
        has_changes = False
        update_dict = {}
        
        # Clean each field
        for field in stats['fields_to_clean']:
            original_value = doc.get(field, '')
            if original_value and isinstance(original_value, str):
                cleaned_value = normalize_text(original_value)
                
                if original_value != cleaned_value:
                    update_dict[field] = cleaned_value
                    stats['fields_cleaned'][field] += 1
                    has_changes = True
                    
                    # Save samples for logging
                    if len(stats['sample_changes']) < 5:
                        stats['sample_changes'].append({
                            'id': doc_id,
                            'field': field,
                            'before': original_value[:60],
                            'after': cleaned_value[:60]
                        })
        
        # Add to update operations
        if has_changes:
            stats['documents_modified'] += 1
            update_operations.append({
                'doc_id': doc_id,
                'updates': update_dict
            })
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Processed {idx + 1}/{len(documents)} documents...")
    
    logger.info(f"[OK] Processing complete!\n")
    
    # Execute batch updates
    logger.info(f"Updating {stats['documents_modified']} documents with normalizations...")
    if update_operations:
        try:
            for operation in update_operations:
                collection.update_one(
                    {'_id': operation['doc_id']},
                    {'$set': operation['updates']}
                )
            logger.info(f"[OK] Updated {len(update_operations)} documents!")
        except Exception as e:
            logger.error(f"[ERROR] Batch update failed: {e}")
            raise
    else:
        logger.info("[OK] No updates needed (all fields clean)")
    
    return collection, stats


def print_detailed_report(stats):
    """Print comprehensive report"""
    logger.info("\n" + "="*80)
    logger.info("[NORMALIZATION REPORT]")
    logger.info("="*80)
    
    logger.info(f"\n[STATISTICS]")
    logger.info(f"   Total documents: {stats['total_documents']}")
    logger.info(f"   Documents modified: {stats['documents_modified']} ({stats['documents_modified']/stats['total_documents']*100:.2f}%)")
    
    logger.info(f"\n[FIELDS CLEANED]")
    logger.info(f"   title cleaned: {stats['fields_cleaned']['title']} occurrences")
    logger.info(f"   article_content cleaned: {stats['fields_cleaned']['article_content']} occurrences")
    logger.info(f"   author cleaned: {stats['fields_cleaned']['author']} occurrences")
    logger.info(f"   Total field cleanups: {sum(stats['fields_cleaned'].values())}")
    
    logger.info(f"\n[SAMPLE CHANGES (first 5)]")
    for idx, change in enumerate(stats['sample_changes']):
        logger.info(f"   {idx+1}. Field: {change['field']} | ID: {change['id']}")
        logger.info(f"      Before: {change['before']}")
        logger.info(f"      After:  {change['after']}")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("NORMALIZE LANGUAGE NOISE")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Mode: NORMALIZE & CLEAN (no deletions)\n")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Normalize documents
        logger.info("\nNormalizing & cleaning documents...")
        collection, stats = normalize_documents(db, collection_name)
        
        # 3. Print detailed report
        print_detailed_report(stats)
        
        # 4. Close connection
        client.close()
        logger.info("[OK] Database connection closed")
        
        logger.info("\n" + "="*80)
        logger.info("[OK] SCRIPT COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"\nResults:")
        logger.info(f"   Documents modified: {stats['documents_modified']}")
        logger.info(f"   Fields cleaned: {sum(stats['fields_cleaned'].values())}")
        logger.info(f"   Total documents: {stats['total_documents']} (no deletions)")
        logger.info(f"\nReady for next phase: Integration & Transformation")
        
    except Exception as e:
        logger.error(f"[ERROR] Script failed: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
