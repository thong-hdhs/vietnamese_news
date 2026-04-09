"""
BƯỚC 3: PHÁT HIỆN & LOẠI BỎ OUTLIERS

Word count outliers: < 20 từ hoặc > 5000 từ
Duplicates: theo title + source hash

Input: Collection 'news_data_preprocessing' từ MongoDB (sau bước 2)
Output: Updated database (removed outliers + duplicates) + Detailed logs
Logs: detect_remove_outliers.log
"""

import logging
import hashlib
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime
import re

# Setup directories
log_dir = Path('../logs/cleaning')
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'detect_remove_outliers.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Parameters
MIN_WORDS = 20
MAX_WORDS = 5000


def connect_mongodb(connection_string):
    """Connect to MongoDB"""
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.server_info()
        logger.info("✅ Connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"❌ Cannot connect to MongoDB: {e}")
        raise


def count_words(text):
    """Count Vietnamese words using simple whitespace + Vietnamese tokenization"""
    if not text:
        return 0
    # Simple word count by splitting on whitespace
    words = text.strip().split()
    return len(words)


def get_duplicate_hash(article):
    """Generate hash key for duplicate detection: title + source"""
    title = article.get('title', '')
    if title is None:
        title = ''
    site = article.get('site', '')  # 'site' instead of 'source'
    if site is None:
        site = ''
    
    # Normalize for hashing
    hash_key = f"{title.strip().lower()}|{site.strip().lower()}"
    return hashlib.md5(hash_key.encode()).hexdigest()


def detect_outliers_and_duplicates(db, collection_name):
    """
    Detect word count outliers and duplicates
    Returns: stats dict with findings
    """
    logger.info("\n" + "="*80)
    logger.info("STEP 3: DETECT OUTLIERS & DUPLICATES")
    logger.info("="*80)
    logger.info(f"Parameters: MIN_WORDS={MIN_WORDS}, MAX_WORDS={MAX_WORDS}\n")
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    logger.info(f"📊 Total documents: {total_docs}")
    
    stats = {
        'total_before': total_docs,
        'word_count_distribution': {
            'less_20': 0,
            'between_20_5000': 0,
            'greater_5000': 0
        },
        'outliers_to_remove': [],
        'duplicates_to_remove': [],
        'document_hashes': {},  # hash -> {doc_id, publish_date}
    }
    
    # Fetch all documents
    logger.info(f"\nFetching all {total_docs} documents...")
    documents = list(collection.find())
    logger.info(f"✅ Fetched {len(documents)} documents\n")
    
    # STEP 1: Analyze word count
    logger.info("Analyzing word count...")
    for idx, doc in enumerate(documents):
        title = doc.get('title', '')
        if title is None:
            title = ''
        content = doc.get('article_content', '')
        if content is None:
            content = ''
        word_count = count_words(title + ' ' + content)
        
        # Categorize word count
        if word_count < MIN_WORDS:
            stats['word_count_distribution']['less_20'] += 1
            stats['outliers_to_remove'].append({
                'id': doc['_id'],
                'reason': f'word_count_too_short ({word_count} words)',
                'word_count': word_count
            })
        elif word_count > MAX_WORDS:
            stats['word_count_distribution']['greater_5000'] += 1
            stats['outliers_to_remove'].append({
                'id': doc['_id'],
                'reason': f'word_count_too_long ({word_count} words)',
                'word_count': word_count
            })
        else:
            stats['word_count_distribution']['between_20_5000'] += 1
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Analyzed {idx + 1}/{len(documents)} documents...")
    
    logger.info(f"\n✅ Word count analysis complete!")
    
    # STEP 2: Detect duplicates (only from non-outlier documents)
    logger.info("\nDetecting duplicates...")
    
    for doc in documents:
        # Skip if marked as outlier (word count issue)
        if any(oid == doc['_id'] for oid in [o['id'] for o in stats['outliers_to_remove']]):
            continue
        
        hash_key = get_duplicate_hash(doc)
        doc_id = doc['_id']
        pub_date = doc.get('publish_date')
        title = doc.get('title', '')
        if title is None:
            title = '' 
        
        if hash_key not in stats['document_hashes']:
            stats['document_hashes'][hash_key] = {
                'doc_id': doc_id,
                'publish_date': pub_date,
                'count': 1
            }
        else:
            # Duplicate found!
            existing = stats['document_hashes'][hash_key]
            
            # Compare publish dates: keep earliest, remove newer
            if pub_date and existing['publish_date']:
                if pub_date < existing['publish_date']:
                    # Current doc is older - keep current, remove existing
                    stats['duplicates_to_remove'].append({
                        'id': existing['doc_id'],
                        'reason': 'duplicate',
                        'hash': hash_key,
                        'keep_id': doc_id
                    })
                    stats['document_hashes'][hash_key] = {
                        'doc_id': doc_id,
                        'publish_date': pub_date,
                        'count': 2
                    }
                else:
                    # Existing doc is older - keep existing, remove current
                    stats['duplicates_to_remove'].append({
                        'id': doc_id,
                        'reason': 'duplicate',
                        'hash': hash_key,
                        'keep_id': existing['doc_id']
                    })
                    existing['count'] += 1
            else:
                # No date comparison possible - keep first, remove new
                stats['duplicates_to_remove'].append({
                    'id': doc_id,
                    'reason': 'duplicate',
                    'hash': hash_key,
                    'keep_id': existing['doc_id']
                })
                existing['count'] += 1
    
    logger.info(f"✅ Duplicate detection complete!")
    
    # Combine all IDs to remove (avoid duplicates in list)
    outlier_ids = set(o['id'] for o in stats['outliers_to_remove'])
    duplicate_ids = set(d['id'] for d in stats['duplicates_to_remove'])
    
    # Remove intersection (shouldn't happen, but just in case)
    all_ids_to_remove = outlier_ids | duplicate_ids
    
    stats['total_to_remove'] = len(all_ids_to_remove)
    stats['total_after'] = total_docs - len(all_ids_to_remove)
    
    return collection, stats, list(all_ids_to_remove)


def delete_outliers_and_duplicates(collection, ids_to_remove):
    """Delete outliers and duplicates from database"""
    logger.info(f"\nDeleting {len(ids_to_remove)} documents from database...")
    
    if ids_to_remove:
        try:
            result = collection.delete_many({'_id': {'$in': ids_to_remove}})
            logger.info(f"✅ Deleted {result.deleted_count} documents!")
            return result.deleted_count
        except Exception as e:
            logger.error(f"❌ Error deleting documents: {e}")
            raise
    else:
        logger.info("✅ No documents to delete")
        return 0


def print_detailed_report(stats):
    """Print comprehensive report"""
    logger.info("\n" + "="*80)
    logger.info("📋 DETAILED ANALYSIS REPORT")
    logger.info("="*80)
    
    logger.info(f"\n📊 WORD COUNT DISTRIBUTION (BEFORE):")
    logger.info(f"   < {MIN_WORDS} words (TOO SHORT):     {stats['word_count_distribution']['less_20']:>6} articles ({stats['word_count_distribution']['less_20']/stats['total_before']*100:>6.2f}%)")
    logger.info(f"   {MIN_WORDS}-{MAX_WORDS} words (VALID):    {stats['word_count_distribution']['between_20_5000']:>6} articles ({stats['word_count_distribution']['between_20_5000']/stats['total_before']*100:>6.2f}%)")
    logger.info(f"   > {MAX_WORDS} words (TOO LONG):      {stats['word_count_distribution']['greater_5000']:>6} articles ({stats['word_count_distribution']['greater_5000']/stats['total_before']*100:>6.2f}%)")
    
    logger.info(f"\n🔍 DUPLICATES DETECTED:")
    logger.info(f"   Total duplicates to remove: {len(stats['duplicates_to_remove'])} articles")
    
    logger.info(f"\n📝 REMOVAL SUMMARY:")
    logger.info(f"   Word count outliers: {len(stats['outliers_to_remove'])} articles")
    logger.info(f"   Duplicates: {len(stats['duplicates_to_remove'])} articles")
    logger.info(f"   Total to remove: {stats['total_to_remove']} articles ({stats['total_to_remove']/stats['total_before']*100:.2f}%)")
    
    logger.info(f"\n✅ SAMPLE OUTLIERS (first 10):")
    for idx, item in enumerate(stats['outliers_to_remove'][:10]):
        logger.info(f"   {idx+1}. ID:{item['id']} | Reason: {item['reason']}")
    
    logger.info(f"\n✅ SAMPLE DUPLICATES (first 10):")
    for idx, item in enumerate(stats['duplicates_to_remove'][:10]):
        logger.info(f"   {idx+1}. ID:{item['id']} | Keep: {item['keep_id']}")
    
    logger.info(f"\n📊 FINAL STATISTICS:")
    logger.info(f"   Before: {stats['total_before']:>6} articles")
    logger.info(f"   After:  {stats['total_after']:>6} articles")
    logger.info(f"   Removed: {stats['total_to_remove']:>6} articles")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("DETECT & REMOVE OUTLIERS")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Mode: DETECT & DELETE outliers + duplicates\n")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Connect to MongoDB
        logger.info("🔌 Connecting to MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Detect outliers and duplicates
        logger.info("\n📥 Detecting outliers & duplicates...")
        collection, stats, ids_to_remove = detect_outliers_and_duplicates(db, collection_name)
        
        # 3. Print detailed report (before deletion)
        print_detailed_report(stats)
        
        # 4. Delete outliers and duplicates
        logger.info("\n🗑️ Deleting outliers & duplicates...")
        deleted_count = delete_outliers_and_duplicates(collection, ids_to_remove)
        
        # 5. Close connection
        client.close()
        logger.info("✅ Database connection closed")
        
        logger.info("\n" + "="*80)
        logger.info("✅ SCRIPT COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"\nResults:")
        logger.info(f"   Removed: {stats['total_to_remove']} articles ({stats['total_to_remove']/stats['total_before']*100:.2f}%)")
        logger.info(f"   Remaining: {stats['total_after']} articles")
        logger.info(f"\nReady for next step: Normalize language noise")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
