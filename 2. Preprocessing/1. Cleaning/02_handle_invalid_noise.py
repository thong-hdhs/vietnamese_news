"""
BƯỚC 2: XỬ LÝ DỮ LIỆU KHÔNG HỢP LỆ

Loại bỏ bài viết không có:
- title (NULL hoặc empty string)
- article_content (NULL hoặc empty string)
- source (NULL hoặc empty string)

Input: Collection 'news_data_preprocessing' từ MongoDB (sau bước 1)
Output: Updated database (removed invalid articles) + Detailed logs
Logs: handle_invalid_data.log
"""

import logging
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
        logging.FileHandler(log_dir / 'handle_invalid_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


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


def is_invalid_article(article):
    """
    Check if article is invalid
    
    Invalid if:
    - title is NULL or empty string
    - article_content is NULL or empty string
    """
    title = article.get('title')
    content = article.get('article_content')
    
    # Check title
    if title is None or (isinstance(title, str) and title.strip() == ''):
        return True, 'empty_title'
    
    # Check content
    if content is None or (isinstance(content, str) and content.strip() == ''):
        return True, 'empty_content'
    
    return False, None


def handle_invalid_data_delete(db, collection_name):
    """
    Delete invalid articles from database
    
    Returns: stats dict with deletion info
    """
    logger.info("\n" + "="*80)
    logger.info("STEP 2: HANDLE INVALID DATA - DELETE INVALID ARTICLES")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs_before = collection.count_documents({})
    logger.info(f"📊 Total documents before: {total_docs_before}")
    
    stats = {
        'total_before': total_docs_before,
        'total_deleted': 0,
        'empty_title': 0,
        'empty_content': 0,
        'deleted_ids': [],
        'delete_reasons': {
            'empty_title': [],
            'empty_content': []
        }
    }
    
    # Fetch all documents to check
    logger.info(f"\nFetching all {total_docs_before} documents...")
    documents = list(collection.find())
    logger.info(f"✅ Fetched {len(documents)} documents")
    
    # Check and collect invalid documents
    logger.info(f"\nAnalyzing documents for invalid data...")
    invalid_ids = []
    
    for idx, doc in enumerate(documents):
        is_invalid, reason = is_invalid_article(doc)
        
        if is_invalid:
            doc_id = str(doc.get('_id'))
            invalid_ids.append(doc_id)
            stats['total_deleted'] += 1
            stats['deleted_ids'].append(doc_id)
            
            if reason == 'empty_title':
                stats['empty_title'] += 1
                stats['delete_reasons']['empty_title'].append(doc_id)
                logger.info(f"   [{idx+1}/{total_docs_before}] ID:{doc_id} | INVALID: empty title")
            elif reason == 'empty_content':
                stats['empty_content'] += 1
                stats['delete_reasons']['empty_content'].append(doc_id)
                logger.info(f"   [{idx+1}/{total_docs_before}] ID:{doc_id} | INVALID: empty content")

        
        if (idx + 1) % 2000 == 0 and (idx + 1) < len(documents):
            logger.info(f"   Analyzed {idx + 1}/{len(documents)} documents...")
    
    logger.info(f"\n✅ Analysis complete! Found {stats['total_deleted']} invalid articles")
    
    # Delete invalid documents
    if invalid_ids:
        logger.info(f"\nDeleting {len(invalid_ids)} invalid articles from database...")
        
        from bson.objectid import ObjectId
        
        # Convert string IDs to ObjectId and delete
        object_ids = []
        for id_str in invalid_ids:
            try:
                object_ids.append(ObjectId(id_str))
            except Exception as e:
                logger.warning(f"   Could not convert ID {id_str}: {e}")
        
        # Delete all in one operation
        if object_ids:
            try:
                result = collection.delete_many({'_id': {'$in': object_ids}})
                logger.info(f"✅ Deleted {result.deleted_count} articles from database")
            except Exception as e:
                logger.error(f"❌ Error deleting documents: {e}")
                # Fallback: delete one by one
                logger.info("   Falling back to delete one by one...")
                for obj_id in object_ids:
                    try:
                        collection.delete_one({'_id': obj_id})
                    except Exception as e2:
                        logger.warning(f"   Could not delete {obj_id}: {e2}")
    else:
        logger.info("✅ No invalid articles found - nothing to delete")
    
    # Verify after deletion
    total_docs_after = collection.count_documents({})
    stats['total_after'] = total_docs_after
    
    logger.info(f"\n📊 Total documents after: {total_docs_after}")
    logger.info(f"📊 Removed: {total_docs_before - total_docs_after} articles")
    
    return stats


def validate_remaining_articles(db, collection_name):
    """Validate remaining articles have title, content, source"""
    logger.info("\n" + "="*80)
    logger.info("STEP 3: VALIDATION - CHECK REMAINING ARTICLES")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    
    validation_stats = {
        'total_validated': 0,
        'with_valid_title': 0,
        'with_valid_content': 0,
        'with_valid_source': 0,
        'missing_title': 0,
        'missing_content': 0,
        'missing_source': 0
    }
    
    logger.info(f"Validating {total_docs} remaining documents...")
    
    for idx, doc in enumerate(collection.find()):
        title = doc.get('title')
        content = doc.get('article_content')
        source = doc.get('source')
        
        # Check title
        if title and (isinstance(title, str) and title.strip() != ''):
            validation_stats['with_valid_title'] += 1
        else:
            validation_stats['missing_title'] += 1
        
        # Check content
        if content and (isinstance(content, str) and content.strip() != ''):
            validation_stats['with_valid_content'] += 1
        else:
            validation_stats['missing_content'] += 1
        
        # Check source
        if source and (isinstance(source, str) and source.strip() != ''):
            validation_stats['with_valid_source'] += 1
        else:
            validation_stats['missing_source'] += 1
        
        validation_stats['total_validated'] += 1
        
        if (idx + 1) % 2000 == 0 and (idx + 1) < total_docs:
            logger.info(f"   Validated {idx + 1}/{total_docs} documents...")
    
    logger.info(f"\n✅ Validation complete!")
    
    # Log validation results
    logger.info(f"\n📊 Validation Results:")
    logger.info(f"   Total remaining: {validation_stats['total_validated']}")
    logger.info(f"   With valid title: {validation_stats['with_valid_title']} ({validation_stats['with_valid_title']/validation_stats['total_validated']*100:.1f}%)")
    logger.info(f"   With valid content: {validation_stats['with_valid_content']} ({validation_stats['with_valid_content']/validation_stats['total_validated']*100:.1f}%)")
    logger.info(f"   With valid source: {validation_stats['with_valid_source']} ({validation_stats['with_valid_source']/validation_stats['total_validated']*100:.1f}%)")
    
    if validation_stats['missing_title'] > 0:
        logger.warning(f"   ⚠️  Missing title: {validation_stats['missing_title']} (UNEXPECTED!)")
    if validation_stats['missing_content'] > 0:
        logger.warning(f"   ⚠️  Missing content: {validation_stats['missing_content']} (UNEXPECTED!)")
    if validation_stats['missing_source'] > 0:
        logger.warning(f"   ⚠️  Missing source: {validation_stats['missing_source']} (UNEXPECTED!)")
    
    return validation_stats


def print_detailed_report(stats, validation_stats):
    """Print comprehensive report"""
    logger.info("\n" + "="*80)
    logger.info("📋 DETAILED PROCESSING REPORT")
    logger.info("="*80)
    
    logger.info(f"\n📊 OVERVIEW:")
    logger.info(f"   Before: {stats['total_before']} articles")
    logger.info(f"   After:  {stats['total_after']} articles")
    logger.info(f"   Removed: {stats['total_deleted']} articles ({stats['total_deleted']/stats['total_before']*100:.2f}%)")
    
    logger.info(f"\n📝 DELETION BREAKDOWN:")
    logger.info(f"   Empty title: {stats['empty_title']} articles ({stats['empty_title']/max(stats['total_before'],1)*100:.2f}%)")
    logger.info(f"   Empty content: {stats['empty_content']} articles ({stats['empty_content']/max(stats['total_before'],1)*100:.2f}%)")
    
    logger.info(f"\n📊 VALIDATION RESULTS (After Deletion):")
    logger.info(f"   Total validated: {validation_stats['total_validated']}")
    logger.info(f"   All have valid title: {validation_stats['missing_title'] == 0}")
    logger.info(f"   All have valid content: {validation_stats['missing_content'] == 0}")
    logger.info(f"   All have valid source: {validation_stats['missing_source'] == 0}")
    
    if stats['total_deleted'] > 0:
        logger.info(f"\n✅ SAMPLE DELETED ARTICLES (first 10):")
        for idx, doc_id in enumerate(stats['deleted_ids'][:10]):
            reason = None
            if doc_id in stats['delete_reasons']['empty_title']:
                reason = 'empty_title'
            elif doc_id in stats['delete_reasons']['empty_content']:
                reason = 'empty_content'
            elif doc_id in stats['delete_reasons']['empty_source']:
                reason = 'empty_source'
            logger.info(f"   {idx+1}. ID:{doc_id} | Reason: {reason}")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("HANDLE INVALID DATA - DELETE INVALID ARTICLES")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Mode: DIRECT DELETE (Removing documents from database)\n")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Connect to MongoDB
        logger.info("🔌 Connecting to MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Delete invalid articles
        logger.info("\n📥 Starting invalid data handling...")
        stats = handle_invalid_data_delete(db, collection_name)
        
        # 3. Validate remaining articles
        logger.info("\n📋 Starting validation...")
        validation_stats = validate_remaining_articles(db, collection_name)
        
        # 4. Print detailed report
        print_detailed_report(stats, validation_stats)
        
        # 5. Close connection
        client.close()
        logger.info("✅ Database connection closed")
        
        logger.info("\n" + "="*80)
        logger.info("✅ SCRIPT COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"\n📊 FINAL SUMMARY:")
        logger.info(f"   Deleted: {stats['total_deleted']} articles")
        logger.info(f"   Remaining: {stats['total_after']} articles")
        logger.info(f"   All remaining articles have valid title, content, and source")
        logger.info(f"\n✅ Ready for next step: Detect & Remove Outliers")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
