"""
BƯỚC 1: XỬ LÝ GIÁ TRỊ THIẾU - UPDATE TRỰC TIẾP DATABASE (OPTIMIZED)

Xử lý dữ liệu thiếu (NULL/None) trực tiếp trong MongoDB:
- author = "None" thay vì NULL/empty
- tags = [category] thay vì NULL/[]
- featured_image_alt = title nếu featured_image có nhưng alt NULL
- images = [] thay vì NULL

⚠️ MODE: DIRECT UPDATE (Chỉnh sửa luôn trong MongoDB)
✅ OPTIMIZED: Batch processing, minimal logging

Input: Collection 'news_data_preprocessing' từ MongoDB
Output: Updated database + Summary logs
Logs: handle_missing_values_update_db_fast.log
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
        logging.FileHandler(log_dir / 'handle_missing_values_update_db_fast.log'),
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


def handle_missing_values_batch_update(db, collection_name):
    """
    Handle missing values by updating MongoDB directly (OPTIMIZED)
    Uses batch operations for faster processing
    """
    logger.info("\n" + "="*80)
    logger.info("STEP 1: HANDLE MISSING VALUES - BATCH UPDATE (FAST)")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    logger.info(f"📊 Total documents in collection: {total_docs}\n")
    
    stats = {
        'total_processed': 0,
        'author_fixed': 0,
        'tags_fixed': 0,
        'featured_image_alt_fixed': 0,
        'images_fixed': 0,
        'total_updates': 0
    }
    
    # Fetch all documents
    logger.info(f"Fetching all {total_docs} documents...")
    documents = list(collection.find())
    logger.info(f"✅ Fetched {len(documents)} documents\n")
    
    # Process documents and collect updates
    logger.info(f"Processing documents...")
    update_operations = []
    
    for idx, doc in enumerate(documents):
        doc_id = doc['_id']
        updates = {}
        
        # 1️⃣ Check and fix author
        author = doc.get('author')
        if author is None or author == '' or author == 'None':
            updates['author'] = 'None'
            stats['author_fixed'] += 1
        
        # 2️⃣ Check and fix tags
        tags = doc.get('tags')
        if tags is None or (isinstance(tags, list) and len(tags) == 0):
            category = doc.get('category', 'Không xác định')
            updates['tags'] = [category]
            stats['tags_fixed'] += 1
        
        # 3️⃣ Check and fix featured_image_alt
        featured_image = doc.get('featured_image')
        featured_image_alt = doc.get('featured_image_alt')
        if featured_image is not None and featured_image_alt is None:
            title = doc.get('title', 'Ảnh minh họa')
            updates['featured_image_alt'] = title
            stats['featured_image_alt_fixed'] += 1
        
        # 4️⃣ Check and fix images
        images = doc.get('images')
        if images is None:
            updates['images'] = []
            stats['images_fixed'] += 1
        
        # Add update operation if needed
        if updates:
            from pymongo import UpdateOne
            update_operations.append(UpdateOne({'_id': doc_id}, {'$set': updates}))
            stats['total_updates'] += 1
        
        stats['total_processed'] += 1
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Processed {idx + 1}/{len(documents)} documents...")
    
    logger.info(f"\n✅ Processing complete! Found {len(update_operations)} documents to update")
    
    # Execute batch updates
    if update_operations:
        logger.info(f"\nExecuting batch updates...")
        try:
            from pymongo import UpdateOne
            result = collection.bulk_write(update_operations)
            logger.info(f"✅ Batch update completed!")
            logger.info(f"   Matched: {result.matched_count}")
            logger.info(f"   Modified: {result.modified_count}")
        except Exception as e:
            logger.error(f"❌ Error in batch update: {e}")
            raise
    
    return stats


def validate_after_update(db, collection_name):
    """Validate changes in database"""
    logger.info("\n" + "="*80)
    logger.info("STEP 2: VALIDATION (AFTER UPDATE)")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    
    validation_stats = {
        'author_null': 0,
        'author_none_string': 0,
        'tags_null': 0,
        'tags_empty_list': 0,
        'images_null': 0,
    }
    
    logger.info(f"Scanning {total_docs} documents for validation...")
    
    for idx, doc in enumerate(collection.find()):
        # Check author
        author = doc.get('author')
        if author is None:
            validation_stats['author_null'] += 1
        elif author == 'None':
            validation_stats['author_none_string'] += 1
        
        # Check tags
        tags = doc.get('tags')
        if tags is None:
            validation_stats['tags_null'] += 1
        elif isinstance(tags, list) and len(tags) == 0:
            validation_stats['tags_empty_list'] += 1
        
        # Check images
        images = doc.get('images')
        if images is None:
            validation_stats['images_null'] += 1
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Validated {idx + 1}/{total_docs} documents...")
    
    logger.info(f"\n✅ Validation complete!")
    
    return validation_stats


def print_summary(stats, validation_stats):
    """Print comprehensive summary"""
    logger.info("\n" + "="*80)
    logger.info("📋 PROCESSING SUMMARY")
    logger.info("="*80)
    
    logger.info(f"\n📊 OVERVIEW:")
    logger.info(f"   Total documents processed: {stats['total_processed']}")
    logger.info(f"   Documents updated: {stats['total_updates']}")
    
    logger.info(f"\n📝 FIXES APPLIED:")
    logger.info(f"   Author fixed: {stats['author_fixed']} documents")
    logger.info(f"   Tags fixed: {stats['tags_fixed']} documents")
    logger.info(f"   Featured image alt fixed: {stats['featured_image_alt_fixed']} documents")
    logger.info(f"   Images fixed: {stats['images_fixed']} documents")
    
    total_fixed = (stats['author_fixed'] + stats['tags_fixed'] + 
                   stats['featured_image_alt_fixed'] + stats['images_fixed'])
    logger.info(f"   Total fixes: {total_fixed}")
    
    logger.info(f"\n📊 VALIDATION RESULTS:")
    logger.info(f"   Author NULL: {validation_stats['author_null']} (should be 0)")
    logger.info(f"   Author 'None': {validation_stats['author_none_string']} (expected value)")
    logger.info(f"   Tags NULL: {validation_stats['tags_null']} (should be 0)")
    logger.info(f"   Tags empty list: {validation_stats['tags_empty_list']} (should be 0)")
    logger.info(f"   Images NULL: {validation_stats['images_null']} (should be 0)")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("HANDLE MISSING VALUES - FAST BATCH UPDATE")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Mode: BATCH UPDATE (Optimized for speed)\n")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Connect to MongoDB
        logger.info("🔌 Connecting to MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Handle missing values with batch update
        logger.info("\n📥 Starting missing values handling...")
        stats = handle_missing_values_batch_update(db, collection_name)
        
        # 3. Validate after update
        logger.info("\n📋 Starting validation...")
        validation_stats = validate_after_update(db, collection_name)
        
        # 4. Print summary
        print_summary(stats, validation_stats)
        
        # 5. Close connection
        client.close()
        logger.info("✅ Database connection closed")
        
        logger.info("\n" + "="*80)
        logger.info("✅ SCRIPT COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"\nResults:")
        logger.info(f"   Updated: {stats['total_updates']} documents")
        logger.info(f"   Changes: {stats['author_fixed'] + stats['tags_fixed'] + stats['featured_image_alt_fixed'] + stats['images_fixed']} field fixes")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
