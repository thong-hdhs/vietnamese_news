#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHASE 3.3: Remove unnecessary fields to optimize schema
Keep: _id, site, category, author, publish_date, article_id, url, full_text, metadata_text
Remove: article_content, title, subheadings, description, tags, meta_keywords, meta_description, 
         featured_image_alt, category_url, featured_image, video_url (11 fields)
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
from datetime import datetime

# Configure logging
log_dir = "../logs/integration"
import os
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/remove_fields.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Fields to remove
FIELDS_TO_REMOVE = [
    'article_content',
    'title',
    'subheadings',
    'description',
    'tags',
    'meta_keywords',
    'meta_description',
    'featured_image_alt',
    'category_url',
    'featured_image',
    'video_url'
]

# Fields to keep
FIELDS_TO_KEEP = [
    '_id',
    'site',
    'category',
    'author',
    'publish_date',
    'article_id',
    'url',
    'full_text',
    'metadata_text'
]

def connect_mongodb():
    """Connect to MongoDB"""
    try:
        client = MongoClient(
            'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority',
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ping')
        return client
    except PyMongoError as e:
        logger.error(f"[ERROR] Kết nối MongoDB thất bại: {e}")
        raise

def remove_fields(db, collection_name):
    """
    Remove unnecessary fields from all documents
    Returns statistics dictionary
    """
    collection = db[collection_name]
    
    logger.info("=" * 80)
    logger.info("PHASE 3.3: XÓA CÁC FIELDS KHÔNG CẦN THIẾT")
    logger.info("=" * 80)
    
    total_docs = collection.count_documents({})
    logger.info(f"\n[INFO] Tổng documents: {total_docs}")
    logger.info(f"\n[INFO] Fields sẽ xóa ({len(FIELDS_TO_REMOVE)}):")
    for field in FIELDS_TO_REMOVE:
        logger.info(f"   - {field}")
    
    logger.info(f"\n[INFO] Fields sẽ giữ lại ({len(FIELDS_TO_KEEP)}):")
    for field in FIELDS_TO_KEEP:
        logger.info(f"   - {field}")
    
    logger.info("\nDang xóa fields...")
    
    stats = {
        'total': total_docs,
        'removed_count': 0,
        'errors': 0
    }
    
    # Build unset query
    unset_query = {field: "" for field in FIELDS_TO_REMOVE}
    
    try:
        # Remove all fields at once
        result = collection.update_many(
            {},
            {'$unset': unset_query}
        )
        
        stats['removed_count'] = result.modified_count
        logger.info(f"[OK] Đã xóa fields từ {result.modified_count} documents\n")
    
    except Exception as e:
        logger.error(f"[ERROR] Lỗi khi xóa fields: {e}")
        stats['errors'] += 1
    
    return stats

def verify_schema(db, collection_name):
    """Verify that only desired fields remain"""
    collection = db[collection_name]
    
    logger.info("=" * 80)
    logger.info("VERIFY SCHEMA")
    logger.info("=" * 80)
    
    # Get a sample document
    sample = collection.find_one()
    if sample:
        logger.info(f"\nSample document fields:")
        for field in sorted(sample.keys()):
            value = sample[field]
            value_type = type(value).__name__
            
            if isinstance(value, str):
                if len(value) > 100:
                    logger.info(f"   ✓ {field} ({value_type}): {len(value)} chars")
                else:
                    logger.info(f"   ✓ {field} ({value_type}): {repr(value)}")
            else:
                logger.info(f"   ✓ {field} ({value_type})")
    
    # Check if removed fields still exist
    logger.info(f"\nChecking if removed fields are gone...")
    docs_with_removed_fields = 0
    
    for field in FIELDS_TO_REMOVE:
        count = collection.count_documents({field: {'$exists': True}})
        if count > 0:
            logger.warning(f"   ⚠️ Field '{field}' still exists in {count} documents")
            docs_with_removed_fields += count
        else:
            logger.info(f"   ✓ Field '{field}' successfully removed")
    
    # Check that required fields exist
    logger.info(f"\nChecking if required fields exist...")
    for field in FIELDS_TO_KEEP:
        if field == '_id':
            count = collection.count_documents({})
        else:
            count = collection.count_documents({field: {'$exists': True}})
        
        if count == collection.count_documents({}):
            logger.info(f"   ✓ Field '{field}' found in all {count} documents")
        else:
            logger.warning(f"   ⚠️ Field '{field}' found in only {count} documents")

def print_samples(db, collection_name, num_samples=3):
    """Print sample documents"""
    collection = db[collection_name]
    
    logger.info("\n" + "=" * 80)
    logger.info("SAMPLE DOCUMENTS (After Field Removal)")
    logger.info("=" * 80)
    
    samples = list(collection.find().limit(num_samples))
    
    for idx, doc in enumerate(samples, 1):
        logger.info(f"\nSample {idx}:")
        logger.info(f"   _id: {doc['_id']}")
        logger.info(f"   site: {doc.get('site', 'N/A')}")
        logger.info(f"   category: {doc.get('category', 'N/A')}")
        logger.info(f"   author: {doc.get('author', 'N/A')}")
        logger.info(f"   article_id: {doc.get('article_id', 'N/A')}")
        logger.info(f"   url: {doc.get('url', 'N/A')}")
        logger.info(f"   full_text (len={len(doc.get('full_text', ''))}): {doc.get('full_text', '')[:150]}...")
        logger.info(f"   metadata_text (len={len(doc.get('metadata_text', ''))}): {doc.get('metadata_text', '')[:150]}...")

def main():
    client = None
    try:
        # Connect
        logger.info("\nKết nối MongoDB...")
        client = connect_mongodb()
        logger.info("[OK] Đã kết nối MongoDB\n")
        
        db = client['vietnamese_news']
        collection_name = 'news_data_preprocessing'
        
        # Remove fields
        stats = remove_fields(db, collection_name)
        
        # Verify schema
        verify_schema(db, collection_name)
        
        # Print samples
        print_samples(db, collection_name)
        
        # Print report
        logger.info("\n" + "=" * 80)
        logger.info("[REMOVAL REPORT]")
        logger.info("=" * 80)
        logger.info(f"\n[STATISTICS]")
        logger.info(f"   Total documents: {stats['total']}")
        logger.info(f"   Documents modified: {stats['removed_count']}")
        logger.info(f"   Errors: {stats['errors']}")
        logger.info(f"\n[SCHEMA UPDATE]")
        logger.info(f"   Fields removed: {len(FIELDS_TO_REMOVE)}")
        logger.info(f"   Fields remaining: {len(FIELDS_TO_KEEP)}")
        logger.info(f"   Schema reduction: {(1 - len(FIELDS_TO_KEEP) / 18) * 100:.1f}%")
        
        logger.info("\n" + "=" * 80)
        logger.info("[OK] Database connection closed")
        logger.info("=" * 80)
        logger.info("\n[OK] FIELD REMOVAL COMPLETED - Schema optimized!")
        logger.info("=" * 80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        raise
    
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    main()
