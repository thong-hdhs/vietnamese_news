#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRODUCTION: Lowercasing - Unify uppercase and lowercase text
- Apply .lower() to full_text + metadata_text
- Execute on all 13,159 documents in MongoDB
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
import os
from datetime import datetime

# Configure logging
log_dir = "../logs/transformation"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/01_lowercasing_production.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

MONGO_URI = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'

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

def main():
    logger.info("=" * 80)
    logger.info("PRODUCTION: LOWERCASING")
    logger.info("=" * 80)
    
    client = connect_mongodb()
    if not client:
        logger.error("Cannot proceed without MongoDB connection")
        return
    
    db = client['vietnamese_news']
    collection = db['news_data_preprocessing']
    
    # Count total documents
    total_docs = collection.count_documents({})
    logger.info(f"\nTotal documents: {total_docs}")
    
    logger.info(f"Starting lowercasing on full_text + metadata_text...")
    start_time = datetime.now()
    
    # Get all documents and apply lowercasing
    updated_count = 0
    processed_count = 0
    
    for doc in collection.find():
        processed_count += 1
        
        # Get original values
        full_text = doc.get('full_text', '')
        metadata_text = doc.get('metadata_text', '')
        
        # Apply lowercasing
        full_text_lower = full_text.lower() if full_text else ''
        metadata_text_lower = metadata_text.lower() if metadata_text else ''
        
        # Check if any changes
        if full_text_lower != full_text or metadata_text_lower != metadata_text:
            collection.update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        'full_text': full_text_lower,
                        'metadata_text': metadata_text_lower
                    }
                }
            )
            updated_count += 1
        
        # Progress indicator
        if processed_count % 1000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"  Progress: {processed_count}/{total_docs} ({(processed_count/total_docs)*100:.1f}%) - Updated: {updated_count} - Elapsed: {elapsed:.1f}s")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"\n[LOWERCASING COMPLETE]")
    logger.info(f"Total processed: {processed_count}")
    logger.info(f"Total updated: {updated_count}")
    logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    
    # Verify sample documents
    logger.info(f"\n[VERIFICATION]")
    samples = list(collection.find({'full_text': {'$ne': None}}, {'full_text': 1, 'metadata_text': 1}).limit(3))
    
    all_lowercase = True
    for idx, sample in enumerate(samples, 1):
        full_text = sample.get('full_text', '')
        metadata_text = sample.get('metadata_text', '')
        
        # Check if all letters are lowercase
        has_uppercase = any(c.isupper() for c in full_text + metadata_text)
        
        if has_uppercase:
            all_lowercase = False
            logger.warning(f"Sample {idx}: Found uppercase letters")
        else:
            logger.info(f"Sample {idx}: ✅ All lowercase")
    
    if all_lowercase:
        logger.info(f"✅ All sampled documents are fully lowercase")
    
    # Summary
    logger.info(f"\n" + "=" * 80)
    logger.info("[PRODUCTION SUMMARY]")
    logger.info("=" * 80)
    logger.info(f"\nTotal documents processed: {processed_count}")
    logger.info(f"Documents with changes: {updated_count}")
    logger.info(f"Execution time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    client.close()

if __name__ == "__main__":
    main()
