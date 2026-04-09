#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRODUCTION: Tokenization - Word Tokenization using pyvi
- Apply ViTokenizer.tokenize() to full_text + metadata_text
- Create new fields: full_text_tokens, metadata_text_tokens
- Execute on all 13,159 documents in MongoDB
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
import os
from datetime import datetime

# Import pyvi
try:
    from pyvi import ViTokenizer
except ImportError:
    import subprocess
    subprocess.check_call(['pip', 'install', 'pyvi'])
    from pyvi import ViTokenizer

# Configure logging
log_dir = "../logs/transformation"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/02_tokenization_production.log", encoding='utf-8'),
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
    logger.info("PRODUCTION: TOKENIZATION (Word Tokenization)")
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
    
    logger.info(f"Starting tokenization on full_text + metadata_text...")
    logger.info(f"Creating new fields: full_text_tokens, metadata_text_tokens...")
    start_time = datetime.now()
    
    # Get all documents and apply tokenization
    updated_count = 0
    processed_count = 0
    error_count = 0
    total_tokens = {
        'full_text': 0,
        'metadata_text': 0
    }
    
    for doc in collection.find():
        processed_count += 1
        
        try:
            # Get original values
            full_text = doc.get('full_text', '')
            metadata_text = doc.get('metadata_text', '')
            
            # Tokenize full_text
            if full_text:
                try:
                    full_text_tokenized_str = ViTokenizer.tokenize(full_text)
                    full_text_tokens = full_text_tokenized_str.split()  # Convert to list
                    total_tokens['full_text'] += len(full_text_tokens)
                except Exception as e:
                    logger.warning(f"  Error tokenizing full_text for doc {doc['_id']}: {e}")
                    full_text_tokens = []
                    error_count += 1
            else:
                full_text_tokens = []
            
            # Tokenize metadata_text
            if metadata_text:
                try:
                    metadata_text_tokenized_str = ViTokenizer.tokenize(metadata_text)
                    metadata_text_tokens = metadata_text_tokenized_str.split()  # Convert to list
                    total_tokens['metadata_text'] += len(metadata_text_tokens)
                except Exception as e:
                    logger.warning(f"  Error tokenizing metadata_text for doc {doc['_id']}: {e}")
                    metadata_text_tokens = []
                    error_count += 1
            else:
                metadata_text_tokens = []
            
            # Update document
            collection.update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        'full_text_tokens': full_text_tokens,
                        'metadata_text_tokens': metadata_text_tokens
                    }
                }
            )
            
            updated_count += 1
            
        except Exception as e:
            logger.error(f"Error processing doc {doc.get('_id')}: {e}")
            error_count += 1
        
        # Progress indicator
        if processed_count % 2000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"  Progress: {processed_count}/{total_docs} ({(processed_count/total_docs)*100:.1f}%) - Errors: {error_count} - Elapsed: {elapsed:.1f}s")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"\n[TOKENIZATION COMPLETE]")
    logger.info(f"Total processed: {processed_count}")
    logger.info(f"Total updated: {updated_count}")
    logger.info(f"Total errors: {error_count}")
    logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    
    # Verify sample documents
    logger.info(f"\n[VERIFICATION]")
    samples = list(collection.find({'full_text_tokens': {'$ne': None}}).limit(3))
    
    for idx, sample in enumerate(samples, 1):
        full_text_tokens = sample.get('full_text_tokens', [])
        metadata_text_tokens = sample.get('metadata_text_tokens', [])
        
        full_text_token_count = len(full_text_tokens)
        metadata_text_token_count = len(metadata_text_tokens)
        
        # Show first 5 tokens
        full_text_preview = ' '.join(full_text_tokens[:5]) if full_text_tokens else '(empty)'
        metadata_text_preview = ' '.join(metadata_text_tokens[:5]) if metadata_text_tokens else '(empty)'
        
        logger.info(f"Sample {idx}:")
        logger.info(f"  full_text_tokens: {full_text_token_count} tokens")
        if full_text_tokens:
            logger.info(f"    Preview (first 5): {full_text_preview}")
        logger.info(f"  metadata_text_tokens: {metadata_text_token_count} tokens")
        if metadata_text_tokens:
            logger.info(f"    Preview (first 5): {metadata_text_preview}")
    
    # Statistics
    if processed_count > 0:
        avg_full_tokens = total_tokens['full_text'] / processed_count
        avg_metadata_tokens = total_tokens['metadata_text'] / processed_count
    else:
        avg_full_tokens = 0
        avg_metadata_tokens = 0
    
    # Summary
    logger.info(f"\n" + "=" * 80)
    logger.info("[PRODUCTION SUMMARY]")
    logger.info("=" * 80)
    logger.info(f"\nTotal documents processed: {processed_count}")
    logger.info(f"Documents updated: {updated_count}")
    logger.info(f"Tokenization errors: {error_count}")
    logger.info(f"\nToken statistics:")
    logger.info(f"  - Average tokens per document (full_text): {avg_full_tokens:.1f}")
    logger.info(f"  - Average tokens per document (metadata_text): {avg_metadata_tokens:.1f}")
    logger.info(f"  - Total full_text tokens: {total_tokens['full_text']:,}")
    logger.info(f"  - Total metadata_text tokens: {total_tokens['metadata_text']:,}")
    logger.info(f"\nExecution time: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    client.close()

if __name__ == "__main__":
    main()
