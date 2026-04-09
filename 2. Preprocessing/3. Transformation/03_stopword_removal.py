#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRODUCTION: Stopword Removal - Remove stopwords + punctuation
- Apply 2-layer cleaning (stopwords + regex filter)
- UPDATE full_text_tokens + metadata_text_tokens in-place on MongoDB
- Process all 13,159 documents
"""

import re
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# === VIETNAMESE STOPWORDS ===
VIETNAMESE_STOPWORDS = {
    # Verbs (tense/aspect/modal)
    'là', 'được', 'bị', 'có', 'làm', 'vẫn', 'cần', 'nên', 'phải', 'sẽ',
    'đã', 'đang', 'sắp', 'chắc', 'mong', 'muốn', 'chơi', 'gặp', 'gọi', 'hỏi',
    'kêu', 'nổi', 'rơi', 'sụp', 'thoát', 'trốn', 'xuất', 'bẻ', 'cắt', 'cộng',
    'cong', 'chân', 'chuốc', 'dẫn', 'dịp', 'dọn', 'dừng', 'dùng', 'kế', 'lẫn',
    
    # Conjunctions
    'và', 'hoặc', 'hay', 'nhưng', 'mà', 'như', 'cũng', 'thì', 'do', 'vì',
    'vậy', 'rồi', 'hoặc_là', 'cũng_như', 'hoàn_toàn_là',
    
    # Prepositions
    'ở', 'từ', 'trong', 'đến', 'với', 'về', 'dưới', 'trên', 'ngoài',
    'cạnh', 'cho', 'sau', 'trước', 'bên', 'giữa', 'ngang', 'dọc',
    'phía', 'phung',
    
    # Articles/Classifiers
    'cái', 'chiếc', 'những', 'các', 'nhiều',
    
    # Demonstratives
    'này', 'kia', 'đó', 'thế', 'ấy', 'đằng', 'gì',
    
    # Particles
    'ơi', 'ờ', 'thôi', 'chứ', 'không', 'không_phải', 'chứ_sao',
    
    # Common nouns
    'sự', 'vụ', 'việc', 'điều', 'cách', 'lần', 'người', 'bạn', 'anh',
    'chị', 'em', 'mình', 'tôi', 'tớ', 'ta', 'chúng_ta', 'chúng_tôi',
    
    # Numbers
    'một', 'hai', 'ba', 'bốn', 'năm', 'sáu', 'bảy', 'tám', 'chín', 'mười',
    'mươi', 'trăm', 'nghìn', 'triệu', 'tỉ', 'lẻ', 'phần', 'nửa',
    
    # Punctuation
    ',', '.', '!', '?', ';', ':', '-', '–', '—', '…',
    '(', ')', '[', ']', '{', '}', '"', "'", '`', ''', ''',
    '/', '\\', '@', '#', '$', '%', '^', '&', '*', '+', '=', '~', '|',
    
    # Whitespace
    '', ' ', '\n', '\t', '\r',
}

class RegexFilters:
    """Advanced filtering patterns"""
    
    PURE_PUNCTUATION = re.compile(r'^[!@#$%^&*()_\+\-=\[\]{};:\'",.<>?/\\|`~]+$')
    VALID_TOKEN = re.compile(r'^[a-z0-9_à-ỹ\-]+$', re.IGNORECASE)
    INVALID_CHARS = re.compile(r'[\u2000-\u206F\u2E00-\u2E7F\U0001F300-\U0001F9FF]')
    
    @staticmethod
    def is_valid(token):
        """Check if token should be kept"""
        
        if RegexFilters.PURE_PUNCTUATION.match(token):
            return False
        
        if RegexFilters.INVALID_CHARS.search(token):
            return False
        
        if not re.search(r'[a-z0-9À-Ỹ]', token, re.IGNORECASE):
            return False
        
        if len(token) < 1:
            return False
        
        return True

def remove_stopwords(tokens, stopwords_set):
    """2-layer cleaning: stopwords + regex validation"""
    cleaned = []
    
    for token in tokens:
        # Layer 1: Stopword matching
        if token in stopwords_set:
            continue
        
        # Layer 2: Regex validation
        if not RegexFilters.is_valid(token):
            continue
        
        cleaned.append(token)
    
    return cleaned

print("=" * 80)
print("PRODUCTION: STOPWORD REMOVAL (2-Layer: Stopwords + Regex)")
print("=" * 80)

# MongoDB Connection
try:
    client = MongoClient(
        'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    )
    db = client["vietnamese_news"]
    collection = db["news_data_preprocessing"]
    print("\n✓ Connected to MongoDB")
except Exception as e:
    print(f"\n[ERROR] MongoDB connection failed: {e}")
    exit(1)

# Count total documents
total_docs = collection.count_documents({})
print(f"Total documents in collection: {total_docs:,}")

start_time = datetime.now()
print(f"\nStarting processing... {start_time.isoformat()}\n")

total_processed = 0
total_updated = 0
total_errors = 0
total_tokens_before = 0
total_tokens_after = 0
sample_updates = []

try:
    for idx, doc in enumerate(collection.find({}), 1):
        doc_id = doc.get('_id')
        full_text_tokens = doc.get('full_text_tokens', [])
        metadata_text_tokens = doc.get('metadata_text_tokens', [])
        
        # Count before
        tokens_before_full = len(full_text_tokens) if isinstance(full_text_tokens, list) else 0
        tokens_before_meta = len(metadata_text_tokens) if isinstance(metadata_text_tokens, list) else 0
        
        # Clean tokens
        if isinstance(full_text_tokens, list):
            full_text_tokens_cleaned = remove_stopwords(full_text_tokens, VIETNAMESE_STOPWORDS)
        else:
            full_text_tokens_cleaned = []
        
        if isinstance(metadata_text_tokens, list):
            metadata_text_tokens_cleaned = remove_stopwords(metadata_text_tokens, VIETNAMESE_STOPWORDS)
        else:
            metadata_text_tokens_cleaned = []
        
        # Count after
        tokens_after_full = len(full_text_tokens_cleaned)
        tokens_after_meta = len(metadata_text_tokens_cleaned)
        
        total_tokens_before += tokens_before_full + tokens_before_meta
        total_tokens_after += tokens_after_full + tokens_after_meta
        
        # Update MongoDB (in-place)
        try:
            collection.update_one(
                {'_id': doc_id},
                {
                    '$set': {
                        'full_text_tokens': full_text_tokens_cleaned,
                        'metadata_text_tokens': metadata_text_tokens_cleaned
                    }
                }
            )
            total_updated += 1
            
            # Collect samples
            if len(sample_updates) < 3:
                sample_updates.append({
                    'doc_id': str(doc_id),
                    'full_text_before': tokens_before_full,
                    'full_text_after': tokens_after_full,
                    'metadata_text_before': tokens_before_meta,
                    'metadata_text_after': tokens_after_meta,
                })
        
        except Exception as e:
            total_errors += 1
            if total_errors <= 5:
                print(f"  [ERROR] Document {doc_id}: {e}")
        
        total_processed += 1
        
        # Progress update every 1000 documents
        if idx % 1000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = idx / elapsed if elapsed > 0 else 0
            print(f"  Processed {idx:,}/{total_docs:,} documents ({rate:.1f} docs/sec)")

except KeyboardInterrupt:
    print("\n[INTERRUPTED] Processing stopped by user")
except Exception as e:
    print(f"\n[ERROR] Processing failed: {e}")

end_time = datetime.now()
elapsed = (end_time - start_time).total_seconds()

# Summary
print("\n" + "=" * 80)
print("[PRODUCTION SUMMARY]")
print("=" * 80)

print(f"\nTotal processed: {total_processed:,}")
print(f"Total updated: {total_updated:,} ({'100%' if total_processed > 0 and total_updated == total_processed else f'{(total_updated/total_processed)*100:.1f}%'})")
print(f"Errors: {total_errors}")

print(f"\nToken reduction:")
print(f"  - Before: {total_tokens_before:,} tokens")
print(f"  - After: {total_tokens_after:,} tokens")
print(f"  - Removed: {total_tokens_before - total_tokens_after:,} tokens")
if total_tokens_before > 0:
    reduction_pct = ((total_tokens_before - total_tokens_after) / total_tokens_before) * 100
    print(f"  - Reduction: {reduction_pct:.2f}%")

print(f"\nExecution time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")

print(f"\n[SAMPLE UPDATES]")
for idx, sample in enumerate(sample_updates, 1):
    print(f"\nSample {idx} (Doc ID: {sample['doc_id']}):")
    print(f"  full_text_tokens: {sample['full_text_before']} → {sample['full_text_after']}")
    print(f"  metadata_text_tokens: {sample['metadata_text_before']} → {sample['metadata_text_after']}")

# Final verification
print(f"\n[VERIFICATION]")
verify_doc = collection.find_one({'full_text_tokens': {'$exists': True, '$ne': None}})
if verify_doc:
    print(f"✓ Sample document verified:")
    print(f"  - Fields after update: {list(verify_doc.keys())}")
    print(f"  - full_text_tokens type: {type(verify_doc.get('full_text_tokens'))}")
    print(f"  - metadata_text_tokens type: {type(verify_doc.get('metadata_text_tokens'))}")

print("\n" + "=" * 80)
print("✅ PRODUCTION COMPLETE")
print("=" * 80)
print(f"\nNote: Fields UPDATED (not new fields created):")
print(f"  - full_text_tokens (UPDATED with cleaned tokens)")
print(f"  - metadata_text_tokens (UPDATED with cleaned tokens)")
print(f"Timestamp: {end_time.isoformat()}")

# Close connection
client.close()
