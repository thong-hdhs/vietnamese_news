"""
PHASE 3.1: GỘP CÁC FIELDS (MERGE FIELDS)

Tạo 2 fields mới từ gộp:
1. full_text = title + subheadings + article_content
2. metadata_text = description + tags + meta_keywords + meta_description

Chưa xóa fields cũ (sẽ xóa ở bước 02)
"""

import logging
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime

# Setup directories
log_dir = Path('../logs/integration')
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'merge_field.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def connect_mongodb(connection_string):
    """Kết nối MongoDB"""
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.server_info()
        logger.info("[OK] Kết nối MongoDB thành công")
        return client
    except Exception as e:
        logger.error(f"[ERROR] Không thể kết nối MongoDB: {e}")
        raise


def safe_text(text):
    """Lấy text an toàn, bỏ qua None/empty"""
    if not text or not isinstance(text, str):
        return ""
    return text.strip()


def merge_full_text(doc):
    """
    Tạo full_text từ title + subheadings + article_content
    
    Format:
    TITLE: {title}
    
    SUBHEADINGS: {subheadings}
    
    CONTENT:
    {article_content}
    """
    parts = []
    
    # Title (required)
    title = safe_text(doc.get('title', ''))
    if title:
        parts.append(f"TITLE: {title}")
    
    # Subheadings (optional)
    subheadings = safe_text(doc.get('subheadings', ''))
    if subheadings:
        parts.append(f"SUBHEADINGS: {subheadings}")
    
    # Content (required)
    content = safe_text(doc.get('article_content', ''))
    if content:
        parts.append(f"CONTENT:\n{content}")
    
    # Join với \n\n
    full_text = "\n\n".join(parts)
    
    return full_text if full_text else None


def merge_metadata_text(doc):
    """
    Tạo metadata_text từ description + tags + meta_keywords + meta_description
    
    Format:
    {description}
    TAGS: {tags}
    {meta_keywords}
    {meta_description}
    """
    parts = []
    
    # Description (optional)
    description = safe_text(doc.get('description', ''))
    if description:
        parts.append(description)
    
    # Tags (optional)
    tags = doc.get('tags', [])
    if tags:
        if isinstance(tags, list):
            tags_str = ", ".join([str(t).strip() for t in tags if t])
        else:
            tags_str = str(tags).strip()
        
        if tags_str:
            parts.append(f"TAGS: {tags_str}")
    
    # Meta Keywords (optional)
    meta_keywords = safe_text(doc.get('meta_keywords', ''))
    if meta_keywords:
        parts.append(meta_keywords)
    
    # Meta Description (optional)
    meta_description = safe_text(doc.get('meta_description', ''))
    if meta_description:
        parts.append(meta_description)
    
    # Join với \n
    metadata_text = "\n".join(parts)
    
    return metadata_text if metadata_text else None


def merge_fields(db, collection_name):
    """
    Gộp fields thành full_text và metadata_text
    
    Returns: stats dict
    """
    logger.info("\n" + "="*80)
    logger.info("PHASE 3.1: GỘP CÁC FIELDS")
    logger.info("="*80)
    
    collection = db[collection_name]
    total_docs = collection.count_documents({})
    logger.info(f"\n[INFO] Tổng documents: {total_docs}")
    
    stats = {
        'total_documents': total_docs,
        'documents_processed': 0,
        'documents_updated': 0,
        'full_text_created': 0,
        'metadata_text_created': 0,
        'errors': 0,
        'sample_merges': []
    }
    
    # Fetch all documents
    logger.info(f"\nLấy tất cả {total_docs} documents...")
    documents = list(collection.find())
    logger.info(f"[OK] Đã lấy {len(documents)} documents\n")
    
    # Process documents
    logger.info("Đang gộp fields...")
    update_operations = []
    
    for idx, doc in enumerate(documents):
        doc_id = doc['_id']
        update_dict = {}
        
        # Merge full_text
        full_text = merge_full_text(doc)
        if full_text:
            update_dict['full_text'] = full_text
            stats['full_text_created'] += 1
        
        # Merge metadata_text
        metadata_text = merge_metadata_text(doc)
        if metadata_text:
            update_dict['metadata_text'] = metadata_text
            stats['metadata_text_created'] += 1
        
        # Thêm vào update operations
        if update_dict:
            stats['documents_updated'] += 1
            update_operations.append({
                'doc_id': doc_id,
                'updates': update_dict
            })
            
            # Lưu sample (5 cái đầu)
            if len(stats['sample_merges']) < 5:
                stats['sample_merges'].append({
                    'id': doc_id,
                    'full_text_preview': full_text[:100].replace('\n', '\\n') if full_text else None,
                    'metadata_text_preview': metadata_text[:60].replace('\n', '\\n') if metadata_text else None
                })
        
        stats['documents_processed'] += 1
        
        if (idx + 1) % 2000 == 0:
            logger.info(f"   Đã xử lý {idx + 1}/{len(documents)} documents...")
    
    logger.info(f"[OK] Xử lý xong!\n")
    
    # Batch update
    logger.info(f"Cập nhật {stats['documents_updated']} documents vào database...")
    if update_operations:
        try:
            for operation in update_operations:
                collection.update_one(
                    {'_id': operation['doc_id']},
                    {'$set': operation['updates']}
                )
            logger.info(f"[OK] Đã cập nhật {len(update_operations)} documents!")
        except Exception as e:
            logger.error(f"[ERROR] Lỗi batch update: {e}")
            stats['errors'] += 1
            raise
    
    return stats


def print_detailed_report(stats):
    """In báo cáo chi tiết"""
    logger.info("\n" + "="*80)
    logger.info("[BÁO CÁO GỘP FIELDS]")
    logger.info("="*80)
    
    logger.info(f"\n[THỐNG KÊ CHUNG]")
    logger.info(f"   Tổng documents: {stats['total_documents']}")
    logger.info(f"   Documents xử lý: {stats['documents_processed']}")
    logger.info(f"   Documents cập nhật: {stats['documents_updated']} ({stats['documents_updated']/stats['total_documents']*100:.1f}%)")
    
    logger.info(f"\n[FIELDS TẠO ĐƯỢC]")
    logger.info(f"   full_text tạo: {stats['full_text_created']} ({stats['full_text_created']/stats['total_documents']*100:.1f}%)")
    logger.info(f"   metadata_text tạo: {stats['metadata_text_created']} ({stats['metadata_text_created']/stats['total_documents']*100:.1f}%)")
    
    logger.info(f"\n[SAMPLE MERGES (5 cái đầu tiên)]")
    for idx, sample in enumerate(stats['sample_merges']):
        logger.info(f"\n   Sample {idx+1}: ID {sample['id']}")
        logger.info(f"   full_text: {sample['full_text_preview']}")
        logger.info(f"   metadata_text: {sample['metadata_text_preview']}")
    
    if stats['errors'] > 0:
        logger.warning(f"\n[LỖI]")
        logger.warning(f"   Tổng lỗi: {stats['errors']}")
    
    logger.info("\n" + "="*80)


def main():
    """Main execution"""
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    database_name = 'vietnamese_news'
    collection_name = 'news_data_preprocessing'
    
    logger.info("="*80)
    logger.info("PHASE 3.1: GỘP CÁC FIELDS (MERGE)")
    logger.info("="*80)
    logger.info(f"Database: {database_name}")
    logger.info(f"Collection: {collection_name}")
    logger.info(f"Mode: GỘP FIELDS (chưa xóa)\n")
    logger.info(f"Timestamp: {datetime.now().isoformat()}\n")
    
    try:
        # 1. Kết nối MongoDB
        logger.info("Kết nối MongoDB...")
        client = connect_mongodb(connection_string)
        db = client[database_name]
        
        # 2. Gộp fields
        logger.info("\nGộp fields...")
        stats = merge_fields(db, collection_name)
        
        # 3. In báo cáo
        print_detailed_report(stats)
        
        # 4. Đóng kết nối
        client.close()
        logger.info("[OK] Đóng kết nối database")
        
        logger.info("\n" + "="*80)
        logger.info("[OK] HOÀN TẤT THÀNH CÔNG!")
        logger.info("="*80)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"\nKết quả:")
        logger.info(f"   full_text tạo: {stats['full_text_created']}")
        logger.info(f"   metadata_text tạo: {stats['metadata_text_created']}")
        logger.info(f"   Documents cập nhật: {stats['documents_updated']}")
        logger.info(f"\nSẵn sàng cho bước tiếp theo: Remove Unnecessary Columns")
        
    except Exception as e:
        logger.error(f"[ERROR] Script thất bại: {e}")
        logger.error(f"Timestamp: {datetime.now().isoformat()}")
        raise


if __name__ == "__main__":
    main()
