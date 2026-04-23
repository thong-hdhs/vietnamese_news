import json
import os
import time
from pymongo import MongoClient
from bson import json_util
from sklearn.model_selection import train_test_split

# --- THÔNG SỐ DATABASE ---
MONGO_URL = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"              
COLLECTION_NAME = "news_data_preprocessing"             

def save_json_lines(data_list, file_path):
    """Ghi dữ liệu từng dòng một để tiết kiệm RAM tuyệt đối"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data_list:
            line = json.dumps(item, ensure_ascii=False)
            f.write(line + '\n')

def main():
    try:
        os.makedirs('0. data', exist_ok=True)
        print("🚀 Đang kết nối MongoDB (Chế độ Batching chống ngắt mạng)...")
        client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=60000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # 1. Kéo dữ liệu theo từng đợt 1000 bài
        all_data = []
        batch_size = 1000
        total_to_get = 13001
        
        for i in range(0, total_to_get, batch_size):
            print(f"⏳ Đang tải đợt: Bài {i} đến {min(i + batch_size, total_to_get)}...")
            
            # Dùng skip và limit để chia nhỏ yêu cầu gửi lên Server
            batch_cursor = collection.find({}).skip(i).limit(batch_size)
            batch_list = list(batch_cursor)
            
            if not batch_list: break # Hết dữ liệu thì dừng
            
            # Chuyển sang Dict bình thường ngay lập tức
            clean_batch = [json.loads(json_util.dumps(d)) for d in batch_list]
            all_data.extend(clean_batch)
            
            # Nghỉ 0.3 giây cho đường truyền ổn định
            time.sleep(0.3)

        print(f"✅ Đã hốt trọn {len(all_data)} bài báo!")

        # 2. Phân tách Labeled và Unlabeled
        labeled = [d for d in all_data if d.get('category') is not None]
        unlabeled = [d for d in all_data if d.get('category') is None]

        # 3. Chia tập 85/15 (Kèm lọc danh mục hiếm)
        from collections import Counter
        counts = Counter([d['category'] for d in labeled])
        final_labeled = [d for d in labeled if counts[d['category']] >= 2]
        rare_data = [d for d in labeled if counts[d['category']] < 2]

        print("✂️ Đang chia tập dữ liệu...")
        train_data, val_data = train_test_split(
            final_labeled, test_size=0.15, 
            stratify=[d['category'] for d in final_labeled], random_state=42
        )
        train_data.extend(rare_data)

        # 4. Ghi file JSON Lines
        print("💾 Đang ghi file JSON (Chế độ tiết kiệm RAM)...")
        save_json_lines(train_data, "0. data/train_data.json")
        save_json_lines(val_data, "0. data/val_data.json")
        save_json_lines(unlabeled, "0. data/unlabeled_data.json")

        print("-" * 30)
        print("✅ THÀNH CÔNG RỰC RỠ!")
        print(f"📊 Train: {len(train_data)} | Val: {len(val_data)} | Unlabeled: {len(unlabeled)}")

    except Exception as e:
        import traceback
        print(f"❌ Lỗi: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()