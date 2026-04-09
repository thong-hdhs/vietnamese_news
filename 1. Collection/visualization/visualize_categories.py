#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script vẽ biểu đồ cột số lượng bài báo theo danh mục
Nhóm 8 danh mục chính
"""

import matplotlib.pyplot as plt
from pymongo import MongoClient

# MongoDB connection
MONGODB_URI = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_collection"

# Định nghĩa 8 danh mục chính (nhóm từ các danh mục con)
CATEGORY_GROUPS = {
    'Thể thao': ['Thể thao'],
    'Sức khỏe': ['Sức khỏe'],
    'Giáo dục': ['Giáo dục'],
    'Thời sự': ['Thời sự'],
    'Giải trí': ['Giải trí', 'Văn hóa giải trí'],
    'Công nghệ': ['Công nghệ', 'Khoa học - Công nghệ', 'Khoa học & CN'],
    'Kinh tế': ['Kinh doanh', 'Kinh tế'],
}

def get_articles_by_category_group():
    """Truy vấn MongoDB và lấy số lượng bài báo theo nhóm danh mục"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        print("✅ Kết nối MongoDB thành công\n")
        
        # Tính số lượng cho mỗi nhóm
        category_stats = {}
        total_articles = collection.count_documents({})
        
        print(f"{'Danh mục':<20} {'Số lượng':<10} {'Phần trăm':<10}")
        print("-" * 40)
        
        for group_name, subcategories in CATEGORY_GROUPS.items():
            # Đếm bài báo trong mỗi subcategory
            count = 0
            for subcat in subcategories:
                count += collection.count_documents({'category': subcat})
            
            category_stats[group_name] = count
            percentage = (count / total_articles) * 100
            print(f"{group_name:<20} {count:<10} {percentage:>6.2f}%")
        
        # Đếm bài báo không có danh mục
        no_category_count = collection.count_documents({'category': None})
        category_stats['Không xác định'] = no_category_count
        percentage = (no_category_count / total_articles) * 100
        print(f"{'Không xác định':<20} {no_category_count:<10} {percentage:>6.2f}%")
        
        print("-" * 40)
        total_counted = sum(category_stats.values())
        print(f"{'TỔNG CỘNG':<20} {total_counted:<10}")
        
        client.close()
        return category_stats
        
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return None

def visualize_categories(category_stats):
    """Vẽ biểu đồ cột số lượng bài báo theo danh mục"""
    
    if not category_stats:
        return
    
    # Sắp xếp dữ liệu theo tên danh mục, bỏ "Không xác định"
    categories = [k for k in category_stats.keys() if k != 'Không xác định']
    counts = [category_stats[cat] for cat in categories]
    
    # Màu sắc cho biểu đồ - Màu đậm, sáng, đẹp
    colors = ["#F11800", "#0804F4", "#59FF00", "#FD9400", "#8E00C6", "#00FFCC", "#F44B39", "#07539F"]
    
    # Tạo biểu đồ cột
    fig, ax = plt.subplots(figsize=(14, 12))
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#ffffff')
    
    bars = ax.bar(categories, counts, color=colors[:len(categories)], edgecolor='#2C3E50', 
                   linewidth=2.5, alpha=0.9, width=0.5)
    
    # Thêm giá trị trên mỗi cột
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width()/2.,
            height + max(counts)*0.01,
            f'{int(count)}',
            ha='center',
            va='bottom',
            fontsize=12,
            fontweight='bold',
            color='#2C3E50'
        )
    
    # Tính tổng (không hiển thị phần trăm)
    total = sum(counts)
    
    # Giảm chiều cao cột bằng cách mở rộng trục Y
    ax.set_ylim(0, 800)
    
    # Tùy chỉnh biểu đồ
    ax.set_xlabel('Danh mục', fontsize=14, fontweight='bold', color='#2C3E50')
    ax.set_ylabel('Số lượng bài báo', fontsize=14, fontweight='bold', color='#2C3E50')
    
    
    ax.grid(axis='y', alpha=0.25, linestyle='--', linewidth=0.8, color='#BDC3C7')
    ax.set_axisbelow(True)
    
    # Cải thiện spine
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#BDC3C7')
    ax.spines['bottom'].set_color('#BDC3C7')
    
    # Xoay nhãn x để dễ đọc
    plt.xticks(rotation=35, ha='right', fontsize=12, fontweight='bold', color='#2C3E50')
    plt.yticks(fontsize=11, color='#2C3E50')
    
    # Thêm thông tin tổng cộng
    total_text = f'{total}/16968 bài báo'
    ax.text(0.98, 0.88, total_text, transform=ax.transAxes,
            fontsize=12, fontweight='bold', color='#FFF',
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round,pad=0.8', facecolor='#2C3E50', alpha=0.85, 
                     edgecolor='#3498DB', linewidth=2))
    
    # Tạo khoảng trắng lớn ở phía trên với cột nhỏ hơn
    plt.subplots_adjust(top=0.65, bottom=0.15, left=0.1, right=0.95)
    
    # Lưu biểu đồ
    output_file = 'articles_by_category_chart.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n💾 Biểu đồ đã được lưu: {output_file}")
    
    plt.close()

def main():
    print("=" * 50)
    print("📊 BIỂU ĐỒ BÀI BÁO THEO DANH MỤC")
    print("=" * 50)
    print()
    
    # Lấy dữ liệu từ MongoDB
    category_stats = get_articles_by_category_group()
    
    if category_stats:
        # Vẽ biểu đồ
        print()
        visualize_categories(category_stats)
        print("\n✅ Hoàn tất!")
    else:
        print("\n❌ Không thể lấy dữ liệu từ MongoDB")

if __name__ == '__main__':
    main()
