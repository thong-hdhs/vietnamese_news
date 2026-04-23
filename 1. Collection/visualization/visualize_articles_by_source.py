#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script truy vấn MongoDB và visualize:
1. Biểu đồ tròn: % số lượng bài báo theo nguồn
2. Biểu đồ cột: Số lượng bài báo theo nguồn
"""

import matplotlib.pyplot as plt
from pymongo import MongoClient
from collections import Counter

# MongoDB connection
MONGODB_URI = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_collection"

def get_articles_by_source():
    """Truy vấn MongoDB và lấy số lượng bài báo theo nguồn"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        print("✅ Kết nối MongoDB thành công")
        
        # Lấy pipelines aggregation để group by site
        pipeline = [
            {
                "$group": {
                    "_id": "$site",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Convert to dict for easier handling
        sources_data = {item['_id']: item['count'] for item in results}
        
        # Get total count
        total_count = collection.count_documents({})
        
        print(f"\n📊 Thống kê bài báo theo nguồn:")
        print(f"   {'Nguồn':<15} {'Số lượng':<10} {'Phần trăm':<10}")
        print("   " + "-" * 35)
        
        for source, count in sorted(sources_data.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_count) * 100
            print(f"   {source:<15} {count:<10} {percentage:>6.2f}%")
        
        print("   " + "-" * 35)
        print(f"   {'TOTAL':<15} {total_count:<10} {'100.00%':>6}")
        
        client.close()
        return sources_data
        
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return None

def visualize_data(sources_data):
    """Tạo 2 biểu đồ riêng: Pie chart và Bar chart"""
    
    if not sources_data:
        return
    
    # Sắp xếp dữ liệu
    sources = list(sources_data.keys())
    counts = list(sources_data.values())
    
    # Màu sắc cho biểu đồ
    colors = ["#94BE3C", "#02C79D" , "#00889F", "#016A95", "#292F56"]
    
    # ============ PIE CHART - FIGURE 1 ============
    fig1, ax1 = plt.subplots(figsize=(10, 8))
    
    total = sum(counts)
    percentages = [(c / total) * 100 for c in counts]
    
    # Create pie chart with percentage labels
    wedges, texts, autotexts = ax1.pie(
        counts,
        labels=sources,
        colors=colors,
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 11, 'weight': 'bold'}
    )
    
    # Customize percentage text
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(12)
        autotext.set_weight('bold')
    
    ax1.set_title('Tỷ lệ % bài báo theo nguồn', fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save pie chart
    pie_file = 'articles_pie_chart.png'
    plt.savefig(pie_file, dpi=300, bbox_inches='tight')
    print(f"\n💾 Biểu đồ tròn đã được lưu: {pie_file}")
    plt.close(fig1)
    
    # ============ BAR CHART - FIGURE 2 ============
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    
    bars = ax2.bar(sources, counts, color=colors, edgecolor='navy', linewidth=1.5, alpha=0.8)
    
    # Add value labels on top of bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width()/2.,
            height,
            f'{int(count)}',
            ha='center',
            va='bottom',
            fontsize=11,
            fontweight='bold'
        )
    
    ax2.set_xlabel('Nguồn tin', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Số lượng bài báo', fontsize=12, fontweight='bold')
    ax2.set_title('Số lượng bài báo theo nguồn', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    ax2.set_axisbelow(True)
    
    # Rotate x labels for better readability
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save bar chart
    bar_file = 'articles_bar_chart.png'
    plt.savefig(bar_file, dpi=300, bbox_inches='tight')
    print(f"💾 Biểu đồ cột đã được lưu: {bar_file}")
    plt.close(fig2)

def main():
    print("=" * 50)
    print("📊 VISUALIZE BÀI BÁO THEO NGUỒN")
    print("=" * 50)
    
    # Get data from MongoDB
    sources_data = get_articles_by_source()
    
    if sources_data:
        # Create visualizations
        visualize_data(sources_data)
        print("\n✅ Hoàn tất!")
    else:
        print("\n❌ Không thể lấy dữ liệu từ MongoDB")

if __name__ == '__main__':
    main()
