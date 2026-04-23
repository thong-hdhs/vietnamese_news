#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script: Scatter Plot Word Count Outliers
Vẽ biểu đồ scatter dễ hiểu: bài bình thường vs outliers
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient

# ===== MONGODB CONNECTION =====
MONGODB_URI = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_preprocessing"

def connect_mongodb():
    """Kết nối MongoDB Atlas"""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        print("✅ MongoDB Atlas kết nối thành công")
        return client
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return None

def get_data():
    """Lấy dữ liệu từ MongoDB và đếm từ dựa trên độ dài mảng full_text_tokens"""
    client = connect_mongodb()
    if not client:
        return None
    
    try:
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Query tất cả documents
        documents = list(collection.find(
            {'full_text_tokens': {'$exists': True, '$ne': []}},
            {'full_text_tokens': 1}
        ))
        
        print(f"✅ Lấy {len(documents)} documents từ MongoDB")
        
        # Đếm số từ dựa trên độ dài mảng full_text_tokens
        data_with_counts = []
        for doc in documents:
            tokens = doc.get('full_text_tokens', [])
            # Độ dài mảng = số từ
            if isinstance(tokens, list):
                word_count = len(tokens)
            else:
                word_count = 0
                
            data_with_counts.append({
                'word_count': word_count,
                '_id': doc.get('_id')
            })
        
        print(f"✅ Đã lấy độ dài từ mảng full_text_tokens cho {len(data_with_counts)} documents\n")
        
        client.close()
        return data_with_counts
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        client.close()
        return None

def create_scatter_chart(data_with_counts):
    """Tạo scatter plot outliers"""
    
    # Tạo dataframe từ word counts
    df = pd.DataFrame(data_with_counts)
    
    # Tính IQR
    Q1 = df['word_count'].quantile(0.25)
    Q3 = df['word_count'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = 0
    upper_bound = Q3 + 1.5 * IQR
    
    # Xác định outliers
    is_outlier = (df['word_count'] < lower_bound) | (df['word_count'] > upper_bound)
    
    # In thống kê
    print("=" * 70)
    print("SCATTER PLOT - Word Count Outliers")
    print("=" * 70)
    print(f"Tổng bài: {len(df)}")
    print(f"Bình thường: {(~is_outlier).sum()}")
    print(f"Outliers: {is_outlier.sum()}")
    print(f"Q1: {Q1:.0f}, Q3: {Q3:.0f}, IQR: {IQR:.0f}")
    print(f"Bounds: [{lower_bound:.0f}, {upper_bound:.0f}]")
    print(f"Word count stats:")
    print(f"  Min: {df['word_count'].min()}")
    print(f"  Max: {df['word_count'].max()}")
    print(f"  Mean: {df['word_count'].mean():.1f}")
    print(f"  Median: {df['word_count'].median():.1f}")
    print("=" * 70 + "\n")
    
    # Tạo figure
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Scatter plot - Normal (xanh)
    normal_idx = np.where(~is_outlier)[0]
    ax.scatter(normal_idx, df[~is_outlier]['word_count'], 
              color='#2ecc71', alpha=0.5, s=20, label='Normal')
    
    # Scatter plot - Outliers (đỏ, tam giác)
    outlier_idx = np.where(is_outlier)[0]
    ax.scatter(outlier_idx, df[is_outlier]['word_count'], 
              color='#e74c3c', alpha=0.8, s=80, marker='^', label=f'Outliers ({is_outlier.sum()})')
    
    # Add bounds
    ax.axhline(y=lower_bound, color='orange', linestyle='--', linewidth=2, alpha=0.7, label=f'Lower: {lower_bound:.0f}')
    ax.axhline(y=upper_bound, color='red', linestyle='--', linewidth=2, alpha=0.7, label=f'Upper: {upper_bound:.0f}')
    
    # Fill zone bình thường
    ax.fill_between(range(len(df)), lower_bound, upper_bound, alpha=0.1, color='green', label='Normal Zone')
    
    # Định dạng
    ax.set_xlabel('Bài báo (Index)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Word Count', fontsize=12, fontweight='bold')
    ax.set_title('Scatter Plot - Từng bài báo', fontsize=14, fontweight='bold', pad=15)
    
    ax.grid(alpha=0.3, linestyle='--')
    ax.legend(loc='upper right', fontsize=11, framealpha=0.95)
    
    plt.tight_layout()
    
    # Lưu file
    output_file = 'scatter_plot_outliers.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✅ Biểu đồ đã lưu: {output_file}")
    
    plt.show()

def main():
    print("\n" + "=" * 70)
    print("SCATTER PLOT - Word Count Analysis (độ dài mảng full_text_tokens)")
    print("=" * 70 + "\n")
    
    data_with_counts = get_data()
    
    if data_with_counts is None or len(data_with_counts) == 0:
        print("❌ Không có dữ liệu")
        return
    
    create_scatter_chart(data_with_counts)

if __name__ == '__main__':
    main()
