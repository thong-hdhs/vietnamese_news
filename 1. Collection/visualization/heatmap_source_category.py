#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Standalone Script: Source vs Category Heatmap
Biểu đồ heatmap mối quan hệ giữa Nguồn - Danh mục
"""

from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# MongoDB connection
MONGODB_URI = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_collection"

def main():
    # Connect to MongoDB
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print("✓ MongoDB connected\n")
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return
    
    try:
        # Query all documents
        documents = list(collection.find({}, {'site': 1, 'category': 1}))
        
        # Create DataFrame
        data = []
        for doc in documents:
            site = doc.get('site', 'Unknown')
            category = doc.get('category', 'Không phân loại')
            data.append({'Site': site, 'Category': category})
        
        df = pd.DataFrame(data)
        
        # Create pivot table for heatmap
        pivot_table = pd.crosstab(df['Site'], df['Category'])
        
        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (16, 10)
        
        # Create Heatmap - Tương quan nguồn vs danh mục
        print("Creating heatmap...")
        fig, ax = plt.subplots(figsize=(14, 6))
        
        sns.heatmap(pivot_table, annot=True, fmt='d', cmap='YlOrRd', 
                    cbar_kws={'label': 'Number of Articles'}, ax=ax,
                    linewidths=0.5, linecolor='gray')
        
        ax.set_title('Correlation Matrix: Source vs Category\n(Mối quan hệ giữa Nguồn - Danh mục)', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.set_xlabel('Category (Danh mục)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Source (Nguồn)', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('source_category_heatmap.png', dpi=300, bbox_inches='tight')
        print("✓ Saved: source_category_heatmap.png")
        plt.close()
        
        # Print pivot table summary
        print("\n" + "=" * 100)
        print("PIVOT TABLE: Source x Category")
        print("=" * 100)
        print(pivot_table)
        print(f"\nTotal articles: {pivot_table.sum().sum()}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Close connection
    client.close()
    print("\n" + "=" * 100)
    print("✓ COMPLETED! Heatmap saved: source_category_heatmap.png")
    print("=" * 100)

if __name__ == '__main__':
    main()
