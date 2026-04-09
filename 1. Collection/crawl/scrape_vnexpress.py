#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script cào 1000 bài báo VnExpress từ 7 thể loại
Lưu trực tiếp vào MongoDB Atlas (news_data_collection)
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pymongo import MongoClient

# MongoDB Atlas connection
MONGODB_URI = "mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority"
DB_NAME = "vietnamese_news"
COLLECTION_NAME = "news_data_collection"

# 7 Chuyên mục VnExpress
CATEGORIES = [
    ("https://vnexpress.net/thoi-su", "Thời sự"),
    ("https://vnexpress.net/kinh-doanh", "Kinh doanh"),
    ("https://vnexpress.net/khoa-hoc-cong-nghe", "Khoa học & CN"),
    ("https://vnexpress.net/suc-khoe", "Sức khỏe"),
    ("https://vnexpress.net/giai-tri", "Giải trí"),
    ("https://vnexpress.net/the-thao", "Thể thao"),
    ("https://vnexpress.net/giao-duc", "Giáo dục")
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# MongoDB setup - Global
client = None
db = None
news_collection = None

def setup_mongodb():
    global client, db, news_collection
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[DB_NAME]
        news_collection = db[COLLECTION_NAME]
        print("✅ MongoDB Atlas kết nối thành công")
        return True
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB Atlas: {e}")
        return False

def extract_article_details(url, category_name, retry=2):
    """Extract 21 fields từ một bài báo VnExpress"""
    for attempt in range(retry):
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=10,
                verify=False
            )
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                if attempt < retry - 1:
                    time.sleep(1)
                    continue
                return None
            
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < retry - 1:
                time.sleep(1)
                continue
            else:
                return None
        except Exception as e:
            if attempt < retry - 1:
                time.sleep(1)
                continue
            return None
    
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Initialize article data - NEW FIELD ORDER
        data = {
            'article_id': None,
            'title': None,
            'article_content': None,
            'description': None,
            'url': url,
            'site': 'vnexpress',
            'author': None,
            'publish_date': None,
            'category': category_name,  # None or category name
            'category_url': None,
            'tags': None,
            'featured_image': None,
            'featured_image_alt': None,
            'images': None,
            'video_url': None,
            'meta_description': None,
            'meta_keywords': None,
            'og_tags': {},
            'paragraph_count': 0,
            'word_count': 0,
            'estimated_read_time_minutes': 0,
            'subheadings': None
        }
        
        # 1. Title
        title_tag = soup.find('h1')
        if title_tag:
            data['title'] = title_tag.text.strip()
        
        # 2. Article ID from URL
        id_match = re.search(r'-(\d+)\.html', url)
        if id_match:
            data['article_id'] = id_match.group(1)
        
        # 3. Description (from meta)
        desc_meta = soup.find('meta', attrs={'name': 'description'})
        if desc_meta:
            data['description'] = desc_meta.get('content')
            data['meta_description'] = desc_meta.get('content')
        
        # 4. Meta keywords
        keywords_meta = soup.find('meta', attrs={'name': 'keywords'})
        if keywords_meta:
            data['meta_keywords'] = keywords_meta.get('content')
        
        # 5. OG Tags
        og_props = ['title', 'description', 'image', 'url', 'type']
        for prop in og_props:
            og_meta = soup.find('meta', attrs={'property': f'og:{prop}'})
            if og_meta:
                data['og_tags'][prop] = og_meta.get('content')
        
        # 6. Publish date - từ <span class="date">
        date_tag = soup.find('span', class_='date')
        if date_tag:
            data['publish_date'] = date_tag.text.strip()
        
        # 7. Author - từ <em> hoặc <strong>
        author_tag = soup.find('em')
        if not author_tag:
            author_tag = soup.find('strong')
        if author_tag:
            data['author'] = author_tag.text.strip()
        
        # 8. Featured image
        img_tag = soup.find('img', class_='lazy')
        if img_tag:
            data['featured_image'] = img_tag.get('src') or img_tag.get('data-src')
            data['featured_image_alt'] = img_tag.get('alt')
        
        # 9. All images
        all_imgs = soup.find_all('img', class_='lazy')
        if all_imgs:
            data['images'] = [img.get('src') or img.get('data-src') for img in all_imgs]
        
        # 10. Video URL
        video_elem = soup.find('iframe')
        if video_elem:
            data['video_url'] = video_elem.get('src')
        
        # 11. Article content - FULL TEXT
        article_elem = soup.find('article')
        if article_elem:
            paragraphs = article_elem.find_all('p')
            content_parts = [p.text.strip() for p in paragraphs if len(p.text.strip()) > 20]
            
            if content_parts:
                data['article_content'] = '\n\n'.join(content_parts)
                data['paragraph_count'] = len(content_parts)
                data['word_count'] = len(data['article_content'].split())
                data['estimated_read_time_minutes'] = max(1, round(data['word_count'] / 200))
        
        # 12. Subheadings
        if article_elem:
            headings = article_elem.find_all(['h2', 'h3', 'h4'])
            if headings:
                data['subheadings'] = [h.text.strip() for h in headings]
        
        return data
            
    except requests.Timeout:
        print(f"    ⏱️  Timeout: {url}")
        return None
    except Exception as e:
        print(f"    ❌ Lỗi: {str(e)[:50]}")
        return None

def get_articles_from_page(url, retry=2):
    """Lấy danh sách link bài báo từ một trang"""
    articles = []
    for attempt in range(retry):
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=10,  # Giảm từ 20 -> 10
                verify=False
            )
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                if attempt < retry - 1:
                    time.sleep(1)
                    continue
                return articles
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # VnExpress dùng h2 và h3 với class='title-news'
            article_elements = soup.find_all(['h2', 'h3'], class_='title-news')
            
            for element in article_elements:
                link_tag = element.find('a')
                if link_tag and link_tag.get('href'):
                    article_url = link_tag['href']
                    if not article_url.startswith('http'):
                        article_url = urljoin(url, article_url)
                    articles.append(article_url)
            
            return articles
            
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < retry - 1:
                time.sleep(1)
                continue
            return articles
        except Exception as e:
            return articles

def main():
    print("=" * 80)
    print("🚀 SCRAPING VnExpress - 16 trang × 7 chuyên mục (FAST MODE)")
    print("=" * 80)
    print(f"📊 Database: {DB_NAME}.{COLLECTION_NAME}\n")
    
    # Setup MongoDB
    if not setup_mongodb():
        print("❌ Không thể kết nối MongoDB. Dừng.")
        return
    
    # Collect all article URLs first - EXACTLY 16 pages per category
    all_article_urls = []
    for category_url, category_name in CATEGORIES:
        print(f"📰 Collecting links: {category_name}...", end=" ", flush=True)
        category_count = 0
        
        # EXACTLY 16 pages per category
        for page in range(1, 17):  # Pages 1-16
            if page == 1:
                page_url = category_url
            else:
                page_url = f"{category_url}-p{page}"
            
            article_urls = get_articles_from_page(page_url)
            
            # Determine if this page should have category assigned
            has_category = (page <= 3)  # Only pages 1-3 get category
            
            for url in article_urls:
                all_article_urls.append({
                    'url': url,
                    'category_name': category_name if has_category else None,
                    'page': page
                })
                category_count += 1
            
            time.sleep(0.2)  # Delay between pages
        
        print(f"collected {category_count}")
    
    print(f"\n✅ Collected {len(all_article_urls)} links\n")
    print(f"📊 Đang trích xuất {len(all_article_urls)} bài báo (8 workers)...\n")
    
    # Extract articles in parallel (8 workers) - save directly to MongoDB
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(extract_article_details, item['url'], item['category_name']): item
            for item in all_article_urls
        }
        
        completed = 0
        failed = 0
        
        for future in as_completed(futures):
            article_data = future.result()
            
            if article_data:
                try:
                    news_collection.insert_one(article_data)
                    completed += 1
                except Exception as e:
                    print(f"  ⚠️  Insert error: {e}")
                    failed += 1
            else:
                failed += 1
            
            if (completed + failed) % 50 == 0:
                print(f"💾 Extracted: {completed}/{len(all_article_urls)} (Failed: {failed})")
    
    # Final summary
    print("\n" + "=" * 80)
    print(f"✅ HOÀN TẤT!")
    print(f"   ✓ Inserted: {completed} bài báo")
    print(f"   ⚠️  Failed: {failed} bài báo")
    
    # Get final count from MongoDB
    total_count = news_collection.count_documents({})
    print(f"   📊 Total in MongoDB: {total_count} documents")
    print("=" * 80)
    
    # Close connection
    client.close()

if __name__ == '__main__':
    main()
