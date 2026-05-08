#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script scrape Bao Thanh Nien - 30 trang x 7 danh muc (~4140 bai)
Luu truc tiep vao MongoDB Atlas (news_data_collection)
- Tong so bai: 4126 bai
- Phan chia: 4126/7 = ~589 bai/danh muc
- Moi trang: ~20 bai => 30 trang/danh muc
- Ghi nhan category: trang 1-7 (1/4.5 ~ 22%)
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re
import random
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

# 7 danh muc Bao Thanh Nien (AJAX)
CATEGORIES = [
    (1854, "Thời sự"),
    (18549, "Kinh tế"),
    (18565, "Sức khỏe"),
    (18526, "Giáo dục"),
    (185285, "Giải trí"),
    (185318, "Thể thao"),
    (185315, "Công nghệ"),
]

# User agents rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15',
]

def get_headers():
    """Get random headers for anti-bot"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'vi-VN,vi;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }

BASE_URL = 'https://thanhnien.vn'

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
        print("OK MongoDB Atlas connected")
        return True
    except Exception as e:
        print(f"ERROR MongoDB connection: {e}")
        return False

def extract_article_details(url, category_name, retry=2):
    """Extract 21 fields from Bao Thanh Nien article"""
    for attempt in range(retry):
        try:
            time.sleep(random.uniform(0.5, 1.5))
            
            response = requests.get(
                url,
                headers=get_headers(),
                timeout=15,
                verify=False
            )
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                if attempt < retry - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                return None
            
            break
        except (requests.Timeout, requests.ConnectionError):
            if attempt < retry - 1:
                time.sleep(random.uniform(2, 5))
                continue
            return None
        except Exception:
            if attempt < retry - 1:
                time.sleep(random.uniform(1, 3))
                continue
            return None
    
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Initialize article data - 21 FIELDS
        data = {
            'article_id': None,
            'title': None,
            'article_content': None,
            'description': None,
            'url': url,
            'site': 'thanhnien',
            'author': None,
            'publish_date': None,
            'category': category_name,
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
        id_match = re.search(r'-(\d+)\.htm', url)
        if id_match:
            data['article_id'] = id_match.group(1)
        
        # 3. Description
        desc_meta = soup.find('meta', attrs={'name': 'description'})
        if desc_meta:
            data['description'] = desc_meta.get('content')
            data['meta_description'] = desc_meta.get('content')
        
        if not data['description']:
            og_desc = soup.find('meta', attrs={'property': 'og:description'})
            if og_desc:
                data['description'] = og_desc.get('content')
                data['meta_description'] = og_desc.get('content')
        
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
        
        # 6. Publish date - Try multiple selectors
        # First try: div.detail-time (Thanh Nien structure)
        date_tag = soup.find('div', class_='detail-time')
        if date_tag:
            date_text = date_tag.get_text(strip=True)
            # Extract just the date/time part (e.g., "07/04/2026 21:27 GMT+7")
            if date_text:
                data['publish_date'] = date_text.split('\n')[0].strip()
        
        # Fallback: time tag
        if not data['publish_date']:
            date_tag = soup.find('time')
            if date_tag:
                data['publish_date'] = date_tag.get_text(strip=True)
        
        # Fallback: span with time class
        if not data['publish_date']:
            date_tag = soup.find('span', class_=re.compile(r'.*time.*', re.I))
            if date_tag:
                data['publish_date'] = date_tag.get_text(strip=True)
        
        # 7. Author - Try multiple selectors
        # First try: div.detail-author (Thanh Nien structure)
        author_tag = soup.find('div', class_='detail-author')
        if author_tag:
            author_text = author_tag.get_text(strip=True)
            # Extract just the author name (first line before dash)
            if author_text:
                # Format is "Author Name\n- email"
                author_name = author_text.split('\n')[0].strip().split('-')[0].strip()
                if author_name:
                    data['author'] = author_name
        
        # Fallback: div.detail-info
        if not data['author']:
            author_tag = soup.find('div', class_='detail-info')
            if author_tag:
                author_text = author_tag.get_text(strip=True)
                if author_text:
                    author_name = author_text.split('\n')[0].strip().split('-')[0].strip()
                    if author_name:
                        data['author'] = author_name
        
        # Fallback: span with author class
        if not data['author']:
            author_tag = soup.find('span', class_=re.compile(r'.*author.*', re.I))
            if not author_tag:
                author_tag = soup.find('a', class_=re.compile(r'.*author.*', re.I))
            if author_tag:
                data['author'] = author_tag.text.strip()
        
        # 8. Featured image
        featured = soup.find('figure')
        if featured:
            img_tag = featured.find('img')
            if img_tag:
                data['featured_image'] = img_tag.get('src') or img_tag.get('data-src')
                data['featured_image_alt'] = img_tag.get('alt')
        else:
            img_tag = soup.find('img', class_=re.compile(r'.*(featured|main).*', re.I))
            if not img_tag:
                all_imgs = soup.find_all('img', limit=5)
                for img in all_imgs:
                    src = img.get('src') or img.get('data-src')
                    if src and 'logo' not in src.lower():
                        img_tag = img
                        break
            
            if img_tag:
                data['featured_image'] = img_tag.get('src') or img_tag.get('data-src')
                data['featured_image_alt'] = img_tag.get('alt')
        
        # 9. All images
        article_body = soup.find('div', class_=re.compile(r'.*(article|detail).*content.*', re.I))
        if not article_body:
            article_body = soup.find('article')
        
        if article_body:
            all_imgs = article_body.find_all('img')
            if all_imgs:
                images_list = []
                for img in all_imgs:
                    src = img.get('src') or img.get('data-src')
                    if src and src not in images_list:
                        images_list.append(src)
                if images_list:
                    data['images'] = images_list
        
        # 10. Video URL
        video_elem = soup.find('iframe')
        if video_elem:
            data['video_url'] = video_elem.get('src')
        
        # 11. Article content
        if article_body:
            paragraphs = article_body.find_all('p')
            content_parts = [p.text.strip() for p in paragraphs if len(p.text.strip()) > 20]
            
            if content_parts:
                data['article_content'] = '\n\n'.join(content_parts)
                data['paragraph_count'] = len(content_parts)
                data['word_count'] = len(data['article_content'].split())
                data['estimated_read_time_minutes'] = max(1, round(data['word_count'] / 200))
        
        # 12. Subheadings
        if article_body:
            headings = article_body.find_all(['h2', 'h3', 'h4'])
            if headings:
                data['subheadings'] = [h.text.strip() for h in headings]
        
        return data
            
    except Exception:
        return None

def get_articles_from_page(category_id, page, retry=2):
    """Get article links from Bao Thanh Nien AJAX page"""
    url = f"{BASE_URL}/timelinelist/{category_id}/{page}.htm"
    
    for attempt in range(retry):
        try:
            time.sleep(random.uniform(0.3, 0.8))
            
            response = requests.get(
                url,
                headers=get_headers(),
                timeout=10,
                verify=False
            )
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                if attempt < retry - 1:
                    time.sleep(1)
                    continue
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Thanh Nien dung div.box-category-item
            items = soup.find_all('div', class_='box-category-item')
            articles = []
            
            for item in items:
                # Tim a.box-category-link-title
                title_link = item.find('a', class_='box-category-link-title')
                if title_link:
                    article_url = title_link.get('href')
                    
                    if article_url:
                        if not article_url.startswith('http'):
                            article_url = urljoin(BASE_URL, article_url)
                        
                        articles.append(article_url)
            
            return articles
            
        except Exception:
            if attempt < retry - 1:
                time.sleep(1)
                continue
            return []

def main():
    print("=" * 80)
    print("SCRAPING BAO THANH NIEN - 30 trang x 7 danh muc (~4140 bai)")
    print("=" * 80)
    print(f"Database: {DB_NAME}.{COLLECTION_NAME}\n")
    
    # Setup MongoDB
    if not setup_mongodb():
        print("Cannot connect to MongoDB. Stopping.")
        return
    
    # Get current count
    current_count = news_collection.count_documents({})
    print(f"Current collection: {current_count} documents\n")
    
    # Collect all article URLs - 30 pages per category
    all_article_urls = []
    total_categories = len(CATEGORIES)
    
    for cat_idx, (cat_id, cat_name) in enumerate(CATEGORIES, 1):
        print(f"[{cat_idx}/{total_categories}] Collecting: {cat_name}...", end=" ", flush=True)
        category_count = 0
        
        # 30 pages per category to get ~589 articles
        for page in range(1, 31):
            article_urls = get_articles_from_page(cat_id, page)
            
            # Only pages 1-7 get category assigned (1/4.5 ~ 22%)
            has_category = (page <= 7)
            
            for url in article_urls:
                all_article_urls.append({
                    'url': url,
                    'category_name': cat_name if has_category else None,
                    'page': page
                })
                category_count += 1
            
            if page % 10 == 0:
                print(".", end="", flush=True)
        
        print(f" {category_count} articles")
    
    print(f"\nOK Collected {len(all_article_urls)} links\n")
    print(f"Extracting {len(all_article_urls)} articles (4 workers, anti-bot)...\n")
    
    # Extract articles in parallel (4 workers to be gentle with server)
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
                except Exception:
                    failed += 1
            else:
                failed += 1
            
            if (completed + failed) % 50 == 0:
                print(f"Extract: {completed}/{len(all_article_urls)} (Failed: {failed})")
    
    # Final summary
    final_count = news_collection.count_documents({})
    new_count = final_count - current_count
    
    print("\n" + "=" * 80)
    print("COMPLETED!")
    print(f"   OK Inserted: {completed} articles")
    print(f"   FAIL: {failed} articles")
    print(f"   Total in MongoDB: {final_count} documents")
    print(f"   New added: {new_count}")
    print("=" * 80)
    
    # Close connection
    client.close()

if __name__ == '__main__':
    main()
