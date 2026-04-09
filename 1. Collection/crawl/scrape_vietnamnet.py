#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script cào báo VietnamNet - 17 trang × 7 thể loại (2,387 bài)
Lưu trực tiếp vào MongoDB Atlas (news_data_collection)
Ghi tiếp tục vào collection sau Tuổi Trẻ và VnExpress
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

# 7 Chuyên mục VietnamNet
CATEGORIES = [
    ("https://vietnamnet.vn/chinh-tri", "Thời sự"),
    ("https://vietnamnet.vn/kinh-doanh", "Kinh doanh"),
    ("https://vietnamnet.vn/giao-duc", "Giáo dục"),
    ("https://vietnamnet.vn/the-thao", "Thể thao"),
    ("https://vietnamnet.vn/suc-khoe", "Sức khỏe"),
    ("https://vietnamnet.vn/cong-nghe", "Công nghệ"),
    ("https://vietnamnet.vn/van-hoa-giai-tri", "Văn hóa giải trí")
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
    """Extract 21 fields từ một bài báo VietnamNet"""
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
        
        # Initialize article data - 21 FIELDS
        data = {
            'article_id': None,
            'title': None,
            'article_content': None,
            'description': None,
            'url': url,
            'site': 'vietnamnet',
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
        
        # 1. Title - từ h1 (VietnamNet dùng h1 cho title)
        title_tag = soup.find('h1')
        if title_tag:
            data['title'] = title_tag.text.strip()
        
        # 2. Article ID từ URL (VietnamNet format: {slug}-{id}.html)
        id_match = re.search(r'-(\d+)\.html', url)
        if id_match:
            data['article_id'] = id_match.group(1)
        
        # 3. Description từ meta
        desc_meta = soup.find('meta', attrs={'name': 'description'})
        if desc_meta:
            data['description'] = desc_meta.get('content')
            data['meta_description'] = desc_meta.get('content')
        
        # Nếu không, thử lấy từ og:description
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
        
        # 6. Publish date - VietnamNet không rõ selector, thử nhiều cách
        # Tìm time tag hoặc span với class chứa "time", "date", "publish"
        date_tag = soup.find('time')
        if date_tag:
            data['publish_date'] = date_tag.get_text(strip=True)
        else:
            # Thử tìm span hoặc div với class chứa "time"
            date_tag = soup.find('span', class_=re.compile(r'.*time.*', re.I))
            if date_tag:
                data['publish_date'] = date_tag.get_text(strip=True)
            else:
                # Thử tìm div class chứa "date"
                date_tag = soup.find('div', class_=re.compile(r'.*date.*', re.I))
                if date_tag:
                    data['publish_date'] = date_tag.get_text(strip=True)
        
        # 7. Author - VietnamNet sử dụng div.article-detail-author__main hoặc div.article-author-multiple__slide
        # Try single author first
        author_tag = soup.find('div', class_='article-detail-author__main')
        if author_tag:
            data['author'] = author_tag.get_text(strip=True)
        else:
            # Try multiple authors
            author_tag = soup.find('div', class_=re.compile(r'article-author-multiple__slide'))
            if author_tag:
                # Get all author names from multiple authors section
                author_slides = soup.find_all('div', class_=re.compile(r'article-author-multiple__slide'))
                authors = []
                for slide in author_slides:
                    name_elem = slide.find(class_='name')
                    if name_elem:
                        authors.append(name_elem.get_text(strip=True))
                if authors:
                    data['author'] = '|'.join(authors)  # Join multiple authors with "|"
            else:
                # Fallback to span.name within article section
                name_tag = soup.find('span', class_='name')
                if name_tag:
                    data['author'] = name_tag.get_text(strip=True)
        
        # 8. Featured image
        # Thử tìm figure > img hoặc div.featured-image > img
        featured = soup.find('figure')
        if featured:
            img_tag = featured.find('img')
            if img_tag:
                data['featured_image'] = img_tag.get('src') or img_tag.get('data-src')
                data['featured_image_alt'] = img_tag.get('alt')
        else:
            # Thử tìm img với class "featured" hoặc "main-image"
            img_tag = soup.find('img', class_=re.compile(r'.*(featured|main).*', re.I))
            if not img_tag:
                # Lấy image đầu tiên trong article
                all_imgs = soup.find_all('img', limit=5)
                for img in all_imgs:
                    src = img.get('src') or img.get('data-src')
                    if src and 'logo' not in src.lower() and 'icon' not in src.lower():
                        img_tag = img
                        break
            
            if img_tag:
                data['featured_image'] = img_tag.get('src') or img_tag.get('data-src')
                data['featured_image_alt'] = img_tag.get('alt')
        
        # 9. All images - tìm trong article body
        # VietnamNet dùng div.article-content hoặc div.detail-content
        article_body = soup.find('div', class_=re.compile(r'.*(article|detail).*content.*', re.I))
        
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
        
        # 10. Video URL - từ iframe
        video_elem = soup.find('iframe')
        if video_elem:
            data['video_url'] = video_elem.get('src')
        
        # 11. Article content - từ article body
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
            
    except requests.Timeout:
        print(f"    ⏱️  Timeout: {url}")
        return None
    except Exception as e:
        print(f"    ❌ Lỗi: {str(e)[:50]}")
        return None

def get_articles_from_page(url, retry=2):
    """Lấy danh sách link bài báo từ một trang VietnamNet"""
    articles = []
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
                return articles
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # VietnamNet dùng div.horizontalPost container (25 bài/trang)
            post_containers = soup.find_all('div', class_='horizontalPost')
            
            for container in post_containers:
                # Tìm h3 > a
                h3 = container.find('h3')
                if h3:
                    link_tag = h3.find('a')
                    if link_tag and link_tag.get('href'):
                        article_url = link_tag['href']
                        
                        # Convert relative URL to absolute
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

def get_paginated_url(category_url, page):
    """Tạo URL phân trang cho VietnamNet"""
    if page == 0:
        return category_url
    else:
        # Format: https://vietnamnet.vn/{category}-page{page}
        return f"{category_url}-page{page}"

def main():
    print("=" * 80)
    print("🚀 SCRAPING VietnamNet - 17 trang × 7 chuyên mục (2,387 bài)")
    print("=" * 80)
    print(f"📊 Database: {DB_NAME}.{COLLECTION_NAME}\n")
    
    # Setup MongoDB
    if not setup_mongodb():
        print("❌ Không thể kết nối MongoDB. Dừng.")
        return
    
    # Get current count before adding
    current_count = news_collection.count_documents({})
    print(f"📊 Collection hiện tại: {current_count} documents\n")
    
    # Collect all article URLs first - EXACTLY 17 pages per category
    all_article_urls = []
    for category_url, category_name in CATEGORIES:
        print(f"📰 Collecting links: {category_name}...", end=" ", flush=True)
        category_count = 0
        
        # EXACTLY 17 pages per category (pages 0-16)
        for page in range(0, 17):  # Pages 0-16
            page_url = get_paginated_url(category_url, page)
            article_urls = get_articles_from_page(page_url)
            
            # Determine if this page should have category assigned
            has_category = (page <= 3)  # Only pages 0-3 get category
            
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
    
    # Get final counts
    final_count = news_collection.count_documents({})
    new_count = final_count - current_count
    print(f"   📊 Total in MongoDB: {final_count} documents")
    print(f"   📊 New documents added: {new_count}")
    print("=" * 80)
    
    # Close connection
    client.close()

if __name__ == '__main__':
    main()
