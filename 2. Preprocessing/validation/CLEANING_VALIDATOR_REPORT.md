# 📋 BÁOCÁO SCRIPT: Cleaning Validator

## 1. 📌 Tổng Quan

**Tên Script:** `cleaning_validator.py`  
**Loại:** Data Validation & Quality Assurance  
**Giai Đoạn:** Validation Phase (Kiểm thử sau Cleaning Phase)  
**Mục Đích:** Xác thực chất lượng dữ liệu sau các bước cleaning (xử lý NULL, xóa HTML entities, chuẩn hóa Unicode, làm sạch whitespace)

---

## 2. 🎯 Chức Năng Chính

Script thực hiện **5 loại kiểm tra** trên toàn bộ documents trong MongoDB collection:

| Kiểm Tra | Mô Tả | Mục Tiêu |
|---------|-------|---------|
| **NULL/Empty Values** | Kiểm tra NULL values và empty strings trong text fields | Đảm bảo không có missing data trong critical fields |
| **HTML Entities** | Phát hiện HTML entities chưa được decode (e.g., `&nbsp;`, `&lt;`) | Xác minh decode_html_entities() hoạt động đúng |
| **Whitespace Issues** | Phát hiện khoảng trắng dư thừa (2+ liên tiếp) | Xác minh clean_whitespace() hoạt động đúng |
| **Control Characters** | Phát hiện ký tự điều khiển (Unicode category 'C') | Xác minh clean_special_chars() hoạt động đúng |
| **Word Count Stats** | Thống kê số từ cho critical fields | Đánh giá tính hợp lý của dữ liệu text |

---

## 3. 🔍 Chi Tiết Các Hàm

### 3.1 `connect_mongodb(connection_string)`
```python
def connect_mongodb(connection_string):
    """Connect to MongoDB with 5 second timeout"""
```
- **Chức năng:** Kết nối tới MongoDB Atlas  
- **Xử lý lỗi:** Timeout 5 giây, log lỗi chi tiết  
- **Output:** MongoClient object hoặc raise exception

---

### 3.2 `count_words(text)`
```python
def count_words(text):
    """Count words by splitting on whitespace"""
```
- **Input:** Text string  
- **Logic:** `len(text.strip().split())`  
- **Output:** Integer (số từ)  
- **Safety:** Kiểm tra type và không NULL

---

### 3.3 `check_html_entities(text)`
```python
def check_html_entities(text):
    """Check if text contains HTML entities using regex"""
```
- **Pattern:** `r'&[a-zA-Z0-9]+;'`  
- **Ví dụ phát hiện:** `&nbsp;`, `&lt;`, `&amp;`, `&mdash;`  
- **Output:** Boolean (True = found issue)

---

### 3.4 `check_excessive_whitespace(text)`
```python
def check_excessive_whitespace(text):
    """Check for 2+ consecutive spaces"""
```
- **Pattern:** `r'  +'` (2 hoặc nhiều hơn)  
- **Output:** Boolean (True = found issue)

---

### 3.5 `check_control_chars(text)`
```python
def check_control_chars(text):
    """Check for Unicode control characters (category C)"""
```
- **Logic:** 
  ```python
  for char in text:
      if unicodedata.category(char)[0] == 'C':  # Category C = Control
          return True
  ```
- **Unicode Categories:** Cc (control), Cf (format), Cs (surrogate), Co (private use), Cn (not assigned)

---

### 3.6 `validate_cleaning(db, collection_name)` ⭐ MAIN FUNCTION

**Quy Trình:**

1. **Initialize Stats Dict**
   - Định nghĩa critical fields: `['title', 'article_content', 'site']`
   - Khởi tạo counters cho tất cả checks

2. **Fetch Toàn Bộ Documents**
   - `collection.find()` → Load tất cả documents vào memory
   - Đảm bảo tính toàn vẹn dữ liệu

3. **Auto-Detect Text Fields**
   - Quét tất cả documents
   - Nhận diện fields có type string hoặc None
   - Exclude: `_id`, `publish_date`, `updated_at`, `created_at`

4. **Validate Từng Document**
   ```
   For each document:
     - Check each text field:
       * Count NULLs
       * Count empty strings
       * Check HTML entities
       * Check excessive whitespace
       * Check control characters
     - For critical fields: Calculate word counts (min, max, avg, sum)
     - Check duplicates by (title + site)
   ```

5. **Calculate Aggregates**
   - Average word count cho critical fields
   - Total issues count
   - Critical issues count

**Output:** Stats dictionary với:
```python
stats = {
    'total_documents': int,
    'critical_fields': list,
    'text_fields': list,
    'null_checks': {field: count},
    'empty_checks': {field: count},
    'whitespace_issues': {field: count},
    'html_entity_issues': {field: count},
    'control_char_issues': {field: count},
    'word_count_stats': {field: {min, max, avg, sum, count}},
    'duplicate_count': int,
    'issues_found': int,
    'critical_issues': int,
    'warnings': list
}
```

---

### 3.7 `print_detailed_report(stats)`

**Output Format:** Chi tiết logs với các sections:

```
[OVERALL STATISTICS]
   - Total documents
   - Total issues found
   - Critical issues

[NULL VALUE CHECKS]
   - Per-field NULL count (nên all = 0)

[EMPTY STRING CHECKS]
   - Per-field empty count (nên all = 0)

[WHITESPACE ISSUES]
   - Per-field count + percentage
   - Nếu > 0: WARNING

[HTML ENTITY ISSUES]
   - Per-field count (nên all = 0)
   - Nếu > 0: ERROR - Not decoded properly

[CONTROL CHARACTER ISSUES]
   - Per-field count (nên all = 0)
   - Nếu > 0: ERROR - Contains invalid chars

[WORD COUNT STATISTICS]
   - Min, Max, Avg word counts
   - Per critical field
   - WARNING nếu min < 5

[DUPLICATE CHECK]
   - Total duplicates found
   - Note: Only checks, doesn't remove

[CRITICAL WARNINGS]
   - Tất cả critical issues
```

---

## 4. 🔧 Cấu Hình & Kết Nối

### MongoDB Connection
```python
connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@...'
database_name = 'vietnamese_news'
collection_name = 'news_data_preprocessing'
```

### Logging Setup
- **Log File:** `../logs/validation/cleaning_validation.log`
- **Console Output:** INFO level
- **Format:** `timestamp - level - message`

---

## 5. 🚀 Workflow Thực Thi

```
START
  ↓
Connect to MongoDB
  ↓
Fetch all documents from collection
  ↓
Auto-detect text fields
  ↓
Loop each document:
  - Check NULLs, empty strings, HTML entities
  - Check whitespace & control chars
  - Count words
  - Check duplicates
  ↓
Calculate statistics & aggregates
  ↓
Print detailed report
  ↓
Close connection
  ↓
Final status:
  - If critical_issues = 0: "[OK] VALIDATION PASSED"
  - Else: "[WARNING] Found X critical issues"
  ↓
END
```

---

## 6. 📊 Dữ Liệu Kiểm Tra

### Critical Fields (bắt buộc)
- `title` - Tiêu đề bài báo
- `article_content` - Nội dung bài viết
- `site` - Tên nguồn/trang web

### Detected Fields (tự động)
- Tất cả fields có type string (except technical fields)
- Ví dụ: `author`, `description`, `url`, v.v.

### Thresholds / Expected Values
| Check | Expected | Warning |
|-------|----------|---------|
| NULL count | 0 | > 0 |
| Empty string count | 0 | > 0 |
| HTML entity count | 0 | > 0 |
| Control char count | 0 | > 0 |
| Whitespace issues | 0-few | > 5% docs |
| Word count (min) | >= 5 | < 5 |
| Duplicates | 0 | > 0 (info only) |

---

## 7. ✅ Expected Output

### ✔️ Passing Validation
```
[OK] VALIDATION PASSED - Data is clean and ready!

All checks show:
  - NULL checks: 0 for all fields
  - Empty checks: 0 for all fields
  - HTML entities: 0 for all fields
  - Control chars: 0 for all fields
  - Whitespace issues: Minimal or 0
  - Word counts: Reasonable ranges (min >= 5)
  - Duplicates: 0 or documented
```

### ⚠️ Failed Validation
```
[WARNING] Found X critical issues:
  - CRITICAL: title has Y NULL values
  - CRITICAL: article_content has Z empty strings
  - Control characters detected in author field
  - HTML entities not fully decoded in description
```

---

## 8. 🔗 Liên Kết Trong Pipeline

### Dependencies
- **Input:** `news_data_preprocessing` collection (từ script `04_normalize_language_noise.py`)
- **Prerequisite:** Phải chạy cleaning steps trước
  - 01_handle_missing_values_fast.py
  - 02_handle_invalid_noise.py
  - 03_detect_remove_outliers.py
  - 04_normalize_language_noise.py

### Next Steps
- ✅ Nếu validation passed → Tiến hành Integration phase
- ❌ Nếu có issues → Quay lại cleaning scripts để fix

---

## 9. 📝 Logging & Output Files

### Log Files Generated
```
../logs/validation/
└── cleaning_validation.log
    - Detailed validation results
    - Per-field statistics
    - Warnings and errors
```

### Information Logged
- Connection status
- Total documents processed
- Progress checkpoints (every 2000 docs)
- Detailed statistics per field
- Critical warnings
- Final validation status

---

## 10. 🛠️ Cách Sử Dụng

### Prerequisites
```bash
pip install pymongo
```

### Chạy Script
```bash
python cleaning_validator.py
```

### Check Results
```bash
# Xem log file
tail -f ../logs/validation/cleaning_validation.log

# Hoặc xem logs từ VS Code
```

---

## 11. 💡 Key Insights

### Strengths ✨
- ✅ Comprehensive validation coverage (5 dimensions)
- ✅ Auto-detects fields (không hardcode)
- ✅ Detailed statistics (min, max, avg word counts)
- ✅ Duplicate detection (though not removal)
- ✅ Clear logging và reporting
- ✅ Batch processing efficient (loads all docs once)

### Considerations 🤔
- **Memory:** Load tất cả documents → Need sufficient RAM for large datasets
- **Duplicates:** Chỉ detect, không remove (need separate dedup script)
- **Speed:** Single-threaded processing
- **HTML Entities:** Regex-based detection (may miss edge cases)

---

## 12. 📈 Performance Notes

### Complexity
- **Time:** O(n) for n documents
- **Space:** O(n) - load all documents into memory

### Estimated Runtime
- ~5-10 documents/second per 1GB RAM
- For 10,000 documents: ~10-20 minutes

### Optimization Options
1. **Batch processing:** Process documents in chunks
2. **Parallel validation:** Use multi-threading
3. **MongoDB aggregation:** Use `$group`, `$match` stages (avoid loading all docs)

---

## 13. 🎓 Kết Luận

**Cleaning Validator** là một script **comprehensive** để xác thực chất lượng dữ liệu sau cleaning phase. Nó:

- ✅ Kiểm tra 5 loại lỗi cleaning phổ biến
- ✅ Cung cấp detailed statistics
- ✅ Tự động detect text fields
- ✅ Logging chi tiết cho debugging

Là **essential checkpoint** trong data preprocessing pipeline trước khi tiến hành Integration & Transformation phases.

---

**Generated:** April 9, 2026  
**Status:** Ready for Validation Phase  
**Next:** Integration Phase (01_merge_field.py)
