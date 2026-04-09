# Phân Tích Vectorization cho Semi-Supervised Learning - Tiếng Việt

## 📌 Bối Cảnh Bài Toán

- **Task**: Semi-supervised learning (25% labeled, 75% unlabeled)
- **Ngôn ngữ**: Tiếng Việt (đã tokenize, remove stopwords)
- **Mục tiêu**: Gắn nhãn 9,869 unlabeled documents từ 3,290 labeled
- **Model**: LogisticRegression với confidence-based self-training

---

## 1️⃣ TF-IDF (Recommended Primary Choice)

### Tại sao TF-IDF phù hợp cho Semi-Supervised Learning?

✅ **Advantages:**

- **Stable & Reproducible**: Không phụ thuộc vào random initialization
- **Fast**: Xử lý 13K docs chỉ trong vài giây
- **Fair to both labeled & unlabeled**: Không có bias từ labeled subset
- **Sparse vectors**: Giảm memory, tăng tốc độ training
- **Interpretable**: Dễ debug, explain feature importance
- **Good baseline**: Đã prove effective cho Vietnamese text classification
- **Thích hợp small labeled set**: TF-IDF không cần fine-tuning như neural models

⚠️ **Disadvantages:**

- Không capture semantic similarity (từ "đẹp" và "tuyệt vời" được treat như independent)
- Sparse representations (88-95% zeros)
- Không xử lý typos, misspellings tốt

### TF-IDF Config cho Tiếng Việt:

```python
TfidfVectorizer(
    max_features=5000,        # 5000 features đủ cho Vietnamese
    min_df=2,                 # Bỏ từ xuất hiện <2 lần
    max_df=0.8,               # Bỏ từ xuất hiện >80% docs (stop-words ngữ cảnh)
    ngram_range=(1, 2),       # Unigrams + bigrams (tiếng Việt cần bigrams)
    sublinear_tf=True,        # Log-scale TF (giảm influence của từ tần suất cao)
    norm='l2'                 # L2 normalization
)
```

**Expected Output:**

- X matrix: (13,159 × 5,000 TF-IDF) + (13,159 × 5 site_onehot) = 5,005 features
- Sparse format → Dense operations efficient
- Processing time: ~3-5 minutes

---

## 2️⃣ FastText (Vietnamese-Optimized Alternative)

### Tại sao xem xét FastText?

✅ **Advantages:**

- **Semantic awareness**: Capture word similarities (đẹp ≈ tuyệt vời)
- **OOV handling**: Xử lý typos tốt hơn Word2Vec
- **Pre-trained Vietnamese models**: Sẵn có từ Facebook Research
- **Dense vectors**: Compact representations
- **Good for small labeled set**: Pre-trained knowledge giúp

⚠️ **Disadvantages:**

- **Slower training**: ~10-15 min vs 3-5 min (TF-IDF)
- **More memory**: Dense vectors (300-dim) vs sparse TF-IDF
- **OOV variability**: Thay vì bỏ, dùng subword combination (có risk)
- **Requires GPU access**: Để training nhanh

### FastText Config cho Tiếng Việt:

```python
from gensim.models import FastText
from sklearn.preprocessing import StandardScaler
import numpy as np

# Option 1: Use pre-trained Vietnamese FastText (Facebook AI)
# Download: https://fasttext.cc/docs/en/crawl-vectors.html
# Model: cc.vi.300.bin

# Option 2: Train on your data
model = FastText(
    sentences=tokenized_texts,
    vector_size=300,
    window=5,
    min_count=2,
    workers=4,
    epochs=10
)

# Get document vectors (average of word vectors)
X_fasttext = np.array([
    np.mean([model.wv[word] for word in doc if word in model.wv], axis=0)
    for doc in tokenized_texts
])

# Combine with site_onehot
X = np.hstack([X_fasttext, site_onehot])  # (13,159 × 305)
```

---

## 3️⃣ PhoBERT (State-of-the-Art, But Overkill for This Task)

### Khi nào dùng PhoBERT?

✅ **Advantages:**

- **Contextual embeddings**: SOTA representation
- **Vietnamese-specific**: Fine-tuned trên Vietnamese corpus
- **Transfer learning**: Pre-trained on 160GB Vietnamese text

⚠️ **Disadvantages:**

- **VERY SLOW**: 30-60 min cho 13K docs (TPU/GPU)
- **Over-parameterized**: Overkill cho semi-supervised baseline
- **Hardware intensive**: Cần GPU (RTX 3090 minimal)
- **Fine-tuning risk**: May overfit trên 3,290 labeled docs

### Khi nào dùng PhoBERT:

```
❌ NOT recommended cho bước này (5.2 Vectorization)
✅ Có thể dùng cho bước 5.3+ nếu TF-IDF performance không tốt
```

---

## 4️⃣ Hybrid Approach (Best of Both Worlds)

### TF-IDF + Site Features (Recommended)

```
X = [TF-IDF 5000 cols] + [site_onehot 5 cols] = 5,005 features
```

**Why hybrid?**

- TF-IDF: Content relevance
- Site: Source bias (thanhnien → Thời sự, tuoitre → Giải trí)
- Combined: Better prediction signals

**Expected Results:**

- F1 baseline: 75-82% (on 3,290 labeled)
- Processing: 5 minutes
- Model size: Small (LogReg weights sparse)

---

## 📊 Comparison Matrix

| Criteria                  | TF-IDF    | FastText    | PhoBERT      |
| ------------------------- | --------- | ----------- | ------------ |
| Speed                     | ⚡⚡⚡ 5m | ⚡⚡ 15m    | 🐌 60m       |
| Semantic awareness        | ⭐⭐⭐    | ⭐⭐⭐⭐    | ⭐⭐⭐⭐⭐   |
| Memory                    | ✅ Low    | ⚠️ Medium   | ❌ High      |
| For Semi-Supervised       | ✅ Ideal  | ✅ Good     | ❌ Overkill  |
| Vietnamese support        | ✅ YES    | ✅ YES      | ✅ YES       |
| Implementation complexity | ✅ Easy   | ⚠️ Medium   | ❌ Hard      |
| Interpretability          | ✅ High   | ⭐⭐ Medium | ❌ Black-box |
| Baseline performance      | ⭐⭐⭐⭐  | ⭐⭐⭐⭐⭐  | ⭐⭐⭐⭐⭐   |

---

## 🎯 RECOMMENDATION for Your Use Case

### ✅ PRIMARY (Implement Now):

**TF-IDF + site_onehot**

```python
X = [5,000 TF-IDF features] + [5 site_onehot features]
Configuration:
  - max_features=5000
  - min_df=2, max_df=0.8
  - ngram_range=(1, 2)
  - sublinear_tf=True

Expected: 5-minute execution, F1=75-82% baseline
```

### 🔄 ALTERNATIVE (If TF-IDF baseline is poor):

**FastText pre-trained Vietnamese**

```python
# If TF-IDF F1 < 70%:
from gensim.models import fasttext
model = fasttext.load_model('cc.vi.300.bin')
X_text = np.mean([word vectors], axis=0) for each doc
X = [300 FastText] + [5 site_onehot]
```

### 🚀 ADVANCED (Last resort):

**PhoBERT fine-tuning**

```python
# Only if TF-IDF AND FastText both fail
# Use only after confirming semi-supervised strategy works
# Requires GPU training (1+ hour)
```

---

## 🔍 Tiếng Việt-Specific Considerations

### 1. **Tokenization** ✅ (Already done)

- Your data: tokenized, stopword-removed
- Vietnamese word boundary: correctly split (underscores)

### 2. **Ngram Strategy** ⭐ IMPORTANT FOR VIETNAMESE

- Unigrams alone: Lose context ("Thị_trường" = market, "Thị" = seeing)
- **Bigrams essential**: ("Thị_trường chứng_khoán" = stock market)
- Config: `ngram_range=(1, 2)` for TF-IDF

### 3. **Vietnamese Morphology**

- Not an inflectional language (like English)
- Stemming/Lemmatization: Less necessary, already handled by tokenization
- Your preprocessing (stopword removal) already optimal

### 4. **Website Source Bias**

- **thanhnien**: ~30% Thời sự, ~20% Kinh tế
- **tuoitre**: ~40% Giải trí, ~20% Sức khỏe
- **vnexpress**: Balanced mix
- **Source signals matter**: Công nghệ mostly from tech-focused sites

→ **site_onehot feature is crucial for semi-supervised** (helps pseudo-label decisions)

---

## 📋 Implementation Plan for PHASE 5.2

```
Step 1: Apply TF-IDF (no bigrams first, test)
Step 2: Combine with site_onehot → X matrix (13,159 × 5,005)
Step 3: Save vectorizer, X, y for next phases
Step 4: (Optional) Try ngram_range=(1,2) if baseline poor

Execution time: ~5 minutes
Output files:
  - vectorizer.pkl (scikit-learn TfidfVectorizer)
  - X_matrix.pkl (13,159 × 5,005 sparse matrix)
  - y_encoded.pkl (13,159 × 1 labels with -1 for unlabeled)
  - vectorization_stats.json (metadata)
```

---

## 🎓 Why NOT Word2Vec/GloVe?

- **Word2Vec**: Pre-trained Vietnamese models outdated (2016-2017)
- **GloVe**: No good Vietnamese pre-trained versions
- **FastText**: Modern, maintained, better subword handling
- **Result**: FastText > Word2Vec > GloVe for Vietnamese

---

## 💡 Key Insight for Semi-Supervised Learning

**Vectorization strategy matters LESS than model strategy:**

- TF-IDF vs FastText: ~3-5% F1 difference
- Self-training rounds & thresholds: ~10-20% F1 improvement

→ **Start simple (TF-IDF), optimize model strategy first**

---

## Summary - TL;DR

**For your semi-supervised learning project:**

✅ **USE: TF-IDF + site_onehot (PHASE 5.2 NOW)**

- Fast, stable, interpretable
- 5 minutes processing
- Expected F1 baseline: 75-82%

⚠️ **CONSIDER: FastText if TF-IDF baseline < 70%**

- Better semantic capture
- 15 minutes processing
- Requires pre-trained model

❌ **SKIP: PhoBERT for now**

- Overkill for baseline
- Slow (60 minutes)
- Use only if other methods fail

**Proceed with TF-IDF implementation in PHASE 5.2?**
