import json

# Read the final_predictions.json file
with open('d:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\final_predictions.json', 'r', encoding='utf-8') as f:
    predictions = json.load(f)

# Group articles by predicted_label
articles_by_category = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: []}

for article in predictions:
    label = article['predicted_label']
    if label in articles_by_category:
        articles_by_category[label].append(article)

# Select 20 articles from each category
selected_articles = []
for label in sorted(articles_by_category.keys()):
    articles = articles_by_category[label]
    selected_articles.extend(articles[:20])

# Print statistics
print(f"Total categories: {len(articles_by_category)}")
for label in sorted(articles_by_category.keys()):
    print(f"Category {label}: {len(articles_by_category[label])} articles, selecting {min(20, len(articles_by_category[label]))}")

print(f"\nTotal selected articles: {len(selected_articles)}")

# Save to a new JSON file
output_path = 'd:\\vietnamese_news\\3. Processing\\5. Final Evaluation\\output\\selected_140_articles.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(selected_articles, f, ensure_ascii=False, indent=2)

print(f"\nSaved to: {output_path}")
