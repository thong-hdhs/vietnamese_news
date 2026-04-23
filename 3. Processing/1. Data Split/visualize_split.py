import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def visualize():
    print("📊 Đang chuẩn bị vẽ biểu đồ từ 13,001 bài báo...")
    try:
        # Quan trọng: lines=True để đọc đúng định dạng JSON Lines cực nhanh
        train_df = pd.read_json("0. data/train_data.json", lines=True)
        val_df = pd.read_json("0. data/val_data.json", lines=True)
        
        # Đếm số lượng theo danh mục
        train_counts = train_df['category'].value_counts().reset_index()
        train_counts.columns = ['Category', 'Count']
        train_counts['Dataset'] = 'Train (85%)'

        val_counts = val_df['category'].value_counts().reset_index()
        val_counts.columns = ['Category', 'Count']
        val_counts['Dataset'] = 'Validation (15%)'

        # Gộp lại để vẽ cột đôi
        plot_data = pd.concat([train_counts, val_counts])

        plt.figure(figsize=(12, 7))
        sns.barplot(data=plot_data, x='Category', y='Count', hue='Dataset')
        
        plt.title('Phân bổ danh mục 13,001 bài - Kỹ thuật Stratified Split', fontsize=14)
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Lưu file ảnh cho Trí dán vào Slide
        plt.savefig("0. data/bieu_do_13000_bai.png")
        print("✅ Đã lưu biểu đồ thành công vào thư mục '0. data'!")
        plt.show()
    except Exception as e:
        print(f"❌ Lỗi khi vẽ biểu đồ: {e}")

if __name__ == "__main__":
    visualize()