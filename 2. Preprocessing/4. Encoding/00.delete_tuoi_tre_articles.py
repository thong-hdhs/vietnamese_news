from pymongo import MongoClient

def main():
    connection_string = 'mongodb+srv://thanhthong:JO1pMQ01y8wy5peD@cluster0.fud2s3r.mongodb.net/?appName=Cluster0&retryWrites=true&w=majority'
    client = MongoClient(connection_string)
    collection = client['vietnamese_news']['news_data_preprocessing']
    
    query = {
        'site': {
            '$regex': '^tuoi.?tre$',
            '$options': 'i'
        }
    }
    
    print("Deleting Tuoi Tre articles...")
    result = collection.delete_many(query)
    print(f"Successfully deleted {result.deleted_count} articles.")
    client.close()

if __name__ == "__main__":
    main()
