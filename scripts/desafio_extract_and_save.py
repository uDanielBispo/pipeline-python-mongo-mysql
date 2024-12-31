# Criar um script do arquivo extract_and_save_data.ipynb

from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = f"mongodb+srv://{DB_USER}:{DB_PASS}@cluster-alura.vse3v.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Alura"
api_produtos_uri = 'https://labdados.com/produtos'


def connect_db_mongo(uri):
    try:
        client = MongoClient(uri, server_api=ServerApi('1')) # Objeto de conexão com o mongo
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connection(client, db_name):
    db = client[db_name] # cria banco de dados e salva em db
    return db

def create_collection(db, collection_name):
    collection = db[collection_name]
    return collection

def extract_api_data(url):
    import requests
    return requests.get(url).json()
    
def insert_data(collection, data_to_insert):
    docs = collection.insert_many(data_to_insert)
    return len(docs.inserted_ids)

if __name__ == "__main__": #Executa o codigo apenas se esse Script for executado diretamente, e não se for importado
    client = connect_db_mongo(uri)
    db = create_connection(client, 'db_produtos_desafio')
    collection = create_collection(db, 'produtos')

    api_produtos_data = extract_api_data(api_produtos_uri)
    print(f"\nQuantidade de dados: {len(api_produtos_data)}")

    n_docs = insert_data(collection, api_produtos_data)
    print(f"\nQuantidade de documentos inseridos: {n_docs}")

    client.close()
