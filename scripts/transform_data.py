from desafio_extract_and_save import connect_db_mongo, create_connection, create_collection

from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = f"mongodb+srv://{DB_USER}:{DB_PASS}@cluster-alura.vse3v.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Alura"
api_produtos_uri = 'https://labdados.com/produtos'


def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {col_name: new_name}})

def select_category(col, category):
    query = {'Categoria do Produto': category}

    lista_categorias = []
    for doc in col.find(query):
        lista_categorias.append(doc)
    
    return lista_categorias

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

def format_date(df, column):
    df[column] = pd.to_datetime(df[column], format="%d/%m/%Y")
    df[column] = df[column].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

if __name__ == "__main__": #Executa o codigo apenas se esse Script for executado diretamente, e não se for importado
    client = connect_db_mongo(uri)
    db = create_connection(client, 'db_produtos_desafio')
    collection = create_collection(db, 'produtos')
    
    #visualize_collection(collection)

    rename_column(collection, "lat", "Latitude")
    rename_column(collection, "lon", "Longitude")

    lista_livros = select_category(collection, 'livros')
    df_livros = create_dataframe(lista_livros)
    format_date(df_livros, "Data da Compra")
    save_csv(df_livros, "./data/tabela_livros.csv")

    lista_produtos = make_regex(collection, "/202[1-9]")
    df_produtos = create_dataframe(lista_produtos)
    format_date(df_produtos, "Data da Compra")
    save_csv(df_produtos, "./data/vendas_a_partir_de_2021.csv")

    client.close()
