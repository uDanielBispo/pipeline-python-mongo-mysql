import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv('SQL_DB_USER')
DB_PASS = os.getenv('SQL_DB_PASS')

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = pw
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"\nBase de dados {db_name} criada")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

def create_product_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE {db_name}.{tb_name}(
                id VARCHAR(100),
                Produto VARCHAR(100),
                Categoria_Produto VARCHAR(100),
                Preco FLOAT(10,2),
                Frete FLOAT(10,2),
                Data_Compra DATE,
                Vendedor VARCHAR(100),
                Local_Compra VARCHAR(100),
                Avaliacao_Compra INT,
                Tipo_Pagamento VARCHAR(100),
                Qntd_Parcelas INT,
                Latitude FLOAT(10,2),
                Longitude FLOAT(10,2),
                
                PRIMARY KEY (id));
    """)
                   
    print(f"\nTabela {tb_name} criada")

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")
    cnx.commit()

if __name__ == "__main__":

    cnx = connect_mysql("localhost", DB_USER, DB_PASS)
    cursor = create_cursor(cnx)

    create_database(cursor, "db_produtos_teste")
    show_databases(cursor)

    create_product_table(cursor, "db_produtos_teste", "tb_livros")
    show_tables(cursor, "db_produtos_teste")

    df = read_csv("/home/daniel/Documentos/python-pipeline-com-Mongo-e-SQL/data/vendas_a_partir_de_2021.csv")
    add_product_data(cnx, cursor, df, "db_produtos_teste", "tb_livros")