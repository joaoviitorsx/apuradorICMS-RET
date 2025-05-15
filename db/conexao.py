import mysql.connector
from mysql.connector import Error

# Configuração padrão para conexão
HOST = 'localhost'
USUARIO = 'root'
SENHA = '123456'


def conectar_banco(nome_banco):
    try:
        conexao = mysql.connector.connect(
            host=HOST,
            user=USUARIO,
            password=SENHA,
            database=nome_banco if nome_banco else None
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def fechar_banco(conexao):
    if conexao and conexao.is_connected():
        conexao.close()


def tabela_empresa(conexao):
    try:
        cursor = conexao.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS empresas_db")
        cursor.execute("USE empresas_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS empresas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cnpj VARCHAR(20),
                razao_social VARCHAR(100)
            )
        """)
        conexao.commit()
    except Error as e:
        print(f"Erro ao criar tabela de empresas: {e}")