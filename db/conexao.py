import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

def env():
    load_dotenv()
    return {
        'host': os.getenv('HOST'),
        'usuario': os.getenv('USUARIO'),
        'senha': os.getenv('SENHA'),
        'banco': os.getenv('BANCO')
    }

def conectarMySQL():
    try:
        config = env()
        conexao = mysql.connector.connect(
            host=config['host'],
            user=config['usuario'],
            password=config['senha']
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao MySQL: {e}")
    return None

def conectarBanco():
    try:
        config = env()
        conexao = mysql.connector.connect(
            host=config['host'],
            user=config['usuario'],
            password=config['senha'],
            database=config['banco']
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao banco de dados '{config['banco']}': {e}")
    return None

def fecharBanco(conexao):
    if conexao and conexao.is_connected():
        conexao.close()

# def iniciliazarBanco():
#     from db.criarTabelas import criar_tabelas_principais, criar_tabela_empresas

#     config = env()
#     conexaoMySQL = conectarMySQL()
#     if not conexaoMySQL:
#         print("[FALHA] Não foi possível conectar ao MySQL.")
#         return None

#     try:
#         cursor = conexaoMySQL.cursor()
#         cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['banco']}")
#         print(f"[INFO] Banco '{config['banco']}' verificado/criado com sucesso.")
#     except Error as e:
#         print(f"[ERRO] ao criar banco '{config['banco']}': {e}")
#     finally:
#         fecharBanco(conexaoMySQL)

#     conexaoFinal = conectarBanco()
#     if conexaoFinal:
#         criar_tabela_empresas(conexaoFinal)
#         criar_tabelas_principais()
#         return conexaoFinal
#     return None
