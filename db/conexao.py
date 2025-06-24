import mysql.connector
from mysql.connector import Error

HOST = 'localhost'
USUARIO = 'root'
SENHA = '12345'
BANCO = 'empresasRT_db'

def conectar_mysql():
    try:
        conexao = mysql.connector.connect(
            host=HOST,
            user=USUARIO,
            password=SENHA
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao MySQL: {e}")
    return None

def conectar_banco():
    try:
        conexao = mysql.connector.connect(
            host=HOST,
            user=USUARIO,
            password=SENHA,
            database=BANCO
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao banco de dados '{BANCO}': {e}")
    return None

def fechar_banco(conexao):
    if conexao and conexao.is_connected():
        conexao.close()

def inicializar_banco():
    from db.criarTabelas import criar_tabelas_principais

    conexao_mysql = conectar_mysql()
    if not conexao_mysql:
        print("[FALHA] Não foi possível conectar ao MySQL.")
        return None

    try:
        cursor = conexao_mysql.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {BANCO}")
        print(f"[INFO] Banco '{BANCO}' verificado/criado com sucesso.")
    except Error as e:
        print(f"[ERRO] ao criar banco '{BANCO}': {e}")
    finally:
        fechar_banco(conexao_mysql)

    conexao_final = conectar_banco()
    if conexao_final:
        criar_tabela_empresas(conexao_final)
        criar_tabelas_principais() 
        return conexao_final
    return None

def criar_tabela_empresas(conexao):
    try:
        cursor = conexao.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS empresas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cnpj VARCHAR(20),
                razao_social VARCHAR(100)
            )
        """)
        conexao.commit()
        print("[INFO] Tabela 'empresas' criada/verificada com sucesso.")
    except Error as e:
        print(f"[ERRO] ao criar tabela 'empresas': {e}")
