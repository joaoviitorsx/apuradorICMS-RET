from db.conexao import conectar_banco, fechar_banco
from utils.cnpj import processar_cnpjs
from PySide6.QtCore import QObject, Signal
class Mensageiro(QObject):
    sinal_log = Signal(str)
    sinal_erro = Signal(str)

mensageiro = Mensageiro()

async def comparar_adicionar_atualizar_fornecedores(nome_banco):
    print("[DEBUG] Iniciando comparação e atualização de fornecedores...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        query_novos = """
            SELECT f.cod_part, f.nome, f.cnpj
            FROM `0150` f
            LEFT JOIN cadastro_fornecedores cf ON f.cod_part = cf.cod_part
            WHERE cf.cod_part IS NULL AND f.cnpj IS NOT NULL AND f.cnpj != ''
        """
        cursor.execute(query_novos)
        fornecedores_novos = cursor.fetchall()

        if fornecedores_novos:
            insert_query = """
                INSERT INTO cadastro_fornecedores (cod_part, nome, cnpj, uf, cnae, decreto, simples)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            dados = [(cod, nome, cnpj, '', '', '', '') for cod, nome, cnpj in fornecedores_novos]
            cursor.executemany(insert_query, dados)
            conexao.commit()
            mensageiro.sinal_log.emit(f"{len(fornecedores_novos)} fornecedor(es) novo(s) inserido(s).")

        query_cnpjs = """
            SELECT cnpj
            FROM cadastro_fornecedores
            WHERE (cnae IS NULL OR cnae = '') AND (decreto IS NULL OR decreto = '') AND (uf IS NULL OR uf = '')
        """
        cursor.execute(query_cnpjs)
        cnpjs_para_atualizar = [row[0] for row in cursor.fetchall()]

        if not cnpjs_para_atualizar:
            mensageiro.sinal_log.emit("Nenhum fornecedor para atualizar.")
            return

        resultados = await processar_cnpjs(cnpjs_para_atualizar)

        update_query = """
            UPDATE cadastro_fornecedores
            SET cnae = %s, decreto = %s, uf = %s, simples = %s
            WHERE cnpj = %s
        """

        dados_update = []
        for cnpj, (cnae, decreto, uf, simples) in resultados.items():
            dados_update.append((cnae, decreto, uf, simples, cnpj))

        if dados_update:
            cursor.executemany(update_query, dados_update)
            conexao.commit()
            mensageiro.sinal_log.emit(f"{len(dados_update)} fornecedor(es) atualizado(s) com sucesso.")

    except Exception as e:
        conexao.rollback()
        mensageiro.sinal_erro.emit(f"Erro ao atualizar fornecedores: {e}")

    finally:
        cursor.close()
        fechar_banco(conexao)