from db.conexao import conectar_banco, fechar_banco
from utils.cnpj import processar_cnpjs
from PySide6.QtCore import QObject, Signal

class Mensageiro(QObject):
    sinal_log = Signal(str)
    sinal_erro = Signal(str)

mensageiro = Mensageiro()

async def comparar_adicionar_atualizar_fornecedores(empresa_id):
    print(f"[DEBUG] Iniciando atualização de fornecedores para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        query_c170 = """
            SELECT DISTINCT cod_part 
            FROM c170 
            WHERE empresa_id = %s
              AND cod_part IS NOT NULL AND cod_part != ''
              AND cod_part NOT IN (
                  SELECT cod_part FROM cadastro_fornecedores WHERE empresa_id = %s
              )
        """
        cursor.execute(query_c170, (empresa_id, empresa_id))
        novos_cod_part = [row[0] for row in cursor.fetchall()]

        if novos_cod_part:
            dados_iniciais = [(empresa_id, cod, '', '', '', '', '', '') for cod in novos_cod_part]
            insert_query = """
                INSERT INTO cadastro_fornecedores 
                (empresa_id, cod_part, nome, cnpj, uf, cnae, decreto, simples)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, dados_iniciais)
            conexao.commit()
            mensageiro.sinal_log.emit(f"{len(novos_cod_part)} fornecedor(es) novo(s) inserido(s) a partir da c170.")

        query_update_cnpj = """
            UPDATE cadastro_fornecedores f
            JOIN `0150` p ON f.cod_part = p.cod_part AND f.empresa_id = p.empresa_id
            SET f.nome = p.nome, f.cnpj = p.cnpj
            WHERE f.empresa_id = %s AND (f.cnpj IS NULL OR f.cnpj = '')
        """
        cursor.execute(query_update_cnpj, (empresa_id,))
        conexao.commit()

        query_cnpjs = """
            SELECT DISTINCT cnpj
            FROM cadastro_fornecedores
            WHERE empresa_id = %s
              AND cnpj IS NOT NULL AND cnpj != ''
              AND (cnae IS NULL OR cnae = '' OR decreto IS NULL OR decreto = '' OR uf IS NULL OR uf = '')
        """
        cursor.execute(query_cnpjs, (empresa_id,))
        cnpjs_para_atualizar = [row[0] for row in cursor.fetchall()]

        if not cnpjs_para_atualizar:
            mensageiro.sinal_log.emit("Nenhum fornecedor para atualizar.")
            return

        print(f"[DEBUG] Consultando dados externos para {len(cnpjs_para_atualizar)} CNPJs...")
        resultados = await processar_cnpjs(cnpjs_para_atualizar)

        update_query = """
            UPDATE cadastro_fornecedores
            SET cnae = %s, decreto = %s, uf = %s, simples = %s
            WHERE cnpj = %s AND empresa_id = %s
        """

        dados_update = []
        for cnpj, info in resultados.items():
            if info:
                cnae, decreto, uf, simples = info
                dados_update.append((cnae, decreto, uf, simples, cnpj, empresa_id))

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
