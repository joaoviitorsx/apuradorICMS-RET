from db.conexao import conectar_banco, fechar_banco
from utils.cnpj import processar_cnpjs
from utils.mensagem import mensagem_error, mensagem_sucesso
import logging

# Opcional: configure o logger uma vez
logging.basicConfig(level=logging.INFO)

async def comparar_adicionar_atualizar_fornecedores(nome_banco):
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        # Verifica se as colunas obrigatórias existem
        cursor.execute("""
            SHOW COLUMNS FROM cadastro_fornecedores
            WHERE Field IN ('cnae', 'decreto', 'uf', 'simples')
        """)
        columns = [row[0] for row in cursor.fetchall()]

        if not all(col in columns for col in ['cnae', 'decreto', 'uf', 'simples']):
            mensagem_error("Colunas obrigatórias ausentes na tabela 'cadastro_fornecedores'.")
            return

        # Insere novos fornecedores encontrados na 0150 que ainda não estão cadastrados
        cursor.execute("""
            SELECT f.cod_part, f.nome, f.cnpj
            FROM `0150` f
            LEFT JOIN cadastro_fornecedores cf ON f.cod_part = cf.cod_part
            WHERE cf.cod_part IS NULL AND f.cnpj IS NOT NULL AND f.cnpj != ''
        """)
        novos_fornecedores = cursor.fetchall()

        for cod_part, nome, cnpj in novos_fornecedores:
            cursor.execute("""
                INSERT INTO cadastro_fornecedores (cod_part, nome, cnpj, uf, cnae, decreto, simples)
                VALUES (%s, %s, %s, '', '', '', '')
            """, (cod_part, nome, cnpj))

        # Busca CNPJs que ainda precisam ser atualizados
        cursor.execute("""
            SELECT cnpj FROM cadastro_fornecedores
            WHERE (cnae IS NULL OR cnae = '') AND (decreto IS NULL OR decreto = '') AND (uf IS NULL OR uf = '')
        """)
        cnpjs = [row[0] for row in cursor.fetchall()]

        if not cnpjs:
            logging.info("Nenhum CNPJ pendente de atualização.")
            return

        # Atualiza em lotes para evitar sobrecarga
        BATCH_SIZE = 100
        total_atualizados = 0

        for i in range(0, len(cnpjs), BATCH_SIZE):
            batch = cnpjs[i:i + BATCH_SIZE]
            resultados = await processar_cnpjs(batch)

            for cnpj, (cnae, decreto, uf, simples) in resultados.items():
                cursor.execute("""
                    UPDATE cadastro_fornecedores
                    SET cnae = %s, decreto = %s, uf = %s, simples = %s
                    WHERE cnpj = %s
                """, (cnae, decreto, uf, simples, cnpj))
                total_atualizados += 1

        conexao.commit()
        mensagem_sucesso(f"Fornecedores atualizados com sucesso: {total_atualizados}")

    except Exception as e:
        conexao.rollback()
        mensagem_error(f"Erro ao atualizar fornecedores: {str(e)}")

    finally:
        cursor.close()
        fechar_banco(conexao)
