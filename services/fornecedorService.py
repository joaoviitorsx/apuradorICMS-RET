from db.conexao import conectar_banco, fechar_banco
from utils.cnpj import processar_cnpjs

async def comparar_adicionar_atualizar_fornecedores(nome_banco):
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        # Verifica se as colunas cnae, decreto, uf, simples existem
        cursor.execute("""
            SHOW COLUMNS FROM cadastro_fornecedores
            WHERE Field IN ('cnae', 'decreto', 'uf', 'simples')
        """)
        columns = [row[0] for row in cursor.fetchall()]

        if not all(col in columns for col in ['cnae', 'decreto', 'uf', 'simples']):
            print("Colunas obrigatórias ausentes na tabela cadastro_fornecedores.")
            return

        # Seleciona fornecedores novos (estão na 0150 mas não estão na cadastro_fornecedores)
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

        # Busca CNPJs que precisam ser atualizados
        cursor.execute("""
            SELECT cnpj FROM cadastro_fornecedores
            WHERE (cnae IS NULL OR cnae = '') AND (decreto IS NULL OR decreto = '') AND (uf IS NULL OR uf = '')
        """)
        cnpjs = [row[0] for row in cursor.fetchall()]

        resultados = await processar_cnpjs(cnpjs)

        for cnpj, (cnae, decreto, uf, simples) in resultados.items():
            cursor.execute("""
                UPDATE cadastro_fornecedores
                SET cnae = %s, decreto = %s, uf = %s, simples = %s
                WHERE cnpj = %s
            """, (cnae, decreto, uf, simples, cnpj))

        conexao.commit()
        print(f"Fornecedores adicionados/atualizados com sucesso: {len(cnpjs)}")

    except Exception as e:
        conexao.rollback()
        print(f"Erro ao atualizar fornecedores: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)