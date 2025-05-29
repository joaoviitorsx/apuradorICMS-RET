from db.conexao import conectar_banco, fechar_banco
from utils.cnpj import processar_cnpjs
from PySide6.QtCore import QObject, Signal

class Mensageiro(QObject):
    sinal_log = Signal(str)
    sinal_erro = Signal(str)

mensageiro = Mensageiro()

BATCH_SIZE = 50  # Tamanho do lote de atualização

async def comparar_adicionar_atualizar_fornecedores(empresa_id):
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        print("🔧 Verificando estrutura da tabela cadastro_fornecedores...")
        cursor.execute("""
            SHOW COLUMNS FROM cadastro_fornecedores
            WHERE Field IN ('cnae', 'decreto', 'uf', 'simples')
        """)
        columns = [row[0] for row in cursor.fetchall()]
        if not all(col in columns for col in ['cnae', 'decreto', 'uf', 'simples']):
            print("⚠️ Colunas obrigatórias não encontradas.")
            return

        print("📦 Buscando fornecedores a adicionar...")
        cursor.execute("""
            SELECT f.cod_part, f.nome, f.cnpj
            FROM `0150` f
            LEFT JOIN cadastro_fornecedores cf ON TRIM(f.cod_part) = TRIM(cf.cod_part) AND f.empresa_id = cf.empresa_id
            WHERE cf.cod_part IS NULL AND f.cnpj IS NOT NULL AND f.cnpj != '' AND f.empresa_id = %s
        """, (empresa_id,))
        fornecedores = cursor.fetchall()

        for cod_part, nome, cnpj in fornecedores:
            cursor.execute("""
                INSERT INTO cadastro_fornecedores (empresa_id, cod_part, nome, cnpj, uf, cnae, decreto, simples)
                VALUES (%s, %s, %s, %s, '', '', '', '')
            """, (empresa_id, cod_part, nome, cnpj))
        conexao.commit()
        print(f"✅ {len(fornecedores)} fornecedores adicionados.")

        print("📊 Buscando fornecedores com dados pendentes...")
        cursor.execute("""
            SELECT cnpj
            FROM cadastro_fornecedores
            WHERE empresa_id = %s AND cnpj IS NOT NULL AND cnpj != ''
              AND (cnae IS NULL OR cnae = '' OR decreto IS NULL OR decreto = '' OR uf IS NULL OR uf = '')
        """, (empresa_id,))
        cnpjs = [row[0] for row in cursor.fetchall()]

        if not cnpjs:
            print("🔍 Nenhum CNPJ pendente de atualização.")
            return

        print(f"🌐 Consultando dados externos para {len(cnpjs)} CNPJs...")
        resultados = await processar_cnpjs(cnpjs)

        print("🧠 Atualizando cadastro_fornecedores em lotes...")
        for i in range(0, len(cnpjs), BATCH_SIZE):
            batch = cnpjs[i:i + BATCH_SIZE]
            for cnpj in batch:
                if cnpj in resultados:
                    cnae, decreto, uf, simples = resultados[cnpj]
                    cursor.execute("""
                        UPDATE cadastro_fornecedores
                        SET cnae = %s, decreto = %s, uf = %s, simples = %s
                        WHERE cnpj = %s AND empresa_id = %s
                    """, (cnae, decreto, uf, simples, cnpj, empresa_id))
            conexao.commit()
            print(f"✅ Lote de {len(batch)} CNPJs atualizado.")

        print("🏁 Atualização concluída com sucesso.")

    except Exception as e:
        conexao.rollback()
        print(f"❌ Erro durante atualização de fornecedores: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)
