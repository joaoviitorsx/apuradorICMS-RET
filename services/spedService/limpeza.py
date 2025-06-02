from db.conexao import conectar_banco, fechar_banco

from db.conexao import conectar_banco, fechar_banco

def limpar_tabelas_temporarias(empresa_id):
    print("iniciando limpeza condicional")
    tabelas = ['`0000`', '`0150`', '`0200`', '`c100`', '`c170`', '`c170nova`']

    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        houve_limpeza = False
        for tabela in tabelas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabela} WHERE empresa_id = %s", (empresa_id,))
            total = cursor.fetchone()[0]
            if total > 0:
                cursor.execute(f"DELETE FROM {tabela} WHERE empresa_id = %s", (empresa_id,))
                houve_limpeza = True

        if houve_limpeza:
            conexao.commit()
            print(f"[LIMPEZA] Dados temporários da empresa {empresa_id} removidos com sucesso.")
        else:
            print(f"[LIMPEZA] Nenhum dado temporário encontrado para a empresa {empresa_id}.")

    except Exception as e:
        print(f"[ERRO] Falha ao limpar dados temporários: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
