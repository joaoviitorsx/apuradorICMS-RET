from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem da tabela c170 para empresa_id={empresa_id}")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        # Confirma conexão com banco
        cursor.execute("SELECT DATABASE()")
        print(f"[DEBUG] Banco conectado: {cursor.fetchone()[0]}")

        # Verifica se há dados na c170
        cursor.execute("SELECT COUNT(*) FROM c170 WHERE empresa_id = %s", (empresa_id,))
        total_c170 = cursor.fetchone()[0]
        print(f"[DEBUG] Registros totais na c170: {total_c170}")

        if total_c170 == 0:
            print(f"[INFO] Nenhum dado encontrado na c170 para empresa_id={empresa_id}. Abortando clonagem.")
            return

        # Garante fornecedores para a filtragem funcionar
        cursor.execute("""
            INSERT IGNORE INTO cadastro_fornecedores (cod_part, nome, uf, decreto, empresa_id)
            SELECT DISTINCT cod_part, CONCAT('FORNECEDOR ', cod_part), 'CE', 'Não', %s
            FROM c170
            WHERE empresa_id = %s AND cod_part IS NOT NULL
        """, (empresa_id, empresa_id))
        conexao.commit()
        print("[OK] Fornecedores ausentes inseridos.")

        # Contar registros válidos para a clonagem
        cursor.execute("""
            SELECT COUNT(*) FROM c170 c
            JOIN cadastro_fornecedores f ON c.cod_part = f.cod_part AND f.empresa_id = %s
            WHERE c.empresa_id = %s
              AND f.decreto = 'Não'
              AND f.uf = 'CE'
              AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """, (empresa_id, empresa_id))
        total_validos = cursor.fetchone()[0]
        print(f"[DEBUG] Registros válidos para clonagem: {total_validos}")

        if total_validos == 0:
            print("[INFO] Nenhum registro atende aos critérios de clonagem. Nada será inserido em c170_clone.")
            return

        # Verificar se já existem registros na clone
        cursor.execute("SELECT COUNT(*) FROM c170_clone WHERE empresa_id = %s", (empresa_id,))
        pre_total_clone = cursor.fetchone()[0]
        print(f"[DEBUG] Registros pré-existentes na c170_clone: {pre_total_clone}")

        # Inserção de dados
        cursor.execute("""
            INSERT INTO c170_clone (
                periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc, 
                cfop, ncm, id_c100, filial, ind_oper, cod_part, num_doc, 
                chv_nfe, aliquota, resultado, chavefinal, empresa_id
            )
            SELECT 
                c.periodo, c.reg, c.num_item, c.cod_item, c.descr_compl, c.qtd, c.unid, c.vl_item, c.vl_desc, 
                c.cfop, c.ncm, c.id_c100, c.filial, c.ind_oper, c.cod_part, c.num_doc, 
                c.chv_nfe, c.aliquota, c.resultado, CONCAT(c.cod_item, c.chv_nfe), c.empresa_id
            FROM c170 c
            WHERE c.empresa_id = %s
              AND c.cod_part IN (
                  SELECT cod_part 
                  FROM cadastro_fornecedores 
                  WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
              )
              AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """, (empresa_id, empresa_id))
        inseridos = cursor.rowcount
        conexao.commit()
        print(f"[OK] Tentativa de inserção concluída. Registros impactados: {inseridos}")

        # Verificação pós-inserção
        cursor.execute("SELECT COUNT(*) FROM c170_clone WHERE empresa_id = %s", (empresa_id,))
        total_clone = cursor.fetchone()[0]
        print(f"[DEBUG] Total de registros na c170_clone após inserção: {total_clone}")

        # Atualiza descr_compl com base na tributação
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND c.empresa_id = t.empresa_id
            SET c.descr_compl = t.produto
            WHERE (c.descr_compl IS NULL OR c.descr_compl = '') AND c.empresa_id = %s
        """, (empresa_id,))
        afetados = cursor.rowcount
        conexao.commit()
        print(f"[OK] descr_compl atualizado com base na tributação. Registros afetados: {afetados}")

    except Exception as err:
        conexao.rollback()
        print(f"[ERRO] Falha ao clonar c170: {err}")
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")
