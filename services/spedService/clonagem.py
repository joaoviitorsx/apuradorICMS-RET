from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"Clonando a tabela c170 para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc, cfop,
                id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado, chavefinal, empresa_id
            )
            SELECT 
                periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc, cfop,
                id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado,
                CONCAT(cod_item, chv_nfe) AS chavefinal, %s
            FROM c170
            WHERE empresa_id = %s
              AND cod_part IN (
                  SELECT cod_part FROM cadastro_fornecedores 
                  WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
              )
              AND cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """, (empresa_id, empresa_id, empresa_id))
        conexao.commit()
        print("[OK] Dados inseridos na tabela c170_clone.")

        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = %s
            SET c.descr_compl = t.produto
            WHERE (c.descr_compl IS NULL OR c.descr_compl = '') AND c.empresa_id = %s
        """, (empresa_id, empresa_id))
        conexao.commit()
        print("[OK] descr_compl na c170_clone atualizado com base na tabela de tributação.")

    except Exception as err:
        conexao.rollback()
        print(f"[ERRO] Falha ao clonar c170: {err}")
    finally:
        cursor.close()
        fechar_banco(conexao)
