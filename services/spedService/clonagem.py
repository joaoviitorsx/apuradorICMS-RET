from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    conexao = None
    try:
        conexao = conectar_banco()
        cursor = conexao.cursor()

        print(f"[INÍCIO] Clonagem da tabela c170nova para c170_clone da empresa_id={empresa_id}")

        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cfop, cst, ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado, chavefinal
            )
            SELECT 
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cfop, NULL AS cst, cod_ncm AS ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, NULL AS aliquota, NULL AS resultado,
                CONCAT(cod_item, chv_nfe) AS chavefinal
            FROM c170nova
            WHERE empresa_id = %s
        """, (empresa_id,))

        conexao.commit()
        print(f"[OK] Dados inseridos na tabela c170_clone a partir da c170nova. Registros afetados: {cursor.rowcount}")

    except Exception as err:
        if conexao:
            conexao.rollback()
        print(f"[ERRO] Falha ao clonar a tabela: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if conexao:
            fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")
