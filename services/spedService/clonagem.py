from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem de dados da tabela c170nova para c170_clone (empresa_id={empresa_id})")
    
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cfop, cst, ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado, chavefinal
            )
            SELECT 
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cst_icms, cfop, NULL, cod_ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, NULL, NULL,
                CONCAT(cod_item, chv_nfe)
            FROM c170nova
            WHERE empresa_id = %s
        """, (empresa_id,))
        
        conexao.commit()
        print(f"[OK] Clonagem concluída com sucesso. Registros afetados: {cursor.rowcount}")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha na clonagem de c170nova para c170_clone: {e}")

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")
