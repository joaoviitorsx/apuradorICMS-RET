from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem de dados da tabela c170nova para c170_clone (empresa_id={empresa_id})")
    
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cst, ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado,
                chavefinal, nome, cnpj
            )
            SELECT 
                n.empresa_id, n.periodo, n.reg, n.num_item, n.cod_item, n.descr_compl,
                n.qtd, n.unid, n.vl_item, n.vl_desc, n.cst_icms, n.cod_ncm, n.id_c100, n.filial,
                n.ind_oper, n.cod_part, n.num_doc, n.chv_nfe, NULL, NULL,
                CONCAT(n.cod_item, n.chv_nfe),f.nome, f.cnpj
            FROM c170nova n
            LEFT JOIN `0150` f ON n.cod_part = f.cod_part AND n.empresa_id = f.empresa_id
            WHERE n.empresa_id = %s
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
