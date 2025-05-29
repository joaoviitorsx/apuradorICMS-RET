from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    TABLE_NAME = 'c170'
    CLONE_TABLE_NAME = 'c170_clone'
    TRIBUTACAO_TABLE = 'cadastro_tributacao'

    conexao = None
    try:
        conexao = conectar_banco()
        cursor = conexao.cursor()

        print(f"[INÍCIO] Clonagem da tabela {TABLE_NAME} para {CLONE_TABLE_NAME} da empresa_id={empresa_id}")

        cursor.execute(f"""
            INSERT IGNORE INTO {CLONE_TABLE_NAME} (
                id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid,
                vl_item, vl_desc, cfop, ncm, id_c100, filial, ind_oper,
                cod_part, num_doc, chv_nfe, aliquota, resultado, chavefinal, empresa_id
            )
            SELECT 
                id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid,
                vl_item, vl_desc, cfop, ncm, id_c100, filial, ind_oper,
                cod_part, num_doc, chv_nfe, aliquota, resultado,
                CONCAT(cod_item, chv_nfe) AS chavefinal, empresa_id
            FROM {TABLE_NAME}
            WHERE empresa_id = %s
            AND cod_part IN (
                SELECT cod_part
                FROM cadastro_fornecedores
                WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
            )
            AND cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """, (empresa_id, empresa_id))

        conexao.commit()
        print(f"[OK] Dados inseridos na tabela {CLONE_TABLE_NAME}. Registros afetados: {cursor.rowcount}")

        # Atualização de descr_compl
        cursor.execute(f"""
            UPDATE {CLONE_TABLE_NAME} c
            JOIN {TRIBUTACAO_TABLE} t
                ON c.cod_item = t.codigo AND c.empresa_id = t.empresa_id
            SET c.descr_compl = t.produto
            WHERE (c.descr_compl IS NULL OR c.descr_compl = '')
            AND c.empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print(f"[OK] descr_compl atualizado com base na tributação. Registros afetados: {cursor.rowcount}")

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
