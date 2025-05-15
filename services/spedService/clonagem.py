from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(nome_banco):
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc, cfop,
                id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado, chavefinal
            )
            SELECT 
                id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc, cfop,
                id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado,
                CONCAT(cod_item, chv_nfe) AS chavefinal
            FROM c170
            WHERE cod_part IN (
                SELECT cod_part FROM cadastro_fornecedores WHERE decreto = 'Não' AND uf = 'CE'
            )
            AND cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """)
        conexao.commit()
        print("Dados inseridos na tabela c170_clone com sucesso.")

        # Atualizar descr_compl com base na cadastro_tributacao
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo
            SET c.descr_compl = t.produto
            WHERE c.descr_compl IS NULL OR c.descr_compl = ''
        """)
        conexao.commit()
        print("descr_compl na c170_clone atualizado com nomes dos produtos.")

    except Exception as err:
        conexao.rollback()
        print(f"Erro ao clonar a tabela: {err}")
    finally:
        cursor.close()
        fechar_banco(conexao)