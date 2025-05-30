from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem fiel ao projeto original (empresa_id={empresa_id})")

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        # Buscar registros da tabela c170 com os mesmos critérios do original
        cursor.execute("""
            SELECT 
                c.empresa_id, c.periodo, c.reg, c.num_item, c.cod_item, c.descr_compl,
                c.qtd, c.unid, c.vl_item, c.vl_desc,
                c.cst_icms, c.ncm, c.id_c100, c.filial,
                c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe,
                c.aliquota, NULL AS resultado,
                CONCAT(c.cod_item, c.chv_nfe) AS chavefinal
            FROM c170 c
            JOIN cadastro_fornecedores f
                ON c.cod_part = f.cod_part AND c.empresa_id = f.empresa_id
            WHERE f.decreto = 'Não' AND f.uf = 'CE'
              AND c.cfop IN ('1101', '1102', '1116', '1401', '1403', '1910')
              AND c.empresa_id = %s
        """, (empresa_id,))

        registros = cursor.fetchall()
        if not registros:
            print("[INFO] Nenhum registro qualificado para clonagem.")
            return

        insert_query = """
            INSERT IGNORE INTO c170_clone (
                empresa_id, periodo, reg, num_item, cod_item, descr_compl,
                qtd, unid, vl_item, vl_desc, cst, ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado,
                chavefinal
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s
            )
        """

        # Inserção em lotes
        batch_size = 500
        total_inseridos = 0

        for i in range(0, len(registros), batch_size):
            lote = registros[i:i + batch_size]
            cursor.executemany(insert_query, lote)
            conexao.commit()
            total_inseridos += cursor.rowcount

        print(f"[OK] Clonagem finalizada. Registros inseridos: {total_inseridos}")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha durante a clonagem fiel: {e}")

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")
