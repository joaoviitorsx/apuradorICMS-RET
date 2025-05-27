from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem da tabela c170 para empresa_id={empresa_id}")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        # Corrige campos decimais em branco que causam erro de conversão
        cursor.execute("""
            UPDATE c170
            SET vl_desc = NULL
            WHERE TRIM(vl_desc) = '' AND empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print("[OK] Campos decimais normalizados para evitar erros de conversão.")

        cursor.execute("SELECT COUNT(*) FROM c170 WHERE empresa_id = %s", (empresa_id,))
        print(f"[DEBUG] Registros totais na c170: {cursor.fetchone()[0]}")

        # Inserir registros na tabela clone
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                empresa_id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid,
                vl_item, vl_desc, cfop, id_c100, filial, ind_oper, cod_part,
                num_doc, chv_nfe, aliquota, resultado, chavefinal
            )
            SELECT 
                c.empresa_id, c.periodo, c.reg, c.num_item, c.cod_item, c.descr_compl, c.qtd, c.unid,
                c.vl_item, c.vl_desc, c.cfop, c.id_c100, c.filial, c.ind_oper, c.cod_part,
                c.num_doc, c.chv_nfe, c.aliquota, c.resultado,
                CONCAT(c.cod_item, c.chv_nfe) AS chavefinal
            FROM c170 c
            JOIN cadastro_fornecedores f
            ON c.cod_part = f.cod_part AND f.empresa_id = c.empresa_id
            WHERE c.empresa_id = 1
            AND f.decreto = 'Não'
            AND f.uf = 'CE'
            AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
        """, (empresa_id,))
        print(f"[OK] Tentativa de inserção concluída. Registros impactados: {cursor.rowcount}")
        conexao.commit()

        cursor.execute("SELECT COUNT(*) FROM c170_clone WHERE empresa_id = %s", (empresa_id,))
        print(f"[DEBUG] Total de registros na c170_clone após inserção: {cursor.fetchone()[0]}")

        # Atualizar descr_compl com nomes do cadastro_tributacao
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND c.empresa_id = t.empresa_id
            SET c.descr_compl = t.produto
            WHERE (c.descr_compl IS NULL OR c.descr_compl = '')
        """)
        conexao.commit()
        print("[OK] descr_compl atualizado com base na tributação. Registros afetados:", cursor.rowcount)

    except Exception as err:
        conexao.rollback()
        print(f"[ERRO] Falha ao clonar c170: {err}")
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")
