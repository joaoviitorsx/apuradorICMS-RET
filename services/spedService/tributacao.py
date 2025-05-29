from db.conexao import conectar_banco, fechar_banco

async def criar_e_preencher_c170nova(empresa_id):
    print(f"[INÍCIO] Preenchendo c170nova para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        # Inserir registros na c170nova vindos da c170 (apenas se ainda não existirem)
        cursor.execute("""
            INSERT IGNORE INTO c170nova (
                cod_item, periodo, reg, num_item, descr_compl, qtd, unid, vl_item, vl_desc,
                cfop, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, empresa_id
            )
            SELECT DISTINCT 
                c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl,
                c.qtd, c.unid, c.vl_item, c.vl_desc, c.cfop, c.id_c100,
                c.filial, c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe, c.empresa_id
            FROM c170 c
            LEFT JOIN c170nova n ON c.cod_item = n.cod_item AND c.empresa_id = n.empresa_id
            JOIN (
                SELECT cod_part FROM cadastro_fornecedores
                WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
            ) f ON c.cod_part = f.cod_part
            WHERE c.empresa_id = %s
              AND c.cfop IN ('1101', '1102', '1116', '1401', '1403', '1910')
              AND n.cod_item IS NULL
        """, (empresa_id, empresa_id))

        total = cursor.rowcount
        conexao.commit()
        print(f"[OK] {total} registros inseridos em c170nova.")

        # Atualizar descr_compl e cod_ncm com base na 0200, mesmo se já vierem preenchidos com dados irrelevantes
        cursor.execute("""
            UPDATE c170nova n
            JOIN `0200` o 
            ON n.cod_item = o.cod_item AND o.empresa_id = %s
            SET 
                n.descr_compl = CASE 
                    WHEN TRIM(n.descr_compl) = '' OR n.descr_compl IS NULL OR n.descr_compl REGEXP '^[0-9]+$' 
                    THEN o.descr_item 
                    ELSE n.descr_compl 
                END,
                n.cod_ncm = CASE 
                    WHEN TRIM(n.cod_ncm) = '' OR n.cod_ncm IS NULL THEN o.cod_ncm 
                    ELSE n.cod_ncm 
                END
            WHERE n.empresa_id = %s
        """, (empresa_id, empresa_id))

        print(f"[OK] descr_compl e cod_ncm atualizados na c170nova. Linhas afetadas: {cursor.rowcount}")
        conexao.commit()

    except Exception as e:
        print(f"[ERRO] Falha em criar_e_preencher_c170nova: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização de c170nova.")

async def atualizar_cadastro_tributacao(empresa_id):
    print(f"[INÍCIO] Atualizando cadastro_tributacao para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO cadastro_tributacao (empresa_id, codigo, produto, ncm)
            SELECT %s, c.cod_item, c.descr_compl, c.ncm
            FROM c170_clone c
            WHERE c.empresa_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM cadastro_tributacao ct
                  WHERE ct.codigo = c.cod_item AND ct.empresa_id = %s
              )
        """, (empresa_id, empresa_id, empresa_id))

        novos = cursor.rowcount
        conexao.commit()
        print(f"[OK] {novos} novos produtos inseridos.")

        if novos > 0:
            print("[SANITIZAÇÃO] Corrigindo alíquotas inválidas...")
            cursor.execute("""
                UPDATE cadastro_tributacao
                SET aliquota = NULL
                WHERE empresa_id = %s AND (
                    TRIM(aliquota) NOT REGEXP '^[0-2]?[0-9](,[0-9]{1,2})?%$'
                    OR CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(5,2)) > 30
                )
            """, (empresa_id,))
            conexao.commit()
            print("[OK] Alíquotas inválidas resetadas.")
        else:
            print("[INFO] Nenhum novo produto encontrado para sanitização.")
    except Exception as e:
        print(f"[ERRO] Falha ao atualizar cadastro_tributacao: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização da atualização de cadastro_tributacao.")

