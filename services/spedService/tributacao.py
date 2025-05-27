from db.conexao import conectar_banco, fechar_banco

async def criar_e_preencher_c170nova(empresa_id):
    print(f"Criando e preenchendo c170nova para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    cfops = ['1101', '1102', '1116', '1401', '1403', '1910']

    cursor.execute("""
        SELECT cod_part
        FROM cadastro_fornecedores
        WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
    """, (empresa_id,))
    fornecedores = set(row[0] for row in cursor.fetchall())
    if not fornecedores:
        print("[INFO] Nenhum fornecedor com os critérios encontrados.")
        return

    query = f"""
        SELECT DISTINCT c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl,
               c.qtd, c.unid, c.vl_item, c.vl_desc, c.cfop, c.id_c100,
               c.filial, c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe, c.empresa_id
        FROM c170 c
        LEFT JOIN c170nova n ON c.cod_item = n.cod_item AND c.empresa_id = n.empresa_id
        WHERE c.empresa_id = %s
          AND c.cod_part IN ({','.join(['%s'] * len(fornecedores))})
          AND c.cfop IN ({','.join(['%s'] * len(cfops))})
          AND n.cod_item IS NULL
    """
    params = (empresa_id,) + tuple(fornecedores) + tuple(cfops)
    cursor.execute(query, params)

    dados = cursor.fetchall()
    if not dados:
        print("[INFO] Nenhum item encontrado para inserir na c170nova.")
        return

    insert_query = """
        INSERT IGNORE INTO c170nova (
            cod_item, periodo, reg, num_item, descr_compl, qtd, unid, vl_item, vl_desc,
            cfop, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, empresa_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, dados)
    conexao.commit()

    cursor.execute("""
        UPDATE c170nova n
        JOIN `0200` o ON n.cod_item = o.cod_item AND o.empresa_id = %s
        SET n.descr_compl = o.descr_item,
            n.cod_ncm = o.cod_ncm
        WHERE (n.descr_compl IS NULL OR n.descr_compl = '')
          AND (n.cod_ncm IS NULL OR n.cod_ncm = '')
          AND n.empresa_id = %s
    """, (empresa_id, empresa_id))
    conexao.commit()

    cursor.close()
    fechar_banco(conexao)
    print("[OK] c170nova preenchida com sucesso.")


async def atualizar_cadastro_tributacao(empresa_id):
    print(f"Atualizando cadastro_tributacao para empresa_id={empresa_id}...")
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
        print(f"[OK] {novos} produtos inseridos no cadastro_tributacao.")

        if novos > 0:
            print("[SANITIZAÇÃO] Limpando alíquotas inválidas...")
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
            print("[INFO] Nenhum novo produto para sanitização.")
    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha ao atualizar cadastro_tributacao: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)
