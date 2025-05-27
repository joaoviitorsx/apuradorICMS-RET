from db.conexao import conectar_banco, fechar_banco

async def criar_e_preencher_c170nova(nome_banco):
    print("Criando e preenchendo c170nova...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    cfops = ['1101', '1102', '1116', '1401', '1403', '1910']

    cursor.execute("""
        SELECT cod_part
        FROM cadastro_fornecedores
        WHERE decreto = 'Não' AND uf = 'CE'
    """)
    fornecedores = set(row[0] for row in cursor.fetchall())
    if not fornecedores:
        print("Nenhum fornecedor com critérios encontrados.")
        return

    query = f"""
        SELECT DISTINCT c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl,
               c.qtd, c.unid, c.vl_item, c.vl_desc, c.cfop, c.id_c100,
               c.filial, c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe
        FROM c170 c
        LEFT JOIN c170nova n ON c.cod_item = n.cod_item
        WHERE c.cod_part IN ({','.join(['%s'] * len(fornecedores))})
        AND c.cfop IN ({','.join(['%s'] * len(cfops))})
        AND n.cod_item IS NULL
    """
    cursor.execute(query, tuple(fornecedores) + tuple(cfops))

    dados = cursor.fetchall()
    if not dados:
        print("Nenhum item para inserir na c170nova.")
        return

    insert_query = """
        INSERT IGNORE INTO c170nova (
            cod_item, periodo, reg, num_item, descr_compl, qtd, unid, vl_item, vl_desc,
            cfop, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, dados)
    conexao.commit()

    cursor.execute("""
        UPDATE c170nova n
        JOIN `0200` o ON n.cod_item = o.cod_item
        SET n.descr_compl = o.descr_item,
            n.cod_ncm = o.cod_ncm
        WHERE (n.descr_compl IS NULL OR n.descr_compl = '')
          AND (n.cod_ncm IS NULL OR n.cod_ncm = '')
    """)
    conexao.commit()

    cursor.close()
    fechar_banco(conexao)
    print("c170nova preenchida com sucesso.")

async def atualizar_cadastro_tributacao(nome_banco, empresa_id):
    print("Atualizando cadastro_tributacao...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO cadastro_tributacao (empresa_id, codigo, produto, ncm)
            SELECT %s, c.cod_item, c.descr_compl, c.ncm
            FROM c170_clone c
            WHERE NOT EXISTS (
                SELECT 1 FROM cadastro_tributacao ct
                WHERE ct.codigo = c.cod_item AND ct.empresa_id = %s
            )
        """, (empresa_id, empresa_id))

        novos = cursor.rowcount
        conexao.commit()
        print(f"[OK] {novos} produtos novos inseridos no cadastro_tributacao.")

        if novos > 0:
            print("[SANITIZAÇÃO] Limpando alíquotas inválidas...")
            cursor.execute("""
                UPDATE cadastro_tributacao
                SET aliquota = NULL
                WHERE
                    TRIM(aliquota) NOT REGEXP '^[0-2]?[0-9](,[0-9]{1,2})?%$'
                    OR CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(5,2)) > 30
                    AND empresa_id = %s
            """, (empresa_id,))
            conexao.commit()
            print("[OK] Alíquotas inválidas foram resetadas para NULL.")
        else:
            print("[INFO] Nenhum produto novo inserido. Sanitização de alíquotas não necessária.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha ao verificar/atualizar cadastro_tributacao: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)
