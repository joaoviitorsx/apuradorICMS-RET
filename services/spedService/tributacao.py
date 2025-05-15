from db.conexao import conectar_banco, fechar_banco

async def criar_e_preencher_c170nova(nome_banco):
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

    # Atualizar descr_compl e cod_ncm a partir da tabela 0200
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

async def atualizar_cadastro_tributacao(nome_banco):
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    insert_query = """
        INSERT IGNORE INTO cadastro_tributacao (codigo, produto, ncm)
        SELECT 
            c170nova.cod_item, 
            c170nova.descr_compl, 
            c170nova.cod_ncm
        FROM c170nova
        WHERE NOT EXISTS (
            SELECT 1 
            FROM cadastro_tributacao 
            WHERE codigo = c170nova.cod_item
        )
    """
    try:
        cursor.execute(insert_query)
        linhas_afetadas = cursor.rowcount
        conexao.commit()
        print(f"{linhas_afetadas} registros inseridos em cadastro_tributacao.")
    except Exception as e:
        conexao.rollback()
        print(f"Erro ao atualizar cadastro_tributacao: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)