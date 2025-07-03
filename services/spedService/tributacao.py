from db.conexao import conectarBanco, fecharBanco

def criarC170nova(empresa_id, lote_tamanho=3000):
    print(f"[INÍCIO] Preenchendo c170nova para empresa_id={empresa_id}")
    conexao = conectarBanco()
    cursor = conexao.cursor()

    total_inseridos = 0
    offset = 0

    try:
        #carregar dados auxiliares em memoria
        print("[parte 1] carregando dados do fornecedores")
        cursor.execute("""
            SELECT cod_part, empresa_id, uf, decreto FROM cadastro_fornecedores
            WHERE empresa_id = %s
        """, (empresa_id,))
        fornecedores = {f"{row[0]}_{row[1]}": {"uf": row[2], "decreto": row[3]} for row in cursor.fetchall()}

        print("[parte 2] carregando dados da tabela 0200")
        cursor.execute("""
            SELECT cod_item, empresa_id, descr_item, cod_ncm FROM `0200`
            WHERE empresa_id = %s
        """, (empresa_id,))
        dados_0200 = {f"{row[0]}_{row[1]}": {"descr_item": row[2], "cod_ncm": row[3]} for row in cursor.fetchall()}

        print("[parte 3] iniciando processamento em lotes")
        while True:
            print(f"[LOTE] OFFSET {offset} - {offset + lote_tamanho}")

            cursor.execute("""
                SELECT 
                    c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl, c.qtd,
                    c.unid, c.vl_item, c.vl_desc, c.cfop, c.cst_icms, c.id_c100,
                    c.filial, c.ind_oper, cc.cod_part, cc.num_doc, cc.chv_nfe,
                    c.empresa_id
                FROM c170 c
                JOIN c100 cc ON cc.id = c.id_c100
                WHERE c.empresa_id = %s
                AND c.cfop IN (
                    '1101', '1401', '1102', '1403', '1910', '1116',
                    '2101', '2102', '2401', '2403', '2910', '2116'
                )
                LIMIT %s OFFSET %s;
            """, (empresa_id, lote_tamanho, offset))

            linhas = cursor.fetchall()
            if not linhas:
                break

            dados_insercao = []
            for row in linhas:
                (
                    cod_item, periodo, reg, num_item, descr_compl, qtd,
                    unid, vl_item, vl_desc, cfop, cst, id_c100, filial,
                    ind_oper, cod_part, num_doc, chv_nfe, empresa_id
                ) = row

                chave_forn = f"{cod_part}_{empresa_id}"
                forn = fornecedores.get(chave_forn)
                if not forn:
                    continue

                #filtro de uf/decreto
                if not ((forn['uf'] == 'CE' and forn['decreto'] == 'Não') or (forn['uf'] != 'CE')):
                    continue

                chave_0200 = f"{cod_item}_{empresa_id}"
                ref_0200 = dados_0200.get(chave_0200, {})
                descricao = ref_0200.get("descr_item") or descr_compl
                cod_ncm = ref_0200.get("cod_ncm")

                dados_insercao.append((
                    cod_item, periodo, reg, num_item, descricao, qtd, unid,
                    vl_item, vl_desc, cfop, cst, id_c100, filial, ind_oper,
                    cod_part, num_doc, chv_nfe, empresa_id, cod_ncm, forn["uf"]
                ))

            #inserção em lote
            if dados_insercao:
                cursor.executemany("""
                    INSERT INTO c170nova (
                        cod_item, periodo, reg, num_item, descr_compl, qtd, unid, 
                        vl_item, vl_desc, cfop, cst, id_c100, filial, ind_oper, 
                        cod_part, num_doc, chv_nfe, empresa_id, cod_ncm, uf
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                """, dados_insercao)
                conexao.commit()
                total_inseridos += len(dados_insercao)
            else:
                print("[AVISO] Nenhum registro válido para inserir neste lote.")

            if len(linhas) < lote_tamanho:
                break

            offset += lote_tamanho

        print(f"[FINALIZADO] Total de {total_inseridos} registros inseridos em c170nova.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha ao preencher c170nova: {e}")

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Conexão encerrada.")
