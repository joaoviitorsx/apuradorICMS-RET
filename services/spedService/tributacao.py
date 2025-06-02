import time
from db.conexao import conectar_banco, fechar_banco

def atualizar_descr_compl_em_lotes(conexao, empresa_id, lote=1000):
    cursor = conexao.cursor()
    total_atualizados = 0
    rodada = 0

    print(f"[descr_compl] Iniciando atualização em lotes com base em cod_item + cod_ncm...")

    try:
        while True:
            rodada += 1
            print(f"  ➤ Lote {rodada}...")

            cursor.execute(f"""
                UPDATE c170nova n
                JOIN (
                    SELECT n.id, o.descr_item
                    FROM c170nova n
                    JOIN `0200` o 
                      ON n.cod_item = o.cod_item
                     AND o.empresa_id = %s
                    WHERE n.empresa_id = %s
                      AND (
                          n.descr_compl IS NULL 
                          OR TRIM(n.descr_compl) = '' 
                          OR n.descr_compl REGEXP '^[0-9]+$'
                          OR n.descr_compl <> o.descr_item
                      )
                    LIMIT {lote}
                ) AS t ON n.id = t.id
                SET n.descr_compl = t.descr_item
            """, (empresa_id, empresa_id))

            atualizados = cursor.rowcount
            total_atualizados += atualizados
            conexao.commit()

            print(f"  ✔️ {atualizados} linhas atualizadas.")
            if atualizados < lote:
                break

        print(f"[descr_compl] Total atualizado: {total_atualizados}")

    except Exception as e:
        print(f"[ERRO] descr_compl em lote: {e}")
        conexao.rollback()

    finally:
        cursor.close()

def atualizar_cod_ncm_em_lotes(conexao, empresa_id, lote=1000):
    cursor = conexao.cursor()
    total_atualizados = 0
    rodada = 0

    print(f"[cod_ncm] Iniciando atualização em lotes com base em cod_item + descr_compl...")

    try:
        while True:
            rodada += 1
            print(f"  ➤ Lote {rodada}...")

            cursor.execute(f"""
                UPDATE c170nova n
                JOIN (
                    SELECT n.id, o.cod_ncm
                    FROM c170nova n
                    JOIN `0200` o 
                      ON n.cod_item = o.cod_item
                     AND o.empresa_id = %s
                    WHERE n.empresa_id = %s
                      AND (n.cod_ncm IS NULL OR TRIM(n.cod_ncm) = '')
                      AND o.cod_ncm IS NOT NULL AND TRIM(o.cod_ncm) <> ''
                    LIMIT {lote}
                ) AS t ON n.id = t.id
                SET n.cod_ncm = t.cod_ncm
            """, (empresa_id, empresa_id))

            atualizados = cursor.rowcount
            total_atualizados += atualizados
            conexao.commit()

            print(f"  ✔️ {atualizados} linhas atualizadas.")
            if atualizados < lote:
                break

        print(f"[cod_ncm] Total atualizado: {total_atualizados}")

    except Exception as e:
        print(f"[ERRO] cod_ncm em lote: {e}")
        conexao.rollback()

    finally:
        cursor.close()

async def criar_e_preencher_c170nova(empresa_id):
    print(f"[INÍCIO] Preenchendo c170nova para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        tempo_inicio = time.time()

        cursor.execute("""
            INSERT IGNORE INTO c170nova (
                cod_item, periodo, reg, num_item, descr_compl, qtd, unid, 
                vl_item, vl_desc, cfop, cst, id_c100, filial, ind_oper, 
                cod_part, num_doc, chv_nfe, empresa_id
            )
            SELECT 
                c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl, 
                c.qtd, c.unid, c.vl_item, c.vl_desc, c.cfop, c.cst_icms, 
                c.id_c100, c.filial, c.ind_oper, cc.cod_part, cc.num_doc, 
                cc.chv_nfe, c.empresa_id
            FROM c170 c
            JOIN c100 cc 
                ON cc.id = c.id_c100
            JOIN (
                SELECT cod_part FROM cadastro_fornecedores
                WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
            ) f ON cc.cod_part = f.cod_part
            LEFT JOIN c170nova n 
                ON n.cod_item = c.cod_item 
            AND n.id_c100 = c.id_c100 
            AND n.empresa_id = c.empresa_id
            WHERE c.empresa_id = %s
            AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
            AND n.cod_item IS NULL
        """, (empresa_id, empresa_id))


        total = cursor.rowcount
        conexao.commit()
        print(f"[OK] {total} registros inseridos em c170nova.")
        print(f"[TEMPO] Inserção concluída em {time.time() - tempo_inicio:.2f}s")

        tempo_inicio = time.time()
        atualizar_descr_compl_em_lotes(conexao, empresa_id)
        print(f"[TEMPO] descr_compl finalizado em {time.time() - tempo_inicio:.2f}s")

        tempo_inicio = time.time()
        atualizar_cod_ncm_em_lotes(conexao, empresa_id)
        print(f"[TEMPO] cod_ncm finalizado em {time.time() - tempo_inicio:.2f}s")

    except Exception as e:
        print(f"[ERRO] Falha em criar_e_preencher_c170nova: {e}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização de c170nova.")

