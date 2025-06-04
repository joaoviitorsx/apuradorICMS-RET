from db.conexao import conectar_banco, fechar_banco

async def atualizar_aliquota(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando alíquotas em c170_clone por lotes...")

    conexao = conectar_banco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("SELECT dt_ini FROM `0000` WHERE empresa_id = %s ORDER BY id DESC LIMIT 1", (empresa_id,))
        row = cursor.fetchone()
        if not row or not row['dt_ini']:
            print("[AVISO] Nenhum dt_ini encontrado. Cancelando.")
            return

        ano = int(row['dt_ini'][4:]) if len(row['dt_ini']) >= 6 else 0
        coluna = "aliquota" if ano >= 2024 else "aliquota_antiga"

        cursor.execute(f"""
            SELECT n.id AS id_c170, c.{coluna} AS nova_aliquota
            FROM c170_clone n
            JOIN cadastro_tributacao c
              ON c.empresa_id = n.empresa_id
             AND c.produto = n.descr_compl
             AND c.ncm = n.ncm
            WHERE n.empresa_id = %s
              AND (n.aliquota IS NULL OR n.aliquota = '')
              AND c.{coluna} IS NOT NULL AND c.{coluna} != ''
        """, (empresa_id,))
        registros = cursor.fetchall()
        total = len(registros)
        print(f"[INFO] {total} registros a atualizar...")

        for i in range(0, total, lote_tamanho):
            lote = registros[i:i + lote_tamanho]
            dados = [(r['nova_aliquota'][:10], r['id_c170']) for r in lote]

            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s
                WHERE id = %s
            """, dados)
            conexao.commit()
            print(f"[OK] Lote {i//lote_tamanho + 1} atualizado com {len(lote)} itens.")

        print(f"[FINALIZADO] Alíquotas atualizadas em {total} registros para empresa {empresa_id}.")

    except Exception as err:
        print(f"[ERRO] ao atualizar alíquotas: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota_simples(empresa_id, periodo):
    print("[INÍCIO] Atualizando alíquotas Simples Nacional (sem teto)...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_fornecedores f 
              ON f.cod_part = c.cod_part AND f.empresa_id = %s
            SET c.aliquota = CONCAT(
                REPLACE(FORMAT(
                    CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10,2)) + 3.00,
                    2
                ), '.', ','), '%')
            WHERE c.periodo = %s AND c.empresa_id = %s
              AND f.simples = 'Sim'
              AND c.aliquota RLIKE '^[0-9]+([.,][0-9]*)?%?$'
        """, (empresa_id, periodo, empresa_id))
        
        conexao.commit()
        print("[OK] Alíquotas do Simples atualizadas (sem teto).")
    except Exception as e:
        print(f"[ERRO] ao atualizar alíquota Simples: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_resultado(empresa_id):
    print("[INÍCIO] Atualizando resultado em c170_clone (considerando desconto)...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone
            SET resultado = CASE
                WHEN aliquota IS NULL OR TRIM(aliquota) = '' THEN 0
                WHEN aliquota RLIKE '^[A-Za-z]' THEN 0
                ELSE
                    ROUND(
                        (
                            CAST(REPLACE(REPLACE(IFNULL(vl_item, '0'), '.', ''), ',', '.') AS DECIMAL(15,4)) -
                            CAST(REPLACE(REPLACE(IFNULL(NULLIF(vl_desc, ''), '0'), '.', ''), ',', '.') AS DECIMAL(15,4))
                        ) *
                        (CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(15,4)) / 100),
                        2
                    )
            END
            WHERE empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print("[OK] Resultado atualizado com sucesso.")
    except Exception as err:
        print(f"[ERRO] ao atualizar resultado: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização da atualização de resultado.")



