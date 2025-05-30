from db.conexao import conectar_banco, fechar_banco

async def atualizar_ncm(empresa_id):
    print("[INÍCIO] Atualizando NCM em c170_clone...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone n
            JOIN `0200` c 
              ON n.cod_item = c.cod_item AND c.empresa_id = %s
            SET n.ncm = c.cod_ncm
            WHERE n.empresa_id = %s AND (n.ncm IS NULL OR n.ncm = '')
              AND c.cod_ncm IS NOT NULL AND c.cod_ncm != ''
        """, (empresa_id, empresa_id))
        conexao.commit()
        print(f"[OK] NCM atualizado.")
    except Exception as err:
        print(f"[ERRO] ao atualizar NCM: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota(empresa_id):
    print("[INÍCIO] Atualizando alíquotas em c170_clone (por lote)...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        # Verificar tamanho da coluna aliquota
        cursor.execute("SHOW COLUMNS FROM c170_clone LIKE 'aliquota'")
        column_info = cursor.fetchone()
        max_length = int(column_info[1].split('(')[1].split(')')[0])

        # Descobrir coluna correta com base no ano
        cursor.execute("SELECT dt_ini FROM `0000` WHERE empresa_id = %s ORDER BY id DESC LIMIT 1", (empresa_id,))
        row = cursor.fetchone()
        if not row or not row[0]:
            print("[AVISO] Nenhum dt_ini encontrado. Cancelando.")
            return

        ano = int(row[0][4:]) if len(row[0]) >= 6 else 0
        coluna = "aliquota" if ano >= 2024 else "aliquota_antiga"

        # Buscar as combinações cod_item + chv_nfe e as alíquotas da tributação
        cursor.execute(f"""
            SELECT n.cod_item, n.chv_nfe, c.{coluna}
            FROM c170_clone n
            JOIN cadastro_tributacao c ON n.cod_item = c.codigo AND c.empresa_id = %s
            WHERE n.empresa_id = %s
              AND (n.aliquota IS NULL OR n.aliquota = '')
              AND c.{coluna} IS NOT NULL AND c.{coluna} != ''
        """, (empresa_id, empresa_id))

        registros = cursor.fetchall()
        if not registros:
            print("[INFO] Nenhum registro a atualizar.")
            return

        # Preparar updates
        updates = []
        for cod_item, chv_nfe, aliquota in registros:
            aliquota_limpa = str(aliquota)[:max_length]
            updates.append((aliquota_limpa, cod_item, chv_nfe, empresa_id))

        # Aplicar updates em lotes
        update_sql = """
            UPDATE c170_clone
            SET aliquota = %s
            WHERE cod_item = %s AND chv_nfe = %s AND empresa_id = %s
        """
        batch_size = 500
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            cursor.executemany(update_sql, batch)
            conexao.commit()

        print(f"[OK] Alíquotas atualizadas com '{coluna}' para empresa_id {empresa_id}. Registros afetados: {len(updates)}")

    except Exception as err:
        print(f"[ERRO] ao atualizar alíquotas: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota_simples(empresa_id, periodo):
    print("[INÍCIO] Atualizando alíquotas Simples Nacional...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_fornecedores f 
              ON f.cod_part = c.cod_part AND f.empresa_id = %s
            SET c.aliquota = CONCAT(
                REPLACE(FORMAT(
                    LEAST(
                        CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10,2)) + 3.00,
                        30.00
                    ), 2
                ), '.', ','), '%')
            WHERE c.periodo = %s AND c.empresa_id = %s
              AND f.simples = 'Sim'
              AND c.aliquota RLIKE '^[0-9]+([.,][0-9]*)?%?$'
              AND CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) <= 20.00
        """, (empresa_id, periodo, empresa_id))
        conexao.commit()
        print("[OK] Alíquotas do Simples atualizadas.")
    except Exception as e:
        print(f"[ERRO] ao atualizar alíquota Simples: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_resultado(empresa_id):
    print("[INÍCIO] Atualizando resultado em c170_clone...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone
            SET resultado = CASE
                WHEN aliquota IS NULL OR aliquota IN ('', ' ') THEN 0
                WHEN aliquota RLIKE '^[A-Za-z]' THEN 0
                ELSE
                    ROUND(
                        CAST(REPLACE(REPLACE(vl_item, '.', ''), ',', '.') AS DECIMAL(10,2)) *
                        (CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(10,4)) / 100),
                        2
                    )
            END
            WHERE empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print("[OK] Resultado atualizado.")
    except Exception as err:
        print(f"[ERRO] ao atualizar resultado: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização da atualização de resultado.")

