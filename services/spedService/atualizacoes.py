from db.conexao import conectar_banco, fechar_banco

async def atualizar_ncm(empresa_id):
    print("Atualizando NCM")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone n
            JOIN `0200` c ON n.cod_item = c.cod_item AND c.empresa_id = %s
            SET n.ncm = c.cod_ncm
            WHERE (n.ncm IS NULL OR n.ncm = '') AND n.empresa_id = %s
        """, (empresa_id, empresa_id))
        conexao.commit()
        print("[OK] c170_clone atualizada com NCMs da 0200.")
    except Exception as err:
        print(f"[ERRO] ao atualizar NCM: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)


async def atualizar_aliquota(empresa_id):
    print("Atualizando alíquotas em c170")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("SHOW COLUMNS FROM c170 LIKE 'aliquota'")
        column_info = cursor.fetchone()
        max_length = int(column_info[1].split('(')[1].split(')')[0])

        cursor.execute("""
            SELECT dt_ini FROM `0000` 
            WHERE empresa_id = %s 
            ORDER BY id DESC LIMIT 1
        """, (empresa_id,))
        row = cursor.fetchone()

        if not row or not row[0]:
            print(f"[WARN] Nenhum dt_ini encontrado para empresa_id={empresa_id}. Pulando atualização de alíquotas.")
            return

        dt_ini = row[0]
        ano = int(dt_ini[4:]) if len(dt_ini) >= 6 else 0
        print(f"[DEBUG] dt_ini: {dt_ini}, ano: {ano}")

        coluna = "aliquota" if ano >= 2024 else "aliquota_antiga"

        cursor.execute(f"""
            UPDATE c170 n
            JOIN cadastro_tributacao c 
              ON n.cod_item = c.codigo AND c.empresa_id = %s
            SET n.aliquota = LEFT(c.{coluna}, {max_length})
            WHERE (n.aliquota IS NULL OR TRIM(n.aliquota) = '') 
              AND n.empresa_id = %s
        """, (empresa_id, empresa_id))

        conexao.commit()
        print(f"[OK] Alíquotas atualizadas com '{coluna}' para empresa_id={empresa_id}.")

    except Exception as err:
        print(f"[ERRO] ao atualizar alíquotas: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota_simples(empresa_id, periodo):
    print("Atualizando alíquotas Simples Nacional")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) FROM c170_clone c
            JOIN cadastro_fornecedores f 
              ON f.cod_part = c.cod_part AND f.empresa_id = %s
            WHERE c.periodo = %s 
              AND c.empresa_id = %s
              AND f.simples = 'Sim'
              AND c.aliquota REGEXP '^[0-9]+([.,][0-9]*)?%?$'
              AND CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) <= 20.00
        """, (empresa_id, periodo, empresa_id))
        count = cursor.fetchone()[0]
        print(f"[DEBUG] Registros a atualizar: {count}")

        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_fornecedores f 
              ON f.cod_part = c.cod_part AND f.empresa_id = %s
            SET c.aliquota = CASE
                WHEN CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) + 3.00 <= 30.00
                THEN CONCAT(
                    REPLACE(FORMAT(
                        CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) + 3.00,
                        2
                    ), '.', ','), '%')
                ELSE c.aliquota
            END
            WHERE c.periodo = %s
              AND c.empresa_id = %s
              AND f.simples = 'Sim'
              AND c.aliquota REGEXP '^[0-9]+([.,][0-9]*)?%?$'
              AND CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) <= 20.00
        """, (empresa_id, periodo, empresa_id))
        conexao.commit()
        print(f"[OK] Alíquotas ajustadas para fornecedores do Simples.")
    except Exception as e:
        print(f"[ERRO] ao atualizar alíquota simples: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)


async def atualizar_resultado(empresa_id):
    print("[INÍCIO] Atualizando resultado em c170_clone")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            UPDATE c170_clone
            SET resultado = CASE
                WHEN aliquota IS NULL OR TRIM(aliquota) = '' THEN 0
                WHEN aliquota REGEXP '^[A-Za-z]' THEN 0
                ELSE 
                    CAST(REPLACE(REPLACE(vl_item, '.', ''), ',', '.') AS DECIMAL(10, 2)) *
                    (CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(10, 4)) / 100)
            END
            WHERE empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print(f"[OK] Resultado atualizado para empresa_id={empresa_id}.")
    except Exception as err:
        conexao.rollback()
        print(f"[ERRO] Falha ao atualizar resultado para empresa_id={empresa_id}: {err}")
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Atualização de resultado finalizada.")
