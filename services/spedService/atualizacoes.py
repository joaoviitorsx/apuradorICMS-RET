from db.conexao import conectar_banco, fechar_banco

async def atualizar_aliquota(empresa_id):
    print("[INÍCIO] Atualizando alíquotas em c170_clone...")

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("SELECT dt_ini FROM `0000` WHERE empresa_id = %s ORDER BY id DESC LIMIT 1", (empresa_id,))
        row = cursor.fetchone()
        if not row or not row[0]:
            print("[AVISO] Nenhum dt_ini encontrado. Cancelando.")
            return

        ano = int(row[0][4:]) if len(row[0]) >= 6 else 0
        coluna = "aliquota" if ano >= 2024 else "aliquota_antiga"

        count_sql = f"""
            SELECT COUNT(*)
            FROM c170_clone n
            JOIN cadastro_tributacao c 
            ON n.cod_item = c.codigo AND c.empresa_id = n.empresa_id
            WHERE n.empresa_id = %s
            AND (n.aliquota IS NULL OR n.aliquota = '')
            AND c.{coluna} IS NOT NULL AND c.{coluna} != ''
        """
        cursor.execute(count_sql, (empresa_id,))
        quantidade = cursor.fetchone()[0]
        print(f"[INFO] Registros que seriam atualizados: {quantidade}")

        update_sql = f"""
            UPDATE c170_clone n
            JOIN cadastro_tributacao c 
              ON n.cod_item = c.codigo AND c.empresa_id = n.empresa_id
            SET n.aliquota = LEFT(c.{coluna}, 10)
            WHERE n.empresa_id = %s
              AND (n.aliquota IS NULL OR n.aliquota = '')
              AND c.{coluna} IS NOT NULL AND c.{coluna} != ''
        """
        cursor.execute(update_sql, (empresa_id,))
        conexao.commit()

        print(f"[OK] Alíquotas atualizadas com '{coluna}' para empresa_id {empresa_id}. Linhas afetadas: {cursor.rowcount}")

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

