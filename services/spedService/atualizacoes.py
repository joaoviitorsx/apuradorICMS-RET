from db.conexao import conectar_banco, fechar_banco

async def atualizar_ncm(nome_banco):
    print("Atualizando NCM...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone n
            JOIN `0200` c ON n.cod_item = c.cod_item
            SET n.ncm = c.cod_ncm
            WHERE n.ncm IS NULL OR n.ncm = ''
        """)
        conexao.commit()
        print("Tabela c170_clone atualizada com NCMs da tabela 0200.")
    except Exception as err:
        print(f"Erro ao atualizar NCM: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota(nome_banco,periodo):
    print("Atualizando alíquotas com validação...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("SELECT dt_ini FROM `0000` ORDER BY id DESC LIMIT 1")
        dt_ini = cursor.fetchone()[0]
        ano = int(dt_ini[4:]) if dt_ini and len(dt_ini) >= 6 else 0
        coluna_origem = "aliquota" if ano >= 2024 else "aliquota_antiga"
        print(f"[DEBUG] Coluna usada para alíquota: {coluna_origem}")

        cursor.execute(f"""
            UPDATE c170_clone n
            JOIN cadastro_tributacao c ON n.cod_item = c.codigo
            SET n.aliquota = c.{coluna_origem}
            WHERE c.{coluna_origem} IN ('1.54%', '4.00%', '8.13%', 'ST', 'ISENTO')
        """)
        print(f"{cursor.rowcount} registros atualizados com nova alíquota.")

        cursor.execute("""
            SELECT cod_item, aliquota FROM c170_clone
            WHERE periodo = %s AND cod_item IN (
                SELECT codigo FROM cadastro_tributacao
                WHERE aliquota IN ('1.54%', '4.00%', '8.13%', 'ST', 'ISENTO')
            )
            LIMIT 10
        """, (periodo,))
        print("[DEBUG] c170_clone após atualização de alíquota:")
        for linha in cursor.fetchall():
            print(f" - Item: {linha[0]} | Alíquota: {linha[1]}")

        conexao.commit()
        print("Alíquotas válidas aplicadas à tabela c170_clone.")

    except Exception as err:
        print(f"Erro ao atualizar alíquotas: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota_simples(nome_banco, periodo):
    print("Iniciando atualização de alíquotas para Simples Nacional...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) FROM c170_clone c
            JOIN cadastro_fornecedores f ON f.cod_part = c.cod_part
            WHERE c.periodo = %s
              AND f.simples = 'Sim'
              AND c.aliquota REGEXP '^[0-9]+([.,][0-9]*)?%?$'
              AND CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) <= 20.00
        """, (periodo,))
        count = cursor.fetchone()[0]
        print(f"[DEBUG] Registros encontrados para ajuste: {count}")

        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_fornecedores f ON f.cod_part = c.cod_part
            SET c.aliquota = CASE
                WHEN 
                    CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) + 3.00 <= 30.00
                THEN CONCAT(
                    REPLACE(FORMAT(
                        CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) + 3.00,
                        2
                    ), '.', ','), '%')
                ELSE c.aliquota
            END
            WHERE c.periodo = %s
              AND f.simples = 'Sim'
              AND c.aliquota REGEXP '^[0-9]+([.,][0-9]*)?%?$'
              AND CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) <= 20.00
        """, (periodo,))
        conexao.commit()
        print(f"Conclusão: {count} atualizados (caso tenham passado no filtro).")
    except Exception as e:
        print(f"ERRO GERAL: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_resultado(nome_banco):
    print("Atualizando resultado...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            UPDATE c170_clone
            SET resultado = CASE
                WHEN aliquota REGEXP '^[A-Za-z]' THEN 0
                WHEN aliquota IS NULL OR TRIM(aliquota) = '' THEN 0
                ELSE CAST(REPLACE(REPLACE(vl_item, '.', ''), ',', '.') AS DECIMAL(10, 2)) *
                     (CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(10, 4)) / 100)
            END
        """)
        conexao.commit()
        print("Resultados calculados com base em vl_item e aliquota.")
    except Exception as err:
        print(f"Erro ao atualizar resultado: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
