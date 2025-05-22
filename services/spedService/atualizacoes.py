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

async def atualizar_aliquota(nome_banco):
    print("Atualizando alíquotas...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("SELECT dt_ini FROM `0000` ORDER BY id DESC LIMIT 1")
        dt_ini = cursor.fetchone()[0]
        ano = int(dt_ini[4:]) if dt_ini and len(dt_ini) >= 6 else 0
        coluna_origem = "aliquota" if ano >= 2024 else "aliquota_antiga"

        cursor.execute(f"""
            UPDATE c170_clone n
            JOIN cadastro_tributacao c ON n.cod_item = c.codigo
            SET n.aliquota = CONCAT(
                REPLACE(FORMAT(CAST(REPLACE(REPLACE(c.{coluna_origem}, '%', ''), ',', '.') AS DECIMAL(10, 2)), 2), '.', ','),
                '%'
            )
            WHERE n.aliquota IS NULL OR n.aliquota = ''
        """)
        conexao.commit()
        print("Tabela c170_clone atualizada com alíquotas da cadastro_tributacao.")

    except Exception as err:
        print(f"Erro ao atualizar alíquotas: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquota_simples(nome_banco):
    print("Atualizando alíquotas para Simples Nacional...")
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_fornecedores f ON f.cod_part = c.cod_part
            SET c.aliquota = CONCAT(
                REPLACE(FORMAT(
                    CAST(REPLACE(REPLACE(c.aliquota, '%', ''), ',', '.') AS DECIMAL(10, 2)) + 3.00,
                    2
                ), '.', ','),
                '%'
            )
            WHERE f.simples = 'Sim'
              AND c.aliquota REGEXP '^[0-9]+([.,][0-9]*)?%?$'
        """)
        conexao.commit()
        print("Tabela c170_clone atualizada com acréscimo de 3% para Simples Nacional.")
    except Exception as e:
        print(f"Erro ao atualizar alíquota simples: {e}")
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
                WHEN aliquota IS NULL OR aliquota = '' THEN 0
                ELSE CAST(REPLACE(REPLACE(vl_item, '.', ''), ',', '.') AS DECIMAL(10, 2)) *
                     (CAST(REPLACE(REPLACE(aliquota, '%', ''), ',', '.') AS DECIMAL(10, 4)) / 100)
            END
        """)
        conexao.commit()
        print("Tabela c170_clone atualizada com resultados calculados.")
    except Exception as err:
        print(f"Erro ao atualizar resultado: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)