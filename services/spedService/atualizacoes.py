from db.conexao import conectar_banco, fechar_banco
from utils.aliquota_uf import obter_aliquota, identificar_categoria
from utils.siglas import obter_sigla_estado
from utils.aliquota import formatar_aliquota

def formatar_aliquota(valor):
    try:
        valor_str = str(valor).replace('%', '').strip()
        valor_float = float(valor_str.replace(',', '.'))
        return f"{valor_float:,.2f}%".replace(",", "v").replace(".", ",").replace("v", ".")
    except:
        return '0,00%'

async def atualizar_aliquota(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando alíquotas em c170_clone a partir de cadastro_tributacao...")

    conexao = conectar_banco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                n.id AS id_c170,
                t.aliquota
            FROM c170_clone n
            JOIN cadastro_tributacao t
              ON n.descr_compl = t.produto AND n.ncm = t.ncm AND n.empresa_id = t.empresa_id
            WHERE n.empresa_id = %s
              AND (n.aliquota IS NULL OR n.aliquota = '')
              AND (t.aliquota IS NOT NULL AND t.aliquota != '')
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)
        print(f"[INFO] {total} registros a atualizar...")

        atualizacoes = []

        for r in registros:
            aliquota = r['aliquota']
            atualizacoes.append((
                aliquota,
                r['id_c170']
            ))

        for i in range(0, total, lote_tamanho):
            lote = atualizacoes[i:i + lote_tamanho]

            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s
                WHERE id = %s
            """, lote)

            conexao.commit()
            print(f"[OK] Lote {i // lote_tamanho + 1} atualizado com {len(lote)} itens.")

        print(f"[FINALIZADO] Alíquotas atualizadas em {total} registros.")

    except Exception as err:
        print(f"[ERRO] ao atualizar alíquotas: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)

async def atualizar_aliquotaRET(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando aliquotaRET na c170_clone a partir de cadastro_tributacao...")

    conexao = conectar_banco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                n.id AS id_c170,
                t.aliquotaRET
            FROM c170_clone n
            JOIN cadastro_tributacao t
              ON n.descr_compl = t.produto AND n.ncm = t.ncm AND n.empresa_id = t.empresa_id
            WHERE n.empresa_id = %s
              AND (n.aliquotaRET IS NULL OR n.aliquotaRET = '')
              AND (t.aliquotaRET IS NOT NULL AND t.aliquotaRET != '')
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)
        print(f"[INFO] {total} registros a atualizar...")

        atualizacoes = []

        for r in registros:
            aliquotaRET = r['aliquotaRET']
            atualizacoes.append((
                aliquotaRET,
                r['id_c170']
            ))

        for i in range(0, total, lote_tamanho):
            lote = atualizacoes[i:i + lote_tamanho]

            cursor.executemany("""
                UPDATE c170_clone
                SET aliquotaRET = %s
                WHERE id = %s
            """, lote)

            conexao.commit()
            print(f"[OK] Lote {i // lote_tamanho + 1} atualizado com {len(lote)} itens.")

        print(f"[FINALIZADO] aliquotaRET atualizada em {total} registros.")

    except Exception as err:
        print(f"[ERRO] ao atualizar aliquotaRET: {err}")
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
    print("[INÍCIO] Atualizando resultado")
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


async def atualizar_resultadoRET(empresa_id):
    print("[INÍCIO] Atualizando resultadoRET")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        cursor.execute("""
            UPDATE c170_clone
            SET resultadoRET = CASE
                WHEN aliquotaRET IS NULL OR TRIM(aliquotaRET) = '' THEN 0
                WHEN aliquotaRET RLIKE '^[A-Za-z]' THEN 0
                ELSE
                    ROUND(
                        (
                            CAST(REPLACE(REPLACE(IFNULL(vl_item, '0'), '.', ''), ',', '.') AS DECIMAL(15,4)) -
                            CAST(REPLACE(REPLACE(IFNULL(NULLIF(vl_desc, ''), '0'), '.', ''), ',', '.') AS DECIMAL(15,4))
                        ) *
                        (CAST(REPLACE(REPLACE(aliquotaRET, '%', ''), ',', '.') AS DECIMAL(15,4)) / 100),
                        2
                    )
            END
            WHERE empresa_id = %s
        """, (empresa_id,))
        conexao.commit()
        print("[OK] ResultadoRET atualizado com sucesso.")
    except Exception as err:
        print(f"[ERRO] ao atualizar resultadoRET: {err}")
        conexao.rollback()
    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Finalização da atualização de resultadoRET.")

def calcular_aliquota_ret(uf_origem, uf_destino='23'):
    if uf_origem == uf_destino:
        return 0

    aliquota_interna = obter_aliquota(uf_destino, 'regra_geral')
    aliquota_interestadual = 0.07 if uf_origem in ['35', '31', '33'] else 0.12

    aliquota_ret = aliquota_interna - aliquota_interestadual
    return round(aliquota_ret, 4) if aliquota_ret > 0 else 0
