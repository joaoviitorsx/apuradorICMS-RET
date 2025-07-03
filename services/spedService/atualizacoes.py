from db.conexao import conectarBanco, fecharBanco
from utils.conversao import Conversor

async def atualizarAliquota(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando alíquotas em c170_clone por lotes...")

    conexao = conectarBanco()
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
        fecharBanco(conexao)

async def atualizarAliquotaRET(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando aliquotaRET na c170_clone a partir de cadastro_tributacao")

    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                n.id AS id_c170,
                t.aliquotaRET
            FROM c170_clone n
            JOIN cadastro_tributacao t
                ON n.descr_compl = t.produto 
                AND n.ncm = t.ncm 
                AND n.empresa_id = t.empresa_id
            WHERE n.empresa_id = %s
                AND (n.aliquotaRET IS NULL OR n.aliquotaRET = '')
                AND (t.aliquotaRET IS NOT NULL AND t.aliquotaRET != '')
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)
        print(f"[INFO] {total} registros a atualizar")

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
        fecharBanco(conexao)

async def aliquotaSimples(empresa_id, periodo):
    print("[INÍCIO] Atualizando alíquotas Simples Nacional")
    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT c.id, c.aliquota, c.aliquotaRET, c.descr_compl, c.cod_part
            FROM c170_clone c
            JOIN cadastro_fornecedores f 
                ON f.cod_part = c.cod_part AND f.empresa_id = %s
            WHERE c.periodo = %s AND c.empresa_id = %s
              AND f.simples = 'Sim'
        """, (empresa_id, periodo, empresa_id))

        registros = cursor.fetchall()
        atualizacoes = []

        for row in registros:
            aliquota_str = str(row.get('aliquota') or '').strip().upper()
            aliquotaRET_str = str(row.get('aliquotaRET') or '').strip().upper()
            
            if aliquota_str in ['ST', 'ISENTO', 'PAUTA', ''] or aliquotaRET_str in ['ST', 'ISENTO', 'PAUTA']:
                continue

            try:
                aliquota = Conversor(row['aliquota'])
                aliquotaRET = Conversor(row['aliquotaRET'] or aliquota)
                
                nova_aliquota = round(aliquota + 3, 2)
                nova_aliquotaRET = round(aliquotaRET + 3, 2)

                aliquota_str = f"{nova_aliquota:.2f}".replace('.', ',') + '%'
                aliquotaRET_str = f"{nova_aliquotaRET:.2f}".replace('.', ',') + '%'

                atualizacoes.append((aliquota_str, aliquotaRET_str, row['id']))
                
            except Exception as e:
                print(f"[AVISO] Erro ao processar registro {row['id']}: {e}")

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s, aliquotaRET = %s
                WHERE id = %s
            """, atualizacoes)

            conexao.commit()

    except Exception as e:
        print(f"[ERRO] ao atualizar alíquota Simples: {e}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Finalização da atualização de alíquota Simples.")

async def atualizarResultado(empresa_id):
    print("[INÍCIO] Atualizando resultado")
    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, vl_item, vl_desc, aliquota 
            FROM c170_clone
            WHERE empresa_id = %s
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)

        atualizacoes = []

        for row in registros:
            vl_item = Conversor(row['vl_item'])
            vl_desc = Conversor(row['vl_desc'])
            aliquota = Conversor(row['aliquota'])

            resultado = round((vl_item - vl_desc) * (aliquota / 100), 2)

            atualizacoes.append((resultado, row['id']))

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET resultado = %s
                WHERE id = %s
            """, atualizacoes)

            conexao.commit()
            print(f"[OK] Resultado atualizado para {total} registros.")

    except Exception as err:
        print(f"[ERRO] ao atualizar resultado: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Finalização da atualização de resultado.")

async def atualizarResultadoRET(empresa_id):
    print("[INÍCIO] Atualizando resultadoRET")
    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, vl_item, vl_desc, aliquotaRET 
            FROM c170_clone
            WHERE empresa_id = %s
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)

        atualizacoes = []

        for row in registros:
            vl_item = Conversor(row['vl_item'])
            vl_desc = Conversor(row['vl_desc'])
            aliquotaRET = Conversor(row['aliquotaRET'])

            resultadoRET = round((vl_item - vl_desc) * (aliquotaRET / 100), 2)

            atualizacoes.append((resultadoRET, row['id']))

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET resultadoRET = %s
                WHERE id = %s
            """, atualizacoes)

            conexao.commit()
            print(f"[OK] ResultadoRET atualizado para {total} registros.")

    except Exception as err:
        print(f"[ERRO] ao atualizar resultadoRET: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Finalização da atualização de resultadoRET.")

async def aplicarDecreto(empresa_id):
    conexao = conectarBanco()
    cursor = conexao.cursor()

    print(f"[INÍCIO] Correção de alíquotas com base na tabela decreto para empresa_id={empresa_id}...")

    try:
        cursor.execute("""
            SELECT c.cod_item, c.descr_compl, c.aliquota, f.uf, t.categoria_fiscal
            FROM c170_clone c
            JOIN cadastro_fornecedores f ON c.cod_part = f.cod_part
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = c.empresa_id
            WHERE c.empresa_id = %s AND f.uf <> 'CE' AND t.categoria_fiscal IS NOT NULL
        """, (empresa_id,))
        registros = cursor.fetchall()

        atualizacoes = []

        for cod_item, produto, aliquota_str, uf, categoria in registros:
            if not aliquota_str:
                continue

            aliquota_normalizada = aliquota_str.strip().upper()

            if aliquota_normalizada in ['ST', 'ISENTO', 'PAUTA']:
                print(f"[RET ESPECIAL] Produto={produto} | UF={uf} | Alíquota={aliquota_normalizada}")
                atualizacoes.append((
                    aliquota_normalizada,
                    empresa_id,
                    cod_item
                ))
                continue

            try:
                aliquota_atual = float(aliquota_normalizada.replace('%', '').replace(',', '.'))
            except:
                print(f"[ERRO] Alíquota inválida para {produto}: {aliquota_str}")
                continue

            coluna_decreto = {
                'regraGeral': 'regra_geral',
                '7cestaBasica': 'cesta_basica_7',
                '12cestaBasica': 'cesta_basica_12',
                'bebidaAlcoolica': 'bebida_alcoolica'
            }.get(categoria)

            if not coluna_decreto:
                print(f"[AVISO] Categoria fiscal não reconhecida: {categoria}")
                continue

            cursor.execute(f"""
                SELECT {coluna_decreto}
                FROM decreto
                WHERE LOWER(uf) = LOWER(%s)
            """, (uf,))
            resultado = cursor.fetchone()

            if not resultado:
                print(f"[ERRO] UF {uf} não encontrada na tabela decreto.")
                continue

            aliquota_esperada = float(resultado[0])

            if round(aliquota_atual, 2) != round(aliquota_esperada, 2):
                print(f"[CORRIGINDO] Produto={produto} | UF={uf} | Cat={categoria} | Atual={aliquota_atual} | Esperado={aliquota_esperada}")
                atualizacoes.append((
                    f"{aliquota_esperada:.2f}%",
                    empresa_id,
                    cod_item
                ))

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s
                WHERE empresa_id = %s AND cod_item = %s
            """, atualizacoes)
            conexao.commit()
            print(f"[OK] {len(atualizacoes)} alíquotas atualizadas com sucesso.")
        else:
            print("[INFO] Nenhuma divergência encontrada.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Durante execução da função de correção: {e}")
    finally:
        cursor.close()
        fecharBanco(conexao)




