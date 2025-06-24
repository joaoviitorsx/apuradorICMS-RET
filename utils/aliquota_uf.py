from db.conexao import conectar_banco, fechar_banco
from utils.aliquota import formatar_aliquota

def definir_tipo_regime(uf_origem, decreto):
    uf = uf_origem.upper().strip()

    if uf == 'CE':
        if decreto.strip().lower() == 'sim':
            return 'decreto'
        else:
            return 'termo'
    else:
        return 'termo'


def obter_aliquota(uf_origem, categoria, decreto='Não'):
    uf = uf_origem.upper().strip()
    tipo_regime = definir_tipo_regime(uf, decreto)

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT regra_geral, cesta_basica_7, cesta_basica_12, bebida_alcoolica
            FROM cadastroAliquotaTermo
            WHERE uf = %s
        """, (uf,))
        dados = cursor.fetchone()

        if not dados:
            return 0

        mapeamento = {
            'regra_geral': dados[0],
            'cesta_basica_7': dados[1],
            'cesta_basica_12': dados[2],
            'bebida_alcoolica': dados[3],
        }

        return mapeamento.get(categoria, 0)

    except Exception as e:
        print(f"[ERRO] ao obter alíquota: {e}")
        return 0
    finally:
        cursor.close()
        fechar_banco(conexao)


def identificar_categoria(uf_origem, aliquota_informada, decreto='Não'):
    uf = uf_origem.upper().strip()
    tipo_regime = definir_tipo_regime(uf, decreto)

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT regra_geral, cesta_basica_7, cesta_basica_12, bebida_alcoolica
            FROM cadastroAliquotaTermo
            WHERE uf = %s
        """, (uf,))
        dados = cursor.fetchone()

        if not dados:
            return None

        mapeamento = {
            'regra_geral': dados[0],
            'cesta_basica_7': dados[1],
            'cesta_basica_12': dados[2],
            'bebida_alcoolica': dados[3],
        }

        for nome, valor in mapeamento.items():
            if abs(float(valor) - float(aliquota_informada)) <= 0.0001:
                return nome

        return None

    except Exception as e:
        print(f"[ERRO] ao identificar categoria: {e}")
        return None

    finally:
        cursor.close()
        fechar_banco(conexao)

def preencherAliquotaRET(empresa_id, lote_tamanho=5000):
    print("Atualizando categoria fiscal e aliquotaRET...")

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT produto, ncm, aliquota
            FROM cadastro_tributacao
            WHERE empresa_id = %s
        """, (empresa_id,))
        registros = cursor.fetchall()

        if not registros:
            print("[INFO] Nenhum registro encontrado para atualizar.")
            return

        atualizacoes = []

        for produto, ncm, aliquota_bruta in registros:
            if not produto or not ncm or not aliquota_bruta:
                print(f"[AVISO] Dados incompletos para Produto: {produto}, NCM: {ncm}. Pulando...")
                continue

            aliquota_str = str(aliquota_bruta).strip().upper()

            if aliquota_str in ['ISENTO', 'ST', 'N/A', 'PAUTA']:
                if aliquota_str in ['ISENTO', 'ST']:
                    categoria = 'ST'
                else:
                    categoria = aliquota_str

                aliquota_ret = aliquota_str

            else:
                try:
                    aliquota_num = float(aliquota_str.replace('%', '').replace(',', '.'))
                except ValueError:
                    print(f"[ERRO] Alíquota inválida para Produto: {produto}, NCM: {ncm}. Pulando...")
                    continue

                cursor.execute("""
                    SELECT COALESCE(f.uf, 'CE')
                    FROM cadastro_fornecedores f
                    JOIN cadastro_tributacao t
                    ON t.produto = %s AND t.ncm = %s AND t.empresa_id = f.empresa_id
                    WHERE t.empresa_id = %s
                    LIMIT 1
                """, (produto, ncm, empresa_id))
                resultado_uf = cursor.fetchone()

                uf_origem = resultado_uf[0].strip().upper() if resultado_uf else 'CE'

                cursor.execute("""
                    SELECT regra_geral, cesta_basica_7, cesta_basica_12, bebida_alcoolica
                    FROM cadastroAliquotaTermo
                    WHERE uf = %s
                """, (uf_origem,))
                dados_uf = cursor.fetchone()

                if not dados_uf:
                    print(f"[ERRO] Nenhuma configuração de alíquotas encontrada para UF: {uf_origem}")
                    continue

                regra_geral, cesta7, cesta12, bebida = dados_uf

                mapeamento = {
                    'regra_geral': regra_geral,
                    'cesta_basica_7': cesta7,
                    'cesta_basica_12': cesta12,
                    'bebida_alcoolica': bebida
                }

                categoria = None
                for nome, valor in mapeamento.items():
                    if abs(float(valor) - aliquota_num) <= 0.01:
                        categoria = nome
                        break

                if not categoria:
                    categoria = 'regra_geral'

                aliquota_ret_num = mapeamento.get(categoria, 0)
                aliquota_ret = f"{aliquota_ret_num:.2f}%".replace('.', ',')

            atualizacoes.append((
                categoria,
                aliquota_ret,
                produto,
                ncm,
                empresa_id
            ))

            if len(atualizacoes) >= lote_tamanho:
                cursor.executemany("""
                    UPDATE cadastro_tributacao
                    SET categoria_fiscal = %s, aliquotaRET = %s
                    WHERE produto = %s AND ncm = %s AND empresa_id = %s
                """, atualizacoes)
                conexao.commit()
                print(f"[LOTE] {len(atualizacoes)} registros atualizados.")
                atualizacoes = []

        if atualizacoes:
            cursor.executemany("""
                UPDATE cadastro_tributacao
                SET categoria_fiscal = %s, aliquotaRET = %s
                WHERE produto = %s AND ncm = %s AND empresa_id = %s
            """, atualizacoes)
            conexao.commit()
            print(f"[FINALIZADO] {len(atualizacoes)} registros atualizados no lote final.")
        else:
            print("[INFO] Nenhuma atualização pendente.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Erro ao atualizar aliquotaRET e categoria: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)


