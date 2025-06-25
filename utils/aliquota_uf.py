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

        for produto, ncm, aliquota in registros:
            if not produto or not ncm or not aliquota:
                print(f"[AVISO] Dados incompletos: Produto={produto}, NCM={ncm}")
                continue

            aliquota_str = str(aliquota).strip().upper()

            if aliquota_str in ["ISENTO", "ST", "SUBSTITUICAO", "0", "0.00", "0,00"]:
                categoria = 'ST'
            else:
                try:
                    aliquota_num = float(aliquota_str.replace('%', '').replace(',', '.'))
                except:
                    print(f"[ERRO] Alíquota inválida: {aliquota_str} | Produto={produto}, NCM={ncm}")
                    continue

                if aliquota_num in [17.00, 12.00, 4.00]:
                    categoria = 'regraGeral'
                elif aliquota_num in [5.95, 4.20, 1.54]:
                    categoria = '7cestaBasica'
                elif aliquota_num in [10.20, 7.20, 2.63]:
                    categoria = '12cestaBasica'
                elif aliquota_num in [37.80, 30.39, 8.13]:
                    categoria = 'bebidaAlcoolica'
                else:
                    categoria = 'regraGeral'

            cursor.execute("""
                SELECT f.uf
                FROM cadastro_fornecedores f
                JOIN c170nova c ON c.cod_part = f.cod_part
                WHERE c.descr_compl = %s AND c.empresa_id = %s
                LIMIT 1
            """, (produto, empresa_id))
            resultado_uf = cursor.fetchone()
            uf_origem = resultado_uf[0].strip().upper() if resultado_uf else 'CE'

            if categoria == 'ST':
                aliquota_ret = '0,00%'
            else:
                colunas_validas = {
                    'regraGeral': 'regra_geral',
                    '7cestaBasica': 'cesta_basica_7',
                    '12cestaBasica': 'cesta_basica_12',
                    'bebidaAlcoolica': 'bebida_alcoolica'
                }
                coluna_aliquota = colunas_validas.get(categoria)

                if not coluna_aliquota:
                    print(f"[ERRO] Categoria inválida: {categoria}")
                    continue

                query = f"""
                    SELECT {coluna_aliquota}
                    FROM cadastroAliquotaTermo
                    WHERE uf = %s
                """
                cursor.execute(query, (uf_origem,))
                resultado = cursor.fetchone()

                if not resultado:
                    print(f"[ERRO] UF {uf_origem} não encontrada para categoria {categoria}.")
                    continue

                aliquota_ret_num = resultado[0]
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
            print(f"[FINALIZADO] {len(atualizacoes)} registros atualizados.")
        else:
            print("[INFO] Nenhuma atualização pendente.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Erro ao atualizar: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)



