from db.conexao import conectar_banco, fechar_banco
from utils.aliquota import formatar_aliquota

aliquotas_por_uf = {
    'AC':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'Al':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'AP':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'AM':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'BA':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'CE': {
        'regra_geral': 0.04,
        'cesta_basica_7': 0.0154,
        'cesta_basica_12': 0.0263,
        'bebida_alcoolica': 0.0813,

        'regra_geral_termo': 0.0408,
        'cesta_basica_7_termo': 0.0219,
        'cesta_basica_12_termo': 0.0299,
        'bebida_alcoolica_termo': 0.0478,
    },
    'DF':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'ES':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'GO':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'MA':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'MS':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'MT':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'MG':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'PA':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'PB':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'PE':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'PI':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'PR':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'RN':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'RS':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'RJ':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'RO':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'RR':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'SC':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'SP':{
        'regra_geral': 0.17,
        'cesta_basica_7': 0.0595,
        'cesta_basica_12':0.1020,
        'bebida alcoolica':0.3780,

        'regra_geral_termo': 0.1096,
        'cesta_basica_7_termo': 0.0512,
        'cesta_basica_12_termo': 0.0658,
        'bebida_alcoolica_termo': 0.2468
    },
    'SE':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
    'TO':{
        'regra_geral': 0.12,
        'cesta_basica_7': 0.042,
        'cesta_basica_12':0.072,
        'bebida alcoolica':0.3039,

        'regra_geral_termo': 0.0831,
        'cesta_basica_7_termo': 0.0416,
        'cesta_basica_12_termo': 0.0512,
        'bebida_alcoolica_termo': 0.1984
    },
}


def definir_tipo_regime(uf_origem, decreto):
    print("definindo tipo de regime")
    uf = uf_origem.upper().strip()

    if uf == 'CE':
        if decreto.strip().lower() == 'sim':
            return 'decreto'
        else:
            return 'termo'
    else:
        return 'termo'


def obter_aliquota(uf_origem, tipo, decreto='Não'):
    print("obtendo aliquota")
    uf = uf_origem.upper().strip()
    tipo_regime = definir_tipo_regime(uf, decreto)

    if tipo_regime == 'termo':
        chave = tipo + '_termo'
    else:
        chave = tipo

    dados_uf = aliquotas_por_uf.get(uf)
    if not dados_uf:
        return 0

    return dados_uf.get(chave, 0)

def identificar_categoria(uf_origem, aliquota_informada, decreto='Não'):
    print("identificando categoria")
    uf = uf_origem.upper().strip()
    tipo_regime = definir_tipo_regime(uf, decreto)

    dados_uf = aliquotas_por_uf.get(uf)
    if not dados_uf:
        return None

    categorias = ['regra_geral', 'cesta_basica_7', 'cesta_basica_12', 'bebida_alcoolica']

    sufixo = '_termo' if tipo_regime == 'termo' else ''

    for categoria in categorias:
        chave = categoria + sufixo
        valor = dados_uf.get(chave)

        if valor is not None and round(valor, 4) == round(aliquota_informada, 4):
            return categoria
    return None

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
                continue

            aliquota_str = str(aliquota_bruta).strip().upper()

            if aliquota_str in ['ISENTO', 'ST', 'N/A', 'PAUTA']:
                categoria = aliquota_str
                aliquota_ret = aliquota_str
            else:
                try:
                    aliquota_num = float(aliquota_str.replace('%', '').replace(',', '.'))
                except:
                    continue

                uf_origem = 'CE'

                cursor.execute("""
                    SELECT regra_geral, cesta_basica_7, cesta_basica_12, bebida_alcoolica
                    FROM cadastroAliquotaTermo
                    WHERE uf = %s
                """, (uf_origem,))
                dados_uf = cursor.fetchone()

                if not dados_uf:
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
                    if abs(valor - aliquota_num) <= 0.01:
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
            print("[INFO] Nenhuma atualização necessária.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Erro ao atualizar aliquotaRET e categoria: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)

