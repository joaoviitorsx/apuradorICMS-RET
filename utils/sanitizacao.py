import re

TAMANHOS_MAXIMOS = {
    'unid': 2,
    'cod_item': 60,
    'descr_item': 255,
    'descr_compl': 255,
    'cod_nat': 10,
    'cod_cta': 255,
    'cod_part': 60,
    'nome': 100,
}

def limpar_aliquota(valor):
    if not valor:
        return None
    valor = str(valor).strip().replace('%', '').replace(',', '.')
    try:
        num = float(valor)
        if num == 0:
            return '0%'
        return f"{num:.2f}%"
    except ValueError:
        valor_upper = valor.upper()
        if valor_upper in ["ST", "ISENTO", "PAUTA"]:
            return valor_upper
        return None
    
def truncar(valor, limite):
    if valor is None:
        return None
    valor_str = str(valor)
    if len(valor_str) > limite:
        print(f"[TRUNCAR] Valor '{valor_str}' excede {limite} chars → '{valor_str[:limite]}'")
    return valor_str[:limite]

def corrigir_unidade(valor):
    if not valor:
        return 'UN'
        
    valor_str = str(valor)
    
    if re.match(r'^\d+[,\.]\d+$', valor_str) or re.match(r'^\d+$', valor_str):
        print(f"[AJUSTE] Campo de unidade numérico: '{valor_str}' → 'UN'")
        return 'UN'
    
    match = re.match(r'^([A-Za-z]+)(\d+)', valor_str)
    if match:
        unidade_base = match.group(1)
        print(f"[AJUSTE] Unidade com número: '{valor_str}' → '{unidade_base}'")
        return unidade_base
    
    tamanho_banco = 3
    if len(valor_str) > tamanho_banco:
        print(f"[TRUNCAR] Unidade: '{valor_str}' → '{valor_str[:tamanho_banco]}'")
        return valor_str[:tamanho_banco]
    
    return valor_str

def corrigir_cst_icms(valor):
    if not valor:
        return "00"

    valor_str = str(valor).strip().replace(',', '.')

    if valor_str.replace('.', '').isdigit():
        try:
            return str(int(float(valor_str))).zfill(2)[:2]
        except ValueError:
            return "00"

    return valor_str[:2]

def corrigir_cfop(valor):
    if valor is None:
        return valor
    valor = re.sub(r'\D', '', valor)
    if len(valor) > 4:
        valor = valor[:4]
    if len(valor) < 4:
        valor = valor.zfill(4)
    return valor

def corrigir_ind_mov(valor):
    if not valor:
        return '0'
    
    valor_str = str(valor)
    if len(valor_str) > 1:
        print(f"[TRUNCAR] ind_mov: '{valor_str}' → '{valor_str[:1]}'")
        return valor_str[:1]
    
    return valor_str

def validar_estrutura_c170(dados):
    try:
        if not dados or len(dados) < 45:
            print(f"[DEBUG] C170 com estrutura insuficiente: {len(dados) if dados else 0} campos (esperado: 45 campos)")
            return False
        
        periodo = dados[0]    
        filial = dados[41]    
        num_doc = dados[43]   

        if not periodo:
            print("[DEBUG CRÍTICO] Periodo faltando.")
        if not filial:
            print("[DEBUG CRÍTICO] Filial faltando.")
        if not num_doc:
            print("[DEBUG CRÍTICO] Num_doc faltando.")

        if not (periodo and filial and num_doc):
            print(f"[DEBUG] C170 sem campos obrigatórios: periodo={periodo}, filial={filial}, num_doc={num_doc}")
            return False
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Falha ao validar C170: {e}")
        return False


def sanitizar_campo(campo, valor):
    regras = {
        'cod_item': lambda v: truncar(v, 60),
        'descr_item': lambda v: truncar(v, 255),
        'descr_compl': lambda v: truncar(v, 255),
        'unid_inv': corrigir_unidade,
        'unid': corrigir_unidade,
        'cod_part': lambda v: truncar(v, 60),
        'nome': lambda v: truncar(v, 100),
        'ind_mov': corrigir_ind_mov,
        'cod_mod': lambda v: str(v).zfill(2)[:2] if v is not None else '00',
        'cst_icms': corrigir_cst_icms,
        'cfop': corrigir_cfop,
        'cod_nat': lambda v: truncar(v, 10),
        'cod_cta': lambda v: truncar(v, 255),
        'reg': lambda v: truncar(v, 4),
        'vl_item': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'vl_desc': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'vl_merc': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_icms': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_ipi': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_pis': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_cofins': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
    }

    try:
        if campo in regras:
            novo_valor = regras[campo](valor)
            if novo_valor != valor:
                print(f"[SANITIZAR] {campo}: '{valor}' → '{novo_valor}'")
            return novo_valor
        return valor
    except Exception as e:
        print(f"[ERRO] sanitizar_campo({campo}) → {e}")
        return valor

def sanitizar_registro(registro_dict):
    return {campo: sanitizar_campo(campo, valor) for campo, valor in registro_dict.items()}

def get_column_index(column_name):
    indices = {
        'periodo': 0,
        'reg': 1,
        'num_item': 2,
        'cod_item': 3,
        'descr_compl': 4,
        'qtd': 5,
        'unid': 6,
        'vl_item': 7,
        'vl_desc': 8,
        'ind_mov': 9,
        'cst_icms': 10,
        'cfop': 11,
        'cod_nat': 12,
        'vl_bc_icms': 13,
        'aliq_icms': 14,
        'vl_icms': 15,
        'cod_cta': 37,
        'id_c100': 40,
        'filial': 41,
        'ind_oper': 42,
        'cod_part': 43,
        'num_doc': 44,
        'chv_nfe': 45,
    }
    if column_name not in indices:
        print(f"[AVISO] get_column_index: coluna '{column_name}' não mapeada.")
    return indices.get(column_name, -1)

def get_fallback_value(column_name):
    fallbacks = {
        'unid': 'UN',
        'ind_mov': '0',
        'cod_item': '0000',
        'descr_compl': '-',
        'cod_nat': '000',
        'cod_cta': '-',
        'cfop': '5102',
        'cst_icms': '00',
    }
    return fallbacks.get(column_name, None)

def get_fallback_value_by_index(index):
    index_to_column = {
        3: 'cod_item',
        4: 'descr_compl',
        6: 'unid',
        9: 'ind_mov',
        10: 'cst_icms',
        11: 'cfop',
        12: 'cod_nat',
        37: 'cod_cta',
    }
    column_name = index_to_column.get(index)
    return get_fallback_value(column_name) if column_name else None

def calcular_periodo(dt_ini_0000):
    return f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'

def is_aliquota_valida(valor: str) -> bool:
    if not isinstance(valor, str):
        return False

    padrao = r'^[0-2]?[0-9](,[0-9]{1,2})?%$'
    if not re.match(padrao, valor):
        return False

    try:
        num = float(valor.replace('%', '').replace(',', '.'))
        return 0 <= num <= 30
    except ValueError:
        return False

def atualizar_aliquotas_e_resultado(nome_banco):
    import asyncio
    from services.spedService.atualizacoes import atualizar_aliquota, atualizar_resultado
    from db.conexao import conectar_banco, fechar_banco

    async def _executar():
        conexao = conectar_banco(nome_banco)
        cursor = conexao.cursor()
        cursor.execute("SELECT dt_ini FROM `0000` ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        cursor.close()
        fechar_banco(conexao)

        periodo = f"{row[0][2:4]}/{row[0][4:]}" if row else "00/0000"
        print(f"[DEBUG] Período obtido para atualização: {periodo}")

        await atualizar_aliquota(nome_banco, periodo)
        await atualizar_resultado(nome_banco)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_executar())
        else:
            loop.run_until_complete(_executar())
    except RuntimeError:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(_executar())
        new_loop.close()
    except Exception as e:
        print(f"[ERRO] Falha ao atualizar alíquotas e resultados: {e}")


