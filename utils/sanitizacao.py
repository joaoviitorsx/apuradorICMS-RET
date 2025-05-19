import re

TAMANHOS_MAXIMOS = {
    'unid': 10,
    'cod_item': 60,
    'descr_item': 255,
    'descr_compl': 255,
    'cod_nat': 10,
    'cod_cta': 255,
    'cod_part': 60,
    'nome': 100,
}

def truncar(valor, limite):
    if valor is None:
        return None
    valor_str = str(valor)
    if len(valor_str) > limite:
        print(f"[TRUNCAR] Valor '{valor_str}' excede {limite} chars → '{valor_str[:limite]}'")
    return valor_str[:limite]

def corrigir_unidade(valor):
    if isinstance(valor, str) and (
        re.match(r'^\d+[,\.]\d+$', valor) or re.match(r'^\d+$', valor)):
        print(f"[AJUSTE] Campo de unidade numérico: '{valor}' → 'UN'")
        return 'UN'
    return valor

def corrigir_cst_icms(valor):
    if not valor:
        return "00"
    
    valor_str = str(valor).strip()
    
    if '.' in valor_str or ',' in valor_str:
        if valor_str.replace('.', '').replace(',', '').isdigit():
            if float(valor_str.replace(',', '.')) == 0:
                return "00"
            return valor_str[:2]
    
    if valor_str.isdigit():
        if len(valor_str) <= 2:
            return valor_str.zfill(2)
        return valor_str[:2]
    
    return valor_str[:2]

def corrigir_ind_mov(valor):
    if valor is None:
        return valor
        
    if isinstance(valor, str):
        valor_ajustado = valor.replace(',', '.')
        
        try:
            num = float(valor_ajustado)
            
            resultado = f"{num:.2f}"
            
            if len(resultado) > 5:
                resultado = resultado[:5]
                if resultado[-1] not in '0123456789':
                    resultado = resultado[:-1]
                    
            print(f"[AJUSTE] ind_mov: '{valor}' → '{resultado}'")
            return resultado
            
        except ValueError:
            if len(valor) > 5:
                print(f"[TRUNCAR] ind_mov não numérico: '{valor}' → '{valor[:5]}'")
                return valor[:5]
    return valor

def validar_estrutura_c170(dados):
    if len(dados) < 11:
        return False
    
    try:
        cst = dados[10]
        return cst is not None and len(str(cst)) <= 2 and str(cst).isdigit()
    except Exception:
        return False

def corrigir_cst_icms(valor):
    if not valor:
        return "00"
    
    valor_str = str(valor).strip()
    
    if '.' in valor_str:
        if valor_str.replace('.', '').isdigit():
            return valor_str[:5]
    
    if valor_str.isdigit():
        if len(valor_str) <= 2:
            return valor_str.zfill(2) 
        return valor_str[:5]
    
    return valor_str[:5]


def corrigir_cfop(valor):
    if valor is None:
        return valor
    valor = re.sub(r'\D', '', valor)
    if len(valor) > 4:
        valor = valor[:4]
    if len(valor) < 4:
        valor = valor.zfill(4)
    return valor

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
        'vl_item': lambda v: v if isinstance(v, (int, float, type(None))) else str(v).replace(',', '.'),
        'vl_desc': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
        'vl_merc': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
        'aliq_icms': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
        'aliq_ipi': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
        'aliq_pis': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
        'aliq_cofins': lambda v: v if isinstance(v, (int, float, type(None))) else v.replace(',', '.'),
    }

    if campo in regras:
        return regras[campo](valor)
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
        'aliq_icms': 14,
        'cod_cta': 37,
        'filial': 41,
        'cod_part': 43,
        # outros se necessário
    }
    return indices.get(column_name, -1)


def get_fallback_value(column_name):
    fallbacks = {
        'unid': 'U',
        'ind_mov': '0',
        'cod_item': '0',
        'descr_compl': '-',
        'cod_nat': '0',
        'cod_cta': '-',
    }
    return fallbacks.get(column_name, None)

def get_fallback_value_by_index(index):
    index_to_column = {
        3: 'cod_item',
        4: 'descr_compl',
        6: 'unid',
        9: 'ind_mov',
        12: 'cod_nat',
        37: 'cod_cta',
    }
    column_name = index_to_column.get(index)
    if column_name:
        return get_fallback_value(column_name)
    return None

def calcular_periodo(dt_ini_0000):
    return f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
