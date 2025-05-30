import re
import asyncio
from services.spedService.atualizacoes import atualizar_aliquota, atualizar_resultado
from db.conexao import conectar_banco, fechar_banco

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
        return ''
    valor = str(valor).strip().replace('%', '').replace(',', '.')
    try:
        num = float(valor)
        return f"{num:.2f}%" if num > 0 else '0%'
    except ValueError:
        valor_upper = valor.upper()
        if valor_upper in {"ST", "ISENTO", "PAUTA"}:
            return valor_upper
        print(f"[AVISO] Valor de alíquota inválido: '{valor}'")
        return ''

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
    if re.match(r'^\d+[,\.]\d+$', valor_str) or valor_str.isdigit():
        return 'UN'
    match = re.match(r'^([A-Za-z]+)(\d+)', valor_str)
    if match:
        return match.group(1)
    return valor_str[:3] if len(valor_str) > 3 else valor_str

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
    valor = re.sub(r'\D', '', str(valor))
    return valor[:4].zfill(4)

def corrigir_ind_mov(valor):
    if not valor:
        return '0'
    valor_str = str(valor)
    if len(valor_str) > 1:
        print(f"[TRUNCAR] ind_mov: '{valor_str}' → '{valor_str[:1]}'")
    return valor_str[:1]

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

async def atualizar_aliquotas_e_resultado(empresa_id):
    conexao = conectar_banco()
    cursor = conexao.cursor()
    cursor.execute("""
        SELECT dt_ini FROM `0000`
        WHERE empresa_id = %s
        ORDER BY id DESC LIMIT 1
    """, (empresa_id,))
    row = cursor.fetchone()
    cursor.close()
    fechar_banco(conexao)

    if row and isinstance(row[0], str) and len(row[0]) >= 6:
        dt_ini = row[0]
        periodo = f"{dt_ini[2:4]}/{dt_ini[4:]}"
        print(f"[DEBUG] Período obtido: {periodo}")
        await atualizar_aliquota(empresa_id)
        await atualizar_resultado(empresa_id)
    else:
        print("[INFO] Nenhum período válido encontrado. SPED ainda não foi processado.")

def executar_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(coro)
        else:
            loop.run_until_complete(coro)
    except RuntimeError:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        new_loop.run_until_complete(coro)
        new_loop.close()
