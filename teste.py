import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import unicodedata
from utils.siglas import obter_sigla_estado
from utils.sanitizacao import truncar, corrigir_unidade, corrigir_ind_mov, corrigir_cst_icms, TAMANHOS_MAXIMOS, calcular_periodo

UNIDADE_PADRAO = "UN"

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms']
}

def limpar_aliquota(valor):
    if not valor:
        return None
    valor = valor.replace('%', '').replace(',', '.').strip()
    try:
        num = float(valor)
        if 0 <= num <= 30:
            return str(round(num, 2)).replace('.', ',') + '%'
    except ValueError:
        pass
    return None

def normalizar_texto(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode().lower().replace('_', '').replace(' ', '')

def mapear_colunas(df):
    colunas_encontradas = {}
    colunas_atuais = [col.lower().strip() for col in df.columns]

    for coluna_padrao, sinonimos in COLUNAS_SINONIMAS.items():
        for nome in sinonimos:
            for col in df.columns:
                if col.strip().lower() == nome.lower():
                    colunas_encontradas[coluna_padrao] = col
                    break
            if coluna_padrao in colunas_encontradas:
                break

    colunas_necessarias = ['CODIGO', 'PRODUTO', 'NCM', 'ALIQUOTA']
    if all(col in colunas_encontradas for col in colunas_necessarias):
        return colunas_encontradas

    messagebox.showerror("Erro", f"Colunas obrigatórias não encontradas na planilha. Colunas atuais: {df.columns.tolist()}")
    return None

def carregar_planilha_tributacao():
    caminho = filedialog.askopenfilename(title="Planilha de Tributação", filetypes=[("Arquivos Excel", "*.xlsx")])
    if not caminho:
        messagebox.showinfo("Aviso", "Nenhum arquivo selecionado.")
        return None
    df = pd.read_excel(caminho, dtype=str)
    mapeamento = mapear_colunas(df)
    if not mapeamento:
        return None
    df.rename(columns=mapeamento, inplace=True)
    print("[DEBUG] Colunas após renomear:", df.columns.tolist())
    return df

def processar_sped_para_dataframes(conteudo):
    linhas = conteudo.split('\n')
    dados_c170 = []
    dt_ini_0000 = None
    filial = None
    ind_oper = cod_part = num_doc = chv_nfe = None
    registros_processados = set()

    for linha in linhas:
        if not linha.strip():
            continue
        partes = linha.split('|')[1:-1]

        if linha.startswith("|0000|"):
            partes += [None] * (15 - len(partes))
            dt_ini_0000 = partes[3]
            cnpj = partes[6]
            filial = cnpj[8:12] if cnpj else '0000'

        elif linha.startswith("|C100|"):
            partes += [None] * (29 - len(partes))
            ind_oper, cod_part, num_doc, chv_nfe = partes[1], partes[4], partes[7], partes[9]

        elif linha.startswith("|C170|"):
            while len(partes) <= 38:
                partes.append(None)
            if len(partes) < 39:
                continue

            partes[10] = corrigir_cst_icms(partes[10])
            partes[6] = truncar(corrigir_unidade(partes[6]), TAMANHOS_MAXIMOS['unid'])
            partes[9] = corrigir_ind_mov(partes[9])
            partes[2] = truncar(partes[2], TAMANHOS_MAXIMOS['cod_item'])
            partes[4] = truncar(partes[4], TAMANHOS_MAXIMOS['descr_compl'])
            partes[12] = truncar(partes[12], TAMANHOS_MAXIMOS['cod_nat'])
            partes[37] = truncar(partes[37], TAMANHOS_MAXIMOS['cod_cta'])

            registro_id = f"{filial}_{num_doc}_{partes[2]}"
            if registro_id in registros_processados:
                continue

            dados = [calcular_periodo(dt_ini_0000), *partes[:39], None, filial, ind_oper, cod_part, num_doc, chv_nfe]
            if len(dados) != 45:
                continue

            dados_c170.append(dados)
            registros_processados.add(registro_id)

    df_c170 = pd.DataFrame(dados_c170, columns=[
        'periodo', 'reg', 'num_item', 'cod_item', 'descr_compl', 'qtd', 'unid', 'vl_item', 'vl_desc',
        'ind_mov', 'cst_icms', 'cfop', 'cod_nat', 'vl_bc_icms', 'aliq_icms', 'vl_icms', 'vl_bc_icms_st',
        'aliq_st', 'vl_icms_st', 'ind_apur', 'cst_ipi', 'cod_enq', 'vl_bc_ipi', 'aliq_ipi', 'vl_ipi',
        'cst_pis', 'vl_bc_pis', 'aliq_pis', 'quant_bc_pis', 'aliq_pis_reais', 'vl_pis', 'cst_cofins',
        'vl_bc_cofins', 'aliq_cofins', 'quant_bc_cofins', 'aliq_cofins_reais', 'vl_cofins', 'cod_cta',
        'vl_abat_nt', 'id_c100', 'filial', 'ind_oper', 'cod_part', 'num_doc', 'chv_nfe'
    ])

    return df_c170

def cruzar_dados(df_c170, df_tributacao):
    df_tributacao['CODIGO'] = df_tributacao['CODIGO'].astype(str).str.strip()
    df_c170['cod_item'] = df_c170['cod_item'].astype(str).str.strip()
    df_merged = df_c170.merge(df_tributacao[['CODIGO', 'ALIQUOTA']], how='left', left_on='cod_item', right_on='CODIGO')
    return df_merged

def preencher_aliquotas_nulas(df):
    nulos = df[df['ALIQUOTA'].isna()]
    for idx, row in nulos.iterrows():
        root = tk.Tk()
        root.withdraw()
        resposta = simpledialog.askstring("Alíquota faltante", f"Código: {row['cod_item']}\nProduto: {row['descr_compl']}")
        if resposta:
            df.at[idx, 'ALIQUOTA'] = limpar_aliquota(resposta)
    return df

def calcular_resultado(df):
    df['vl_item_float'] = df['vl_item'].str.replace(',', '.').astype(float)
    df['aliquota_float'] = df['ALIQUOTA'].str.replace('%', '').str.replace(',', '.').astype(float)
    df['resultado'] = df['vl_item_float'] * (df['aliquota_float'] / 100)
    return df

def exportar_para_excel(df):
    caminho = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Planilha Excel", "*.xlsx")])
    if not caminho:
        return
    df.to_excel(caminho, index=False)
    messagebox.showinfo("Sucesso", f"Planilha exportada para:\n{caminho}")

def fluxo_completo():
    root = tk.Tk()
    root.withdraw()
    caminho_sped = filedialog.askopenfilename(title="Selecione o arquivo SPED", filetypes=[("Arquivos TXT", "*.txt")])
    if not caminho_sped:
        messagebox.showinfo("Aviso", "Nenhum arquivo SPED selecionado.")
        return

    with open(caminho_sped, 'r', encoding='utf-8', errors='ignore') as f:
        conteudo = f.read()

    df_c170 = processar_sped_para_dataframes(conteudo)
    df_tributacao = carregar_planilha_tributacao()
    if df_tributacao is None:
        return

    df_cruzado = cruzar_dados(df_c170, df_tributacao)
    df_completado = preencher_aliquotas_nulas(df_cruzado)
    df_final = calcular_resultado(df_completado)

    exportar_para_excel(df_final)

if __name__ == '__main__':
    fluxo_completo()
