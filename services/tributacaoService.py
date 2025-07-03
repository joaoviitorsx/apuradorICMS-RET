import pandas as pd
import unicodedata
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectarBanco, fecharBanco
from utils.aliquota import formatar_aliquota
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso
from ui.popupAliquota import PopupAliquota

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms'],
    'RET': ['ret', 'retencao', 'ret_icms', 'valor_ret', 'ret_icms_valor', 'rt', 'Ret']
}

def normalizar_texto(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode().lower().replace('_', '').replace(' ', '')

def mapear_colunas(df):
    colunas_encontradas = {}

    for coluna_padrao, sinonimos in COLUNAS_SINONIMAS.items():
        for nome in sinonimos:
            for col in df.columns:
                if col.strip().lower() == nome.lower():
                    colunas_encontradas[coluna_padrao] = col
                    break
            if coluna_padrao in colunas_encontradas:
                break

    colunas_necessarias = ['CODIGO', 'PRODUTO', 'NCM', 'ALIQUOTA', 'RET']
    if all(col in colunas_encontradas for col in colunas_necessarias):
        return colunas_encontradas

    mensagem_error(f"Erro: Colunas esperadas não encontradas. Colunas atuais: {df.columns.tolist()}")
    return None

def enviar_tributacao(empresa_id, progress_bar):
    conexao = conectarBanco()
    progress_bar.setValue(0)

    filename, _ = QFileDialog.getOpenFileName(None, "Enviar Tributação", "", "Arquivos Excel (*.xlsx)")
    if not filename:
        mensagem_aviso("Nenhum arquivo selecionado.")
        return

    progress_bar.setValue(10)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        return

    progress_bar.setValue(20)

    try:
        df = pd.read_excel(filename, dtype=str)
        
        mapeamento = mapear_colunas(df)
        if not mapeamento:
            mensagem_error("Não foi possível identificar as colunas necessárias na planilha.")
            return
        
        df = df.rename(columns=mapeamento)

        col_aliquota = mapeamento['ALIQUOTA']
        df[col_aliquota] = df[col_aliquota].fillna('').astype(str).str.strip().apply(formatar_aliquota)
        
        col_aliquotaRT = mapeamento['RET']
        df[col_aliquotaRT] = df[col_aliquotaRT].fillna('').astype(str).str.strip()
        df[col_aliquotaRT] = df[col_aliquotaRT].apply(lambda x: formatar_aliquota(x) if x.strip() else '')

        df_inserir = df[[mapeamento['CODIGO'], mapeamento['PRODUTO'], mapeamento['NCM'], mapeamento['ALIQUOTA'], mapeamento['RET']]].copy()
        df_inserir['empresa_id'] = empresa_id

        cursor = conexao.cursor()

        cursor.execute("""
            SELECT codigo, produto, ncm, aliquota, aliquotaRET FROM cadastro_tributacao
            WHERE empresa_id = %s
        """, (empresa_id,))
        registros_existentes = {
            (str(c).strip(), str(p).strip(), str(n).strip()): (str(a).strip(), str(r).strip())
            for c, p, n, a, r in cursor.fetchall()
        }

        novos_registros = []
        atualizacoes = []
        
        total_linhas = 0
        linhas_com_ret = 0

        for _, linha in df_inserir.iterrows():
            total_linhas += 1
            codigo = str(linha[mapeamento['CODIGO']]).strip()
            produto = str(linha[mapeamento['PRODUTO']]).strip()
            ncm = str(linha[mapeamento['NCM']]).strip()
            aliquota = str(linha[mapeamento['ALIQUOTA']]).strip()
            aliquotaRET = str(linha[mapeamento['RET']]).strip()
            
            if aliquotaRET:
                linhas_com_ret += 1
            
            aliquota_upper = aliquota.upper()
            if any(caso in aliquota_upper for caso in ['ST', 'ISENTO', 'PAUTA']):
                aliquotaRET = aliquota
            elif not aliquotaRET and aliquota:
                try:
                    valor = aliquota.upper().replace('%', '').replace(',', '.').strip()
                    float(valor)
                    aliquotaRET = aliquota
                except ValueError:
                    pass

            aliquota_str = aliquota.upper().replace('%', '').replace(',', '.').strip()
            if aliquota_str in ["ISENTO", "ST", "SUBSTITUICAO", "0", "0.00"]:
                categoria = 'ST'
            else:
                try:
                    aliquota_num = float(aliquota_str)
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
                except ValueError:
                    categoria = 'regraGeral'

            chave = (codigo, produto, ncm)
            aliquota_existente, aliquotaRET_existente = registros_existentes.get(chave, (None, None))

            if chave not in registros_existentes:
                novos_registros.append((empresa_id, codigo, produto, ncm, aliquota, aliquotaRET, categoria))
            elif aliquota_existente != aliquota or aliquotaRET_existente != aliquotaRET:
                atualizacoes.append((aliquota, aliquotaRET, empresa_id, codigo, produto, ncm))

        print(f"[INFO] Processando {total_linhas} produtos ({linhas_com_ret} com RET informado)")

        if novos_registros:
            cursor.executemany("""
                INSERT INTO cadastro_tributacao (empresa_id, codigo, produto, ncm, aliquota, aliquotaRET, categoria_fiscal)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, novos_registros)
            print(f"[INFO] {len(novos_registros)} novos produtos cadastrados")

        if atualizacoes:
            cursor.executemany("""
                UPDATE cadastro_tributacao
                SET aliquota = %s, aliquotaRET = %s
                WHERE empresa_id = %s AND codigo = %s AND produto = %s AND ncm = %s
            """, atualizacoes)
            print(f"[INFO] {len(atualizacoes)} produtos atualizados")

        cursor.execute("""
            UPDATE cadastro_tributacao
            SET aliquotaRET = aliquota
            WHERE empresa_id = %s 
            AND (aliquotaRET IS NULL OR aliquotaRET = '')
            AND aliquota IS NOT NULL 
            AND aliquota != ''
            AND aliquota NOT IN ('ST', 'ISENTO', 'PAUTA')
        """, (empresa_id,))
        
        rows_affected = cursor.rowcount
        if rows_affected > 0:
            print(f"[INFO] {rows_affected} produtos receberam RET igual à alíquota")

        cursor.execute("""
            UPDATE cadastro_tributacao 
            SET aliquota_antiga = CASE
                WHEN aliquota = '1.54%' THEN '1.40%'
                WHEN aliquota = '2.63%' THEN '2.40%'
                WHEN aliquota = '4%' THEN '3.60%'
                WHEN aliquota = '4.00%' THEN '3.60%'
                WHEN aliquota = '8.13%' THEN '8.13%'
                WHEN aliquota = 'ST' THEN 'ST'
                WHEN aliquota = 'ISENTO' THEN 'ISENTO'
                WHEN aliquota = 'PAUTA' THEN 'PAUTA'
                ELSE 'N/A'
            END
            WHERE empresa_id = %s
        """, (empresa_id,))

        cursor.execute("""
            SELECT COUNT(*) FROM cadastro_tributacao
            WHERE empresa_id = %s AND aliquotaRET != ''
        """, (empresa_id,))
        count_ret = cursor.fetchone()[0]
        print(f"[INFO] Total de produtos com RET preenchido: {count_ret}")

        conexao.commit()
        progress_bar.setValue(100)
        total = len(novos_registros) + len(atualizacoes)
        mensagem_sucesso(f"Tributação enviada com sucesso! Total: {total} registros.")

    except Exception as e:
        mensagem_error(f"Erro ao processar o arquivo: {str(e)}")
        conexao.rollback()
    finally:
        progress_bar.setValue(0)