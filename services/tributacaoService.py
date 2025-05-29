import pandas as pd
import unicodedata
from PySide6.QtWidgets import QFileDialog, QMessageBox
from db.conexao import conectar_banco, fechar_banco
from utils.aliquota import formatar_aliquota
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso
from ui.popupAliquota import PopupAliquota
from utils.sanitizacao import atualizar_aliquotas_e_resultado

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms']
}

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

    mensagem_error(f"Erro: Colunas esperadas não encontradas. Colunas atuais: {df.columns.tolist()}")
    return None

import pandas as pd
import unicodedata
from PySide6.QtWidgets import QFileDialog, QMessageBox
from db.conexao import conectar_banco, fechar_banco
from utils.aliquota import formatar_aliquota
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso
from ui.popupAliquota import PopupAliquota
from utils.sanitizacao import atualizar_aliquotas_e_resultado

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms']
}

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

    mensagem_error(f"Erro: Colunas esperadas não encontradas. Colunas atuais: {df.columns.tolist()}")
    return None

def enviar_tributacao(empresa_id, progress_bar):
    from PySide6.QtWidgets import QFileDialog
    conexao = conectar_banco()
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

        print("[DEBUG] Colunas mapeadas:", mapeamento)
        df = df.rename(columns=mapeamento)

        col_aliquota = mapeamento['ALIQUOTA']
        df[col_aliquota] = df[col_aliquota].fillna('').astype(str).str.strip().apply(formatar_aliquota)

        df_inserir = df[[mapeamento['CODIGO'], mapeamento['PRODUTO'], mapeamento['NCM'], mapeamento['ALIQUOTA']]].copy()
        df_inserir['empresa_id'] = empresa_id

        cursor = conexao.cursor()

        cursor.execute("SELECT codigo FROM cadastro_tributacao WHERE empresa_id = %s", (empresa_id,))
        codigos_existentes = set(row[0] for row in cursor.fetchall())

        novos_registros = []
        atualizacoes = []

        for _, linha in df_inserir.iterrows():
            codigo = str(linha[mapeamento['CODIGO']]).strip()
            produto = str(linha[mapeamento['PRODUTO']]).strip()
            ncm = str(linha[mapeamento['NCM']]).strip()
            aliquota = str(linha[mapeamento['ALIQUOTA']]).strip()
            empresa = empresa_id

            dados = (empresa, codigo, produto, ncm, aliquota)

            if codigo not in codigos_existentes:
                novos_registros.append(dados)
            else:
                atualizacoes.append(dados)

        if novos_registros:
            cursor.executemany("""
                INSERT INTO cadastro_tributacao (empresa_id, codigo, produto, ncm, aliquota)
                VALUES (%s, %s, %s, %s, %s)
            """, novos_registros)
            print(f"[DEBUG] {len(novos_registros)} novos registros inseridos.")

        if atualizacoes:
            cursor.executemany("""
                UPDATE cadastro_tributacao
                SET produto = %s, ncm = %s, aliquota = %s
                WHERE codigo = %s AND empresa_id = %s
            """, [(p, n, a, c, e) for (e, c, p, n, a) in atualizacoes])
            print(f"[DEBUG] {len(atualizacoes)} registros atualizados.")

        # Atualiza coluna de alíquota antiga
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

        conexao.commit()
        progress_bar.setValue(100)
        total = len(novos_registros) + len(atualizacoes)
        mensagem_sucesso(f"Tributação enviada com sucesso! Total: {total} registros.")
        print(f"[DEBUG] Total final processado: {total}")

    except Exception as e:
        mensagem_error(f"Erro ao processar o arquivo: {str(e)}")
        conexao.rollback()
    finally:
        progress_bar.setValue(0)
        cursor.close()
        fechar_banco(conexao)

def atualizar_aliquota_c170_clone(empresa_id, periodo=None):
    print("[EXPORTAÇÃO] Atualizando alíquotas na c170_clone...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        if periodo:
            cursor.execute("""
                UPDATE c170_clone c
                JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = %s
                SET c.aliquota = t.aliquota
                WHERE t.aliquota IS NOT NULL 
                  AND TRIM(t.aliquota) <> '' 
                  AND c.periodo = %s AND c.empresa_id = %s
            """, (empresa_id, periodo, empresa_id))
        else:
            cursor.execute("""
                UPDATE c170_clone c
                JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = %s
                SET c.aliquota = t.aliquota
                WHERE t.aliquota IS NOT NULL 
                  AND TRIM(t.aliquota) <> '' AND c.empresa_id = %s
            """, (empresa_id, empresa_id))

        conexao.commit()
        print("[EXPORTAÇÃO] Alíquotas atualizadas com sucesso.")
    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] ao atualizar alíquotas: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)

def atualizar_aliquota_c170_clone(empresa_id, periodo=None):
    print("[EXPORTAÇÃO] Atualizando alíquotas na c170_clone...")
    conexao = conectar_banco()
    cursor = conexao.cursor()
    try:
        if periodo:
            cursor.execute("""
                UPDATE c170_clone c
                JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = %s
                SET c.aliquota = t.aliquota
                WHERE t.aliquota IS NOT NULL 
                  AND TRIM(t.aliquota) <> '' 
                  AND c.periodo = %s AND c.empresa_id = %s
            """, (empresa_id, periodo, empresa_id))
        else:
            cursor.execute("""
                UPDATE c170_clone c
                JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND t.empresa_id = %s
                SET c.aliquota = t.aliquota
                WHERE t.aliquota IS NOT NULL 
                  AND TRIM(t.aliquota) <> '' AND c.empresa_id = %s
            """, (empresa_id, empresa_id))

        conexao.commit()
        print("[EXPORTAÇÃO] Alíquotas atualizadas com sucesso.")
    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] ao atualizar alíquotas: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)
