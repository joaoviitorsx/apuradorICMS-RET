import os
import re
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime
from ui.popupAliquota import PopupAliquota
from db.conexao import conectar_banco, fechar_banco
from PySide6.QtWidgets import QFileDialog, QMessageBox
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso
from utils.sanitizacao import is_aliquota_valida, atualizar_aliquotas_e_resultado
from services.spedService.atualizacoes import atualizar_aliquota, atualizar_resultado
from services.tributacaoService import atualizar_aliquota_c170_clone

def exportar_resultado(empresa_id, mes, ano, progress_bar):
    print(f"[DEBUG] Exportando resultado para {mes}/{ano} para empresa_id={empresa_id}")
    try:
        progress_bar.setValue(5)
        periodo = f"{mes}/{ano}"

        conexao = conectar_banco()
        if not conexao:
            mensagem_error("Não foi possível conectar ao banco de dados.")
            return
        cursor = conexao.cursor()

        cursor.execute("""
            SELECT codigo, produto, ncm 
            FROM cadastro_tributacao 
            WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
        """, (empresa_id,))
        produtos_nulos = cursor.fetchall()

        if produtos_nulos:
            popup = PopupAliquota(produtos_nulos, empresa_id)
            resultado = popup.exec()
            if resultado != 1:
                mensagem_aviso("Preenchimento de alíquotas cancelado.")
                return

        cursor.execute("""
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c 
            INNER JOIN `0150` f ON f.cod_part = c.cod_part 
            WHERE c.periodo = %s AND c.empresa_id = %s AND f.empresa_id = %s
        """, (periodo, empresa_id, empresa_id))
        dados = cursor.fetchall()

        if not dados:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        colunas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(dados, columns=colunas)

        for campo in ['resultado', 'vl_item', 'aliquota']:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)
        if 'aliquota' in df.columns:
            df['aliquota'] = df['aliquota'].apply(lambda x: x if is_aliquota_valida(x) else '')

        cursor.execute("SELECT razao_social FROM empresas WHERE id = %s", (empresa_id,))
        nome_empresa_result = cursor.fetchone()
        nome_empresa = nome_empresa_result[0] if nome_empresa_result else "empresa"

        cursor.execute("SELECT periodo, dt_ini, dt_fin FROM `0000` WHERE empresa_id = %s AND periodo = %s LIMIT 1", (empresa_id, periodo))
        resultado = cursor.fetchone()
        if not resultado:
            mensagem_error("Período não encontrado na tabela 0000.")
            return
        _, dt_ini, dt_fin = resultado

        sugestao_nome = f"{ano}-{mes}-{nome_empresa}.xlsx"
        caminho_arquivo, _ = QFileDialog.getSaveFileName(None, "Salvar Resultado", sugestao_nome, "Planilhas Excel (*.xlsx)")
        if not caminho_arquivo:
            mensagem_aviso("Exportação cancelada pelo usuário.")
            return

        progress_bar.setValue(60)

        dt_ini_fmt = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
        dt_fin_fmt = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
        periodo_legivel = f"Período: {dt_ini_fmt} a {dt_fin_fmt}"

        with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, startrow=2)
            sheet = writer.book.active
            sheet["A1"] = nome_empresa
            sheet["A2"] = periodo_legivel

        progress_bar.setValue(100)
        mensagem_sucesso(f"Tabela exportada com sucesso para:\n{caminho_arquivo}")

        abrir = QMessageBox.question(
            None, "Abrir Arquivo", "Deseja abrir a planilha exportada?", QMessageBox.Yes | QMessageBox.No
        )
        if abrir == QMessageBox.Yes:
            os.startfile(caminho_arquivo)

    except Exception as e:
        mensagem_error(f"Erro ao exportar: {e}")
    finally:
        try:
            cursor.close()
            fechar_banco(conexao)
        except:
            pass
