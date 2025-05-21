import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from ui.popupAliquota import PopupAliquota
from db.conexao import conectar_banco, fechar_banco
from PySide6.QtWidgets import QFileDialog, QMessageBox
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso

def exportar_resultado(nome_banco, mes, ano, progress_bar):
    try:
        progress_bar.setValue(5)
        periodo = f"{mes}/{ano}"

        conexao = conectar_banco(nome_banco)
        if not conexao:
            mensagem_error("Não foi possível conectar ao banco de dados.")
            return

        cursor = conexao.cursor()

        cursor.execute("""
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c 
            INNER JOIN `0150` f ON f.cod_part = c.cod_part 
            WHERE c.periodo = %s
        """, (periodo,))
        dados = cursor.fetchall()

        if not dados:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        colunas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(dados, columns=colunas)

        cursor.execute("SELECT codigo, produto, ncm FROM cadastro_tributacao WHERE aliquota IS NULL OR TRIM(aliquota) = ''")
        produtos_nulos = cursor.fetchall()

        if produtos_nulos:
            popup = PopupAliquota(produtos_nulos, nome_banco)
            resultado = popup.exec()
            if resultado != 1:
                mensagem_aviso("Preenchimento de alíquotas cancelado.")
                return

        for campo in ['resultado', 'vl_item', 'aliquota']:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)

        cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
        cnpj = cursor.fetchone()[0]

        cursor.execute("SELECT periodo, dt_ini, dt_fin FROM `0000` WHERE periodo = %s LIMIT 1", (periodo,))
        resultado = cursor.fetchone()
        if not resultado:
            mensagem_error("Período não encontrado na tabela 0000.")
            return
        _, dt_ini, dt_fin = resultado

        conexao_emp = conectar_banco("empresas_db")
        cursor_emp = conexao_emp.cursor()
        cursor_emp.execute("SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1", (cnpj,))
        razao_result = cursor_emp.fetchone()
        if not razao_result:
            mensagem_error("Empresa não encontrada na base principal.")
            return
        nome_empresa = razao_result[0]

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
            None,
            "Abrir Arquivo",
            "Deseja abrir a planilha exportada?",
            QMessageBox.Yes | QMessageBox.No
        )
        if abrir == QMessageBox.Yes:
            os.startfile(caminho_arquivo)

    except Exception as e:
        mensagem_error(f"Erro ao exportar: {e}")

    finally:
        try:
            cursor.close()
            fechar_banco(conexao)
            cursor_emp.close()
            fechar_banco(conexao_emp)
        except:
            pass
