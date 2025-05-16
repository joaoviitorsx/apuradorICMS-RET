import pandas as pd
from datetime import datetime
from pathlib import Path
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso

def exportar_resultado(nome_banco, mes, ano, progress_bar):
    progress_bar.setValue(0)

    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Não foi possível conectar ao banco de dados.")
        return

    try:
        periodo = f"{mes}/{ano}"

        query_dados = f"""
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c
            INNER JOIN `0150` f ON f.cod_part = c.cod_part
            WHERE c.periodo = '{periodo}'
        """
        df = pd.read_sql_query(query_dados, conexao)

        if df.empty:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        for campo in ['resultado', 'vl_item', 'aliquota']:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)

        cursor = conexao.cursor()
        cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
        cnpj = cursor.fetchone()[0]

        cursor.execute("""
            SELECT periodo, dt_ini, dt_fin FROM `0000`
            WHERE periodo = %s LIMIT 1
        """, (periodo,))
        resultado = cursor.fetchone()
        if not resultado:
            mensagem_error("Período não encontrado na tabela 0000.")
            return
        periodo_str, dt_ini, dt_fin = resultado

        dt_ini_fmt = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
        dt_fin_fmt = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
        periodo_legivel = f"Período: {dt_ini_fmt} a {dt_fin_fmt}"

        conexao_emp = conectar_banco("empresas_db")
        cursor_emp = conexao_emp.cursor()
        cursor_emp.execute("""
            SELECT razao_social FROM empresas
            WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1
        """, (cnpj,))
        razao_result = cursor_emp.fetchone()
        if not razao_result:
            mensagem_error("Empresa não encontrada na base principal.")
            return
        nome_empresa = razao_result[0]

        periodo_mes = f"{ano}-{mes}"
        sugestao_nome = f"{periodo_mes}-{nome_empresa}.xlsx"
        caminho_arquivo, _ = QFileDialog.getSaveFileName(None, "Salvar Resultado", sugestao_nome, "Planilhas Excel (*.xlsx)")

        if not caminho_arquivo:
            mensagem_aviso("Salvamento cancelado pelo usuário.")
            return

        progress_bar.setValue(60)

        with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, startrow=2)
            sheet = writer.book.active
            sheet['A1'] = nome_empresa
            sheet['A2'] = periodo_legivel

        progress_bar.setValue(100)
        mensagem_sucesso(f"Tabela exportada com sucesso para:\n{caminho_arquivo}")

    except Exception as e:
        mensagem_error(f"Erro ao exportar a tabela: {e}")

    finally:
        try:
            cursor.close()
            fechar_banco(conexao)
            cursor_emp.close()
            fechar_banco(conexao_emp)
        except:
            pass

def baixar_tabela_com_mes_ano(nome_banco, mes, ano, progress_bar):
    try:
        progress_bar.setValue(5)
        conexao = conectar_banco(nome_banco)
        if not conexao:
            mensagem_error("Não foi possível conectar ao banco de dados.")
            return

        periodo_consulta = f"{mes}/{ano}"
        query = """
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c 
            INNER JOIN `0150` f ON f.cod_part = c.cod_part 
            WHERE c.periodo = %s
        """
        df = pd.read_sql_query(query, conexao, params=(periodo_consulta,))
        if df.empty:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        for campo in ['resultado', 'vl_item', 'aliquota']:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)

        cursor = conexao.cursor()
        cursor.execute("SELECT dt_ini, dt_fin FROM `0000` WHERE periodo = %s LIMIT 1", (periodo_consulta,))
        resultado = cursor.fetchone()
        if not resultado:
            mensagem_error("Período não encontrado na tabela 0000.")
            return
        dt_ini, dt_fin = resultado

        cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
        cnpj = cursor.fetchone()[0]

        conexao_emp = conectar_banco("empresas_db")
        cursor_emp = conexao_emp.cursor()
        cursor_emp.execute("SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1", (cnpj,))
        nome_empresa = cursor_emp.fetchone()[0]

        progress_bar.setValue(50)

        caminho_arquivo, _ = QFileDialog.getSaveFileName(
            None,
            "Salvar Arquivo Excel",
            f"{mes}-{ano}-{nome_empresa}.xlsx",
            "Arquivos Excel (*.xlsx)"
        )

        if not caminho_arquivo:
            mensagem_aviso("Exportação cancelada pelo usuário.")
            return

        dt_ini_str = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
        dt_fin_str = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
        periodo_legivel = f"Período: {dt_ini_str} a {dt_fin_str}"

        with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, startrow=2)
            sheet = writer.book.active
            sheet['A1'] = nome_empresa
            sheet['A2'] = periodo_legivel

        progress_bar.setValue(100)
        mensagem_sucesso(f"Tabela exportada com sucesso para:\n{caminho_arquivo}")

    except Exception as e:
        mensagem_error(f"Erro ao exportar planilha: {e}")
    finally:
        try:
            cursor.close()
            fechar_banco(conexao)
            cursor_emp.close()
            fechar_banco(conexao_emp)
        except:
            pass