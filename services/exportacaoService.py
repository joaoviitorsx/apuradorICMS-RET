import pandas as pd
from pathlib import Path
from datetime import datetime
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso

def exportar_resultado(nome_banco, mes, ano, progress_bar):
    progress_bar.setValue(0)
    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Não foi possível conectar ao banco de dados.")
        return

    try:
        cursor = conexao.cursor()
        query = f"""
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c 
            INNER JOIN `0150` f ON f.cod_part = c.cod_part 
            WHERE c.periodo LIKE '{mes}/{ano}%'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        df['resultado'] = df['resultado'].astype(str).str.replace('.', ',')
        df['vl_item'] = df['vl_item'].astype(str).str.replace('.', ',')
        df['aliquota'] = df['aliquota'].astype(str).str.replace('.', ',')

        cursor.execute("SELECT cnpj FROM `0000` LIMIT 1")
        cnpj = cursor.fetchone()[0]

        cursor.execute("SELECT periodo FROM `0000` WHERE periodo LIKE %s LIMIT 1", (f"{mes}/{ano}%",))
        periodo_raw = cursor.fetchone()[0]
        periodo_mes = f"{periodo_raw[3:]}-{periodo_raw[:2]}"

        conexao_empresa = conectar_banco("empresas_db")
        cursor_emp = conexao_empresa.cursor()
        cursor_emp.execute("""
            SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1
        """, (cnpj,))
        razao = cursor_emp.fetchone()[0]

        downloads = Path.home() / "Downloads" / "Super" / "Resultados" / razao
        downloads.mkdir(parents=True, exist_ok=True)
        file_path = downloads / f"{periodo_mes}-{razao}.xlsx"

        cursor.execute("SELECT dt_ini, dt_fin FROM `0000` WHERE periodo LIKE %s LIMIT 1", (f"%{mes}/{ano}%",))
        dt_ini, dt_fin = cursor.fetchone()

        dt_ini_fmt = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
        dt_fin_fmt = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, startrow=2)
            sheet = writer.book.active
            sheet['A1'] = razao
            sheet['A2'] = f"Período: {dt_ini_fmt} a {dt_fin_fmt}"

        progress_bar.setValue(100)
        mensagem_sucesso(f"Tabela exportada com sucesso para {file_path}")

    except Exception as e:
        mensagem_error(f"Erro ao exportar a tabela: {e}")
    finally:
        cursor_emp.close()
        conexao_empresa.close()
        cursor.close()
        fechar_banco(conexao)

def baixar_tabela_com_mes_ano(nome_banco, mes, ano, progress_bar):
    try:
        progress_bar.setValue(5)
        conexao = conectar_banco(nome_banco)
        cursor = conexao.cursor()

        cursor.execute("""
            SELECT c.*, f.nome, f.cnpj 
            FROM c170_clone c 
            INNER JOIN `0150` f ON f.cod_part = c.cod_part 
            WHERE c.periodo = %s
        """, (f"{mes}/{ano}",))

        rows = cursor.fetchall()
        if not rows:
            mensagem_aviso("Não existem dados para o mês e ano selecionados.")
            return

        colunas = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=colunas)

        for campo in ['resultado', 'vl_item', 'aliquota']:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)

        cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
        cnpj = cursor.fetchone()[0]

        cursor.execute("SELECT periodo, dt_ini, dt_fin FROM `0000` WHERE periodo = %s LIMIT 1", (f"{mes}/{ano}",))
        resultado = cursor.fetchone()
        if not resultado:
            mensagem_error("Período não encontrado na tabela 0000.")
            return
        periodo_str, dt_ini, dt_fin = resultado

        conexao_emp = conectar_banco("empresas_db")
        cursor_emp = conexao_emp.cursor()
        cursor_emp.execute("SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1", (cnpj,))
        razao_result = cursor_emp.fetchone()
        if not razao_result:
            mensagem_error("Empresa não encontrada na base principal.")
            return
        nome_empresa = razao_result[0]

        pasta_saida = Path.home() / "Downloads" / "Super" / "Resultados" / nome_empresa
        pasta_saida.mkdir(parents=True, exist_ok=True)
        periodo_mes = f"{ano}-{mes}"
        caminho_arquivo = pasta_saida / f"{periodo_mes}-{nome_empresa}.xlsx"

        progress_bar.setValue(50)

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