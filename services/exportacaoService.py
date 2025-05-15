import pandas as pd
from pathlib import Path
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