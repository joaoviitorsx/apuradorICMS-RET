from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QFileDialog
import pandas as pd
from pathlib import Path
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso

class ExportarTabelaThread(QThread):
    progresso = Signal(int)
    mensagem = Signal(str)

    def __init__(self, nome_banco, mes, ano):
        super().__init__()
        self.nome_banco = nome_banco
        self.mes = mes
        self.ano = ano

    def run(self):
        self.progresso.emit(5)
        conexao = conectar_banco(self.nome_banco)
        if not conexao:
            self.mensagem.emit("Erro ao conectar ao banco de dados.")
            return

        try:
            cursor = conexao.cursor()
            query = """
                SELECT c.*, f.nome, f.cnpj 
                FROM c170_clone c 
                INNER JOIN `0150` f ON f.cod_part = c.cod_part 
                WHERE c.periodo = %s
            """
            cursor.execute(query, (f"{self.mes}/{self.ano}",))
            rows = cursor.fetchall()

            if not rows:
                self.mensagem.emit("Não existem dados para o mês e ano selecionados.")
                return

            colunas = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=colunas)

            for campo in ['resultado', 'vl_item', 'aliquota']:
                if campo in df.columns:
                    df[campo] = df[campo].astype(str).str.replace('.', ',', regex=False)

            cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
            cnpj = cursor.fetchone()[0]

            cursor.execute("SELECT periodo, dt_ini, dt_fin FROM `0000` WHERE periodo = %s LIMIT 1", (f"{self.mes}/{self.ano}",))
            resultado = cursor.fetchone()
            if not resultado:
                self.mensagem.emit("Período não encontrado.")
                return
            periodo_str, dt_ini, dt_fin = resultado

            conexao_emp = conectar_banco("empresas_db")
            cursor_emp = conexao_emp.cursor()
            cursor_emp.execute("SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1", (cnpj,))
            razao_result = cursor_emp.fetchone()
            if not razao_result:
                self.mensagem.emit("Empresa não encontrada.")
                return
            nome_empresa = razao_result[0]

            caminho_arquivo, _ = QFileDialog.getSaveFileName(
                None,
                "Salvar Resultado",
                f"{nome_empresa}-{self.ano}-{self.mes}.xlsx",
                "Planilhas Excel (*.xlsx)"
            )
            if not caminho_arquivo:
                self.mensagem.emit("Exportação cancelada pelo usuário.")
                return

            self.progresso.emit(50)

            dt_ini_str = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
            dt_fin_str = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
            periodo_legivel = f"Período: {dt_ini_str} a {dt_fin_str}"

            with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, startrow=2)
                sheet = writer.book.active
                sheet['A1'] = nome_empresa
                sheet['A2'] = periodo_legivel

            self.progresso.emit(100)
            self.mensagem.emit(f"Tabela exportada com sucesso para:\n{caminho_arquivo}")

        except Exception as e:
            self.mensagem.emit(f"Erro ao exportar: {e}")
        finally:
            try:
                cursor.close()
                fechar_banco(conexao)
                cursor_emp.close()
                fechar_banco(conexao_emp)
            except:
                pass
