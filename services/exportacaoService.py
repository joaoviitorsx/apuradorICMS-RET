import os
import time
import xlsxwriter
from PySide6.QtCore import QThread, Signal
from ui.popupAliquota import PopupAliquota
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso

class ExportWorker(QThread):
    progress = Signal(int)
    finished = Signal(str)
    erro = Signal(str)

    def __init__(self, empresa_id, mes, ano, caminho_arquivo):
        super().__init__()
        self.empresa_id = empresa_id
        self.mes = mes
        self.ano = ano
        self.caminho_arquivo = caminho_arquivo

    def run(self):
        try:
            self.progress.emit(5)
            periodo = f"{int(self.mes):02d}/{self.ano}"

            conexao = conectar_banco()
            if not conexao:
                self.erro.emit("Não foi possível conectar ao banco de dados.")
                return
            cursor = conexao.cursor()

            cursor.execute("""
                SELECT codigo, produto, ncm 
                FROM cadastro_tributacao 
                WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
            """, (self.empresa_id,))
            produtos_nulos = cursor.fetchall()

            if produtos_nulos:
                self.erro.emit("Existem produtos com alíquotas nulas. Preencha antes de exportar.")
                return

            cursor.execute("""
                SELECT c.*, IFNULL(f.nome, '') AS nome, IFNULL(f.cnpj, '') AS cnpj 
                FROM c170_clone c 
                LEFT JOIN `0150` f ON f.cod_part = c.cod_part AND f.empresa_id = c.empresa_id
                WHERE c.periodo = %s AND c.empresa_id = %s
            """, (periodo, self.empresa_id))
            dados = cursor.fetchall()

            if not dados:
                self.erro.emit("Não existem dados para o mês e ano selecionados.")
                return

            colunas = [desc[0] for desc in cursor.description]

            cursor.execute("SELECT razao_social FROM empresas WHERE id = %s", (self.empresa_id,))
            nome_empresa_result = cursor.fetchone()
            nome_empresa = nome_empresa_result[0] if nome_empresa_result else "empresa"

            cursor.execute("SELECT periodo, dt_ini, dt_fin FROM `0000` WHERE empresa_id = %s AND periodo = %s LIMIT 1", (self.empresa_id, periodo))
            resultado = cursor.fetchone()
            if not resultado:
                self.erro.emit("Período não encontrado na tabela 0000.")
                return
            _, dt_ini, dt_fin = resultado

            caminho_arquivo = self.caminho_arquivo

            self.progress.emit(60)

            dt_ini_fmt = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
            dt_fin_fmt = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
            periodo_legivel = f"Período: {dt_ini_fmt} a {dt_fin_fmt}"

            start_time = time.time()

            workbook = xlsxwriter.Workbook(caminho_arquivo)
            worksheet = workbook.add_worksheet()

            worksheet.write('A1', nome_empresa)
            worksheet.write('A2', periodo_legivel)

            for col_idx, col_name in enumerate(colunas):
                worksheet.write(2, col_idx, col_name)

            for row_idx, row in enumerate(dados, start=3):
                for col_idx, valor in enumerate(row):
                    worksheet.write(row_idx, col_idx, valor)

                if row_idx % 10000 == 0:
                    progresso = min(95, 60 + int(row_idx / len(dados) * 40))
                    self.progress.emit(progresso)

            workbook.close()

            tempo_total = time.time() - start_time
            print(f"[DEBUG] Exportação concluída em {tempo_total:.2f} segundos")

            self.progress.emit(100)
            self.finished.emit(caminho_arquivo)

        except Exception as e:
            self.erro.emit(f"Erro ao exportar: {e}")
        finally:
            try:
                cursor.close()
                fechar_banco(conexao)
            except:
                pass
