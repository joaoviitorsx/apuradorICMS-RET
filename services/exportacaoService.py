import xlsxwriter
from PySide6.QtCore import QThread, Signal
from utils.aliquota import formatarAliquota
from db.conexao import conectarBanco, fecharBanco

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

            conexao = conectarBanco()
            if not conexao:
                self.erro.emit("Não foi possível conectar ao banco de dados.")
                return
            cursor = conexao.cursor()

            cursor.execute("""
                SELECT codigo, produto, ncm 
                FROM cadastro_tributacao 
                WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
            """, (self.empresa_id,))
            if cursor.fetchall():
                self.erro.emit("Existem produtos com alíquotas nulas. Verifique antes de exportar.")
                return

            cursor.execute("""
                SELECT DISTINCT 
                    c.id, c.empresa_id, c.id_c100, c.ind_oper, c.filial, c.periodo, c.reg, c.cod_part,
                    IFNULL(f.nome, '') AS nome, IFNULL(f.cnpj, '') AS cnpj,
                    c.num_doc, c.cod_item, c.chv_nfe, c.num_item, c.descr_compl, c.ncm, c.unid,
                    c.qtd, c.vl_item, c.vl_desc, c.cfop, c.cst, c.uf, c.aliquota, c.resultado,
                    c.aliquotaRET, c.resultadoRET
                FROM c170_clone c
                LEFT JOIN `0150` f 
                    ON f.cod_part = c.cod_part AND f.empresa_id = c.empresa_id AND f.periodo = c.periodo
                WHERE c.periodo = %s AND c.empresa_id = %s
            """, (periodo, self.empresa_id))

            dados = cursor.fetchall()

            if not dados:
                self.erro.emit("Não existem dados para o período selecionado.")
                return

            colunas = [
                'id', 'empresa_id', 'id_c100', 'ind_oper', 'filial', 'periodo', 'reg', 'cod_part',
                'nome', 'cnpj', 'num_doc', 'cod_item', 'chv_nfe', 'num_item', 'descr_compl', 'ncm', 'unid',
                'qtd', 'vl_item', 'vl_desc', 'cfop', 'cst', 'uf', 'aliquota', 'resultado',
                'aliquotaRET', 'resultadoRET'
            ]

            cursor.execute("SELECT razao_social FROM empresas WHERE id = %s", (self.empresa_id,))
            nome_empresa = cursor.fetchone()
            nome_empresa = nome_empresa[0] if nome_empresa else "empresa"

            cursor.execute("""
                SELECT periodo, dt_ini, dt_fin 
                FROM `0000` 
                WHERE empresa_id = %s AND periodo = %s LIMIT 1
            """, (self.empresa_id, periodo))
            resultado = cursor.fetchone()
            if not resultado:
                self.erro.emit("Período não encontrado na tabela 0000.")
                return

            _, dt_ini, dt_fin = resultado

            dt_ini_fmt = f"{dt_ini[:2]}/{dt_ini[2:4]}/{dt_ini[4:]}"
            dt_fin_fmt = f"{dt_fin[:2]}/{dt_fin[2:4]}/{dt_fin[4:]}"
            periodo_legivel = f"Período: {dt_ini_fmt} a {dt_fin_fmt}"

            self.progress.emit(60)

            workbook = xlsxwriter.Workbook(self.caminho_arquivo)
            worksheet = workbook.add_worksheet()

            worksheet.write('A1', nome_empresa)
            worksheet.write('A2', periodo_legivel)

            colunas_numericas = {'qtd', 'vl_item', 'vl_desc', 'resultado', 'resultadoRET'}

            for col_idx, col_name in enumerate(colunas):
                worksheet.write(2, col_idx, col_name)

            for row_idx, row in enumerate(dados, start=3):
                dados_dict = dict(zip(colunas, row))

                for col_idx, nome_coluna in enumerate(colunas):
                    valor = dados_dict.get(nome_coluna, '')

                    if nome_coluna in {'aliquota', 'aliquotaRET'}:
                        valor = formatarAliquota(valor)

                    elif nome_coluna in colunas_numericas:
                        try:
                            valor = float(valor)
                            valor = f"{valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
                        except:
                            valor = '0,00'

                    worksheet.write_string(row_idx, col_idx, str(valor))

                if row_idx % 10000 == 0:
                    progresso = min(95, 60 + int(row_idx / len(dados) * 40))
                    self.progress.emit(progresso)

            workbook.close()
            self.progress.emit(100)
            self.finished.emit(self.caminho_arquivo)

        except Exception as e:
            self.erro.emit(f"Erro ao exportar: {e}")
        finally:
            try:
                cursor.close()
                fecharBanco(conexao)
            except:
                pass
