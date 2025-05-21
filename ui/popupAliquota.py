from PySide6 import QtWidgets, QtGui, QtCore
from utils.mensagem import mensagem_error, mensagem_sucesso
import pandas as pd
from db.conexao import conectar_banco, fechar_banco
from PySide6.QtWidgets import QFileDialog

class PopupAliquota(QtWidgets.QDialog):
    def __init__(self, dados, nome_banco):
        super().__init__()
        self.setWindowTitle("Preencher Alíquotas Nulas")
        self.setGeometry(300, 150, 900, 600)

        self.dados = dados
        self.nome_banco = nome_banco
        self.aliquotas = ["1.54%", "4.00%", "8.13%", "ST", "ISENTO"]

        self._setup_ui()
        self._aplicar_estilo()

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        self.tabela = QtWidgets.QTableWidget()
        self.tabela.setColumnCount(4)
        self.tabela.setHorizontalHeaderLabels(["Código", "Produto", "NCM", "Alíquota"])
        self.tabela.setRowCount(len(self.dados))

        for row, (codigo, produto, ncm) in enumerate(self.dados):
            self.tabela.setItem(row, 0, QtWidgets.QTableWidgetItem(str(codigo)))
            self.tabela.setItem(row, 1, QtWidgets.QTableWidgetItem(str(produto)))
            self.tabela.setItem(row, 2, QtWidgets.QTableWidgetItem(str(ncm)))

            combo = QtWidgets.QComboBox()
            combo.addItems(self.aliquotas)
            self.tabela.setCellWidget(row, 3, combo)

        self.tabela.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        layout.addWidget(self.tabela)

        botoes_layout = QtWidgets.QHBoxLayout()

        btn_modelo = QtWidgets.QPushButton("Gerar Planilha Modelo")
        btn_modelo.clicked.connect(self.gerar_planilha_modelo)
        botoes_layout.addWidget(btn_modelo)

        btn_importar = QtWidgets.QPushButton("Importar Planilha")
        btn_importar.clicked.connect(self.importar_planilha)
        botoes_layout.addWidget(btn_importar)

        btn_salvar = QtWidgets.QPushButton("Salvar Tudo")
        btn_salvar.setObjectName("btn_salvar")
        btn_salvar.clicked.connect(self.salvar_todas)
        botoes_layout.addWidget(btn_salvar)

        layout.addLayout(botoes_layout)

    def _aplicar_estilo(self):
        self.setStyleSheet("""
            QDialog {
                background-color: #030d18;
            }
            QTableWidget {
                background-color: #ffffff;
                color: #000000;
                gridline-color: #cccccc;
                font-size: 14px;
            }
            QHeaderView::section {
                background-color: #001F3F;
                color: white;
                padding: 4px;
                font-weight: bold;
                border: 1px solid #2E236C;
            }
            QComboBox {
                background-color: #ffffff;
                color: #000000;
            }
            QPushButton {
                padding: 6px 12px;
                font-size: 14px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #2E236C;
                color: white;
            }
            QPushButton#btn_salvar {
                background-color: #28a745;
                color: white;
            }
            QPushButton#btn_salvar:hover {
                background-color: #218838;
            }
        """)

    def salvar_todas(self):
        conexao = conectar_banco(self.nome_banco)
        cursor = conexao.cursor()
        try:
            for row in range(self.tabela.rowCount()):
                codigo = self.tabela.item(row, 0).text()
                aliquota = self.tabela.cellWidget(row, 3).currentText()
                if aliquota not in self.aliquotas:
                    mensagem_error(f"Alíquota inválida para o código {codigo}: {aliquota}")
                    return
                cursor.execute("""
                    UPDATE cadastro_tributacao SET aliquota = %s
                    WHERE codigo = %s
                """, (aliquota, codigo))
            conexao.commit()
            mensagem_sucesso("Alíquotas salvas com sucesso!")
            self.accept()
        except Exception as e:
            mensagem_error(f"Erro ao salvar alíquotas: {e}")
            conexao.rollback()
        finally:
            cursor.close()
            fechar_banco(conexao)

    def importar_planilha(self):
        caminho, _ = QFileDialog.getOpenFileName(self, "Importar Planilha de Alíquotas", "", "Excel (*.xlsx *.xls)")
        if not caminho:
            return

        try:
            df = pd.read_excel(caminho, dtype=str)
            colunas = list(df.columns.str.upper())
            df.columns = colunas

            col_cod = next((c for c in colunas if 'COD' in c), None)
            col_aliq = next((c for c in colunas if 'ALIQ' in c), None)

            if not col_cod or not col_aliq:
                mensagem_error("A planilha deve conter colunas semelhantes a 'CÓDIGO' e 'ALÍQUOTA'.")
                return

            cod_to_aliq = dict(zip(df[col_cod], df[col_aliq]))
            preenchidos = 0
            for row in range(self.tabela.rowCount()):
                codigo = self.tabela.item(row, 0).text()
                if codigo in cod_to_aliq:
                    valor = cod_to_aliq[codigo].strip().upper()
                    combo = self.tabela.cellWidget(row, 3)
                    index = combo.findText(valor)
                    if index >= 0:
                        combo.setCurrentIndex(index)
                        preenchidos += 1

            mensagem_sucesso(f"Planilha importada com sucesso. {preenchidos} itens preenchidos.")
        except Exception as e:
            mensagem_error(f"Erro ao importar planilha: {e}")

    def gerar_planilha_modelo(self):
        caminho, _ = QFileDialog.getSaveFileName(self, "Salvar Planilha Modelo", "modelo_aliquotas.xlsx", "Excel (*.xlsx)")
        if not caminho:
            return
        try:
            df = pd.DataFrame(self.dados, columns=["Codigo", "Produto", "NCM"])
            df["Aliquota"] = ""
            df.to_excel(caminho, index=False)
            mensagem_sucesso("Planilha modelo gerada com sucesso!")
        except Exception as e:
            mensagem_error(f"Erro ao gerar planilha modelo: {e}")
