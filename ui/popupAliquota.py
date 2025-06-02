import pandas as pd
from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QHBoxLayout, QMessageBox
from PySide6.QtCore import Qt
from db.conexao import conectar_banco, fechar_banco
from utils.sanitizacao import atualizar_aliquotas_e_resultado

class PopupAliquota(QDialog):
    def __init__(self, empresa_id, parent=None):
        super().__init__(parent)
        self.empresa_id = empresa_id
        self.setWindowTitle("Preencher Alíquotas Nulas")
        self.setMinimumSize(800, 600)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        self.label = QLabel("Preencha as alíquotas nulas antes de prosseguir:")
        layout.addWidget(self.label)

        self.tabela = QTableWidget()
        layout.addWidget(self.tabela)

        botoes_extra = QHBoxLayout()
        
        self.botao_criar_planilha = QPushButton("Criar Planilha Modelo")
        self.botao_criar_planilha.clicked.connect(self.exportar_planilha_modelo)
        self.botao_criar_planilha.setStyleSheet("""
            QPushButton {
                background-color: #007bff;
                color: white;
                font-weight: bold;
                border-radius: 6px;
                padding: 6px 12px;
            }
            QPushButton:hover {
                background-color: #0054b3;
            }
        """)
        self.botao_criar_planilha.setCursor(Qt.PointingHandCursor)
        botoes_extra.addWidget(self.botao_criar_planilha)

        self.botao_importar_planilha = QPushButton("Importar Planilha")
        self.botao_importar_planilha.clicked.connect(self.importar_planilha)
        self.botao_importar_planilha.setStyleSheet("""
            QPushButton {
                background-color: #007bff;
                color: white;
                font-weight: bold;
                border-radius: 6px;
                padding: 6px 12px;
            }
            QPushButton:hover {
                background-color: #0054b3;
            }
        """)
        self.botao_importar_planilha.setCursor(Qt.PointingHandCursor)
        botoes_extra.addWidget(self.botao_importar_planilha)

        layout.addLayout(botoes_extra)

        self.botao_salvar = QPushButton("Salvar Tudo")
        self.botao_salvar.clicked.connect(self.salvar_dados)
        self.botao_salvar.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                font-weight: bold;
                border-radius: 6px;
                padding: 6px 12px;
            }
            QPushButton:hover {
                background-color: #166628;
            }
        """)
        self.botao_salvar.setCursor(Qt.PointingHandCursor)
        layout.addWidget(self.botao_salvar)

        self.carregar_dados()


    def carregar_dados(self):
        conexao = conectar_banco()
        cursor = conexao.cursor()

        cursor.execute("""
            SELECT id, codigo, produto, ncm, aliquota FROM cadastro_tributacao
            WHERE empresa_id = %s AND (aliquota IS NULL OR aliquota = '')
        """, (self.empresa_id,))

        dados = cursor.fetchall()
        cursor.close()
        fechar_banco(conexao)

        self.tabela.setRowCount(len(dados))
        self.tabela.setColumnCount(5)
        self.tabela.setHorizontalHeaderLabels(["ID", "Código", "Produto", "NCM", "Alíquota"])
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        for row_idx, (id_, codigo, produto, ncm, aliquota) in enumerate(dados):
            self.tabela.setItem(row_idx, 0, QTableWidgetItem(str(id_)))
            self.tabela.setItem(row_idx, 1, QTableWidgetItem(codigo))
            self.tabela.setItem(row_idx, 2, QTableWidgetItem(produto))
            self.tabela.setItem(row_idx, 3, QTableWidgetItem(ncm))
            item_aliquota = QTableWidgetItem(aliquota if aliquota else "")
            item_aliquota.setFlags(item_aliquota.flags() | Qt.ItemIsEditable)
            self.tabela.setItem(row_idx, 4, item_aliquota)

    def salvar_dados(self):
        conexao = conectar_banco()
        cursor = conexao.cursor()

        try:
            for row in range(self.tabela.rowCount()):
                id_item = int(self.tabela.item(row, 0).text())
                nova_aliquota = self.tabela.item(row, 4).text().strip()
                cursor.execute("""
                    UPDATE cadastro_tributacao
                    SET aliquota = %s
                    WHERE id = %s AND empresa_id = %s
                """, (nova_aliquota, id_item, self.empresa_id))

            conexao.commit()
            self.label.setText("Alíquotas atualizadas com sucesso.")
            atualizar_aliquotas_e_resultado(self.empresa_id)
            self.accept()

        except Exception as e:
            conexao.rollback()
            self.label.setText(f"Erro ao salvar: {e}")
        finally:
            cursor.close()
            fechar_banco(conexao)

    def exportar_planilha_modelo(self):
        caminho, _ = QFileDialog.getSaveFileName(self, "Salvar Planilha Modelo", "planilha_modelo.xlsx", "Arquivos Excel (*.xlsx)")
        if not caminho:
            return

        dados = []
        for row in range(self.tabela.rowCount()):
            dados.append({
                "Código": self.tabela.item(row, 1).text(),
                "Produto": self.tabela.item(row, 2).text(),
                "NCM": self.tabela.item(row, 3).text(),
                "Alíquota": self.tabela.item(row, 4).text()
            })

        df = pd.DataFrame(dados)
        df.to_excel(caminho, index=False)

        resposta = QMessageBox.question(
            self,
            "Abrir Planilha",
            "Planilha modelo criada com sucesso.\nDeseja abri-la agora?",
            QMessageBox.Yes | QMessageBox.No
        )

        if resposta == QMessageBox.Yes:
            import os
            os.startfile(caminho)

    def importar_planilha(self):
        caminho, _ = QFileDialog.getOpenFileName(self, "Selecionar Planilha", "", "Arquivos Excel (*.xlsx *.xls)")
        if not caminho:
            return

        try:
            df = pd.read_excel(caminho)
            colunas_normalizadas = {col.strip().lower().replace("ç", "c").replace("á", "a").replace("í", "i").replace("ú", "u"): col for col in df.columns}
            
            col_codigo = colunas_normalizadas.get("codigo")
            col_aliquota = colunas_normalizadas.get("aliquota") or colunas_normalizadas.get("aliquota%") or colunas_normalizadas.get("aliquota icms")

            if not col_codigo or not col_aliquota:
                QMessageBox.warning(self, "Importação falhou", "Colunas 'Código' e/ou 'Alíquota' não foram encontradas na planilha.")
                return

            codigos_planilha = df.set_index(col_codigo)[col_aliquota].dropna().to_dict()

            atualizados = 0
            for row in range(self.tabela.rowCount()):
                item_codigo = self.tabela.item(row, 1)
                item_aliquota = self.tabela.item(row, 4)
                if item_codigo and item_aliquota:
                    codigo = item_codigo.text()
                    if codigo in codigos_planilha:
                        novo_valor = str(codigos_planilha[codigo])
                        item_aliquota.setText(novo_valor)
                        atualizados += 1

            QMessageBox.information(self, "Importação concluída", f"{atualizados} alíquotas atualizadas com sucesso.")
        except Exception as e:
            QMessageBox.critical(self, "Erro ao importar", f"Ocorreu um erro ao importar a planilha:\n{e}")

