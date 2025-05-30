from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView
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

        self.botao_salvar = QPushButton("Salvar Tudo")
        self.botao_salvar.clicked.connect(self.salvar_dados)
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
