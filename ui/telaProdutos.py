from PySide6 import QtWidgets, QtGui, QtCore
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_aviso, mensagem_sucesso, mensagem_error


class TelaProduto(QtWidgets.QWidget):
    def __init__(self, empresa_id):
        super().__init__()
        self.empresa_id = empresa_id
        self.setWindowTitle("Gerenciar Produtos e Tributação")
        self.setGeometry(300, 150, 850, 600)
        self.setStyleSheet("background-color: #030d18; color: white;")

        self.layout = QtWidgets.QVBoxLayout(self)

        self._criar_barra_pesquisa()
        self._criar_tabela()
        self._criar_botoes()
        self.carregar_dados()

    def _criar_barra_pesquisa(self):
        search_layout = QtWidgets.QHBoxLayout()
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Buscar por qualquer campo...")
        self.search_input.setStyleSheet("background-color: white; color: black; padding: 5px; font-size: 14px;")
        self.search_input.textChanged.connect(self.filtrar_tabela)

        search_layout.addWidget(self.search_input)
        self.layout.addLayout(search_layout)

    def _criar_tabela(self):
        self.tabela = QtWidgets.QTableWidget()
        self.tabela.setColumnCount(6)
        self.tabela.setHorizontalHeaderLabels(['Código', 'Produto', 'NCM', 'Alíquota', 'Alíquota RET', 'Categoria Fiscal'])
        self.tabela.horizontalHeader().setStretchLastSection(True)
        self.tabela.setSelectionBehavior(QtWidgets.QTableWidget.SelectRows)
        self.tabela.setEditTriggers(QtWidgets.QTableWidget.NoEditTriggers)
        self.layout.addWidget(self.tabela)

    def _criar_botoes(self):
        botoes_layout = QtWidgets.QHBoxLayout()

        self.btn_adicionar = QtWidgets.QPushButton("Adicionar")
        self.btn_adicionar.setStyleSheet(self._estilo_botao("#28a745", "#218838"))
        self.btn_adicionar.clicked.connect(self.adicionar_produto)

        self.btn_editar = QtWidgets.QPushButton("Editar")
        self.btn_editar.setStyleSheet(self._estilo_botao("#007bff", "#0069d9"))
        self.btn_editar.clicked.connect(self.editar_produto)

        self.btn_excluir = QtWidgets.QPushButton("Excluir")
        self.btn_excluir.setStyleSheet(self._estilo_botao("#dc3545", "#c82333"))
        self.btn_excluir.clicked.connect(self.excluir_produto)

        for btn in [self.btn_adicionar, self.btn_editar, self.btn_excluir]:
            btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            btn.setFont(QtGui.QFont("Arial", 12))
            botoes_layout.addWidget(btn)

        self.layout.addLayout(botoes_layout)

    def _estilo_botao(self, cor, hover):
        return f"""
            QPushButton {{
                background-color: {cor};
                color: white;
                border: none;
                padding: 8px 15px;
                border-radius: 5px;
            }}
            QPushButton:hover {{
                background-color: {hover};
            }}
        """

    def carregar_dados(self):
        self.tabela.setRowCount(0)
        conexao = conectar_banco()
        cursor = conexao.cursor()
        try:
            cursor.execute("""
                SELECT codigo, produto, ncm, aliquota, aliquotaRET, categoria_fiscal
                FROM cadastro_tributacao
                WHERE empresa_id = %s
            """, (self.empresa_id,))
            self.dados_originais = cursor.fetchall()

            for row_idx, row in enumerate(self.dados_originais):
                self.tabela.insertRow(row_idx)
                for col_idx, valor in enumerate(row):
                    item = QtWidgets.QTableWidgetItem(str(valor))
                    self.tabela.setItem(row_idx, col_idx, item)

        except Exception as e:
            mensagem_error(f"Erro ao carregar dados: {e}")
        finally:
            cursor.close()
            fechar_banco(conexao)

    def filtrar_tabela(self):
        termo = self.search_input.text().lower()
        self.tabela.setRowCount(0)

        for row in self.dados_originais:
            if any(termo in str(campo).lower() for campo in row):
                row_idx = self.tabela.rowCount()
                self.tabela.insertRow(row_idx)
                for col_idx, valor in enumerate(row):
                    self.tabela.setItem(row_idx, col_idx, QtWidgets.QTableWidgetItem(str(valor)))

    def adicionar_produto(self):
        self._abrir_dialogo_edicao("adicionar")

    def editar_produto(self):
        linha = self.tabela.currentRow()
        if linha < 0:
            mensagem_aviso("Selecione um produto para editar.")
            return

        dados = [self.tabela.item(linha, i).text() for i in range(6)]
        self._abrir_dialogo_edicao("editar", dados)

    def excluir_produto(self):
        linha = self.tabela.currentRow()
        if linha < 0:
            mensagem_aviso("Selecione um produto para excluir.")
            return

        codigo = self.tabela.item(linha, 0).text()
        produto = self.tabela.item(linha, 1).text()

        confirmacao = QtWidgets.QMessageBox.question(
            self, "Confirmar Exclusão",
            f"Deseja excluir o produto '{produto}' (código: {codigo})?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
        )

        if confirmacao == QtWidgets.QMessageBox.Yes:
            conexao = conectar_banco()
            cursor = conexao.cursor()
            try:
                cursor.execute("""
                    DELETE FROM cadastro_tributacao
                    WHERE empresa_id = %s AND codigo = %s
                """, (self.empresa_id, codigo))
                conexao.commit()
                mensagem_sucesso("Produto excluído com sucesso.")
                self.carregar_dados()
            except Exception as e:
                mensagem_error(f"Erro ao excluir: {e}")
                conexao.rollback()
            finally:
                cursor.close()
                fechar_banco(conexao)

    def _abrir_dialogo_edicao(self, modo, dados=None):
        dialogo = QtWidgets.QDialog(self)
        dialogo.setWindowTitle("Editar Produto" if modo == "editar" else "Adicionar Produto")
        layout = QtWidgets.QFormLayout(dialogo)

        campos = {}
        labels = ['Código', 'Produto', 'NCM', 'Alíquota', 'Alíquota RET', 'Categoria Fiscal']
        for i, label in enumerate(labels):
            campo = QtWidgets.QLineEdit()
            if dados:
                campo.setText(dados[i])
            layout.addRow(label + ':', campo)
            campos[label.lower().replace(' ', '_')] = campo

        botoes = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        botoes.accepted.connect(lambda: self._salvar_edicao(dialogo, campos, modo))
        botoes.rejected.connect(dialogo.reject)
        layout.addRow(botoes)

        dialogo.exec()

    def _salvar_edicao(self, dialogo, campos, modo):
        dados = {k: v.text().strip() for k, v in campos.items()}
        conexao = conectar_banco()
        cursor = conexao.cursor()
        try:
            if modo == "editar":
                cursor.execute("""
                    UPDATE cadastro_tributacao
                    SET produto = %s, ncm = %s, aliquota = %s, aliquotaRET = %s, categoria_fiscal = %s
                    WHERE empresa_id = %s AND codigo = %s
                """, (
                    dados['produto'], dados['ncm'], dados['aliquota'],
                    dados['aliquota_ret'], dados['categoria_fiscal'],
                    self.empresa_id, dados['codigo']
                ))
            else:
                cursor.execute("""
                    INSERT INTO cadastro_tributacao
                    (empresa_id, codigo, produto, ncm, aliquota, aliquotaRET, categoria_fiscal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.empresa_id, dados['codigo'], dados['produto'],
                    dados['ncm'], dados['aliquota'], dados['aliquota_ret'],
                    dados['categoria_fiscal']
                ))
            conexao.commit()
            mensagem_sucesso("Produto salvo com sucesso.")
            dialogo.accept()
            self.carregar_dados()
        except Exception as e:
            mensagem_error(f"Erro ao salvar: {e}")
            conexao.rollback()
        finally:
            cursor.close()
            fechar_banco(conexao)
