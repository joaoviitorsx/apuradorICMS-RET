from PySide6 import QtWidgets, QtGui, QtCore
from unidecode import unidecode
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_aviso, mensagem_sucesso, mensagem_error

class TelaProduto(QtWidgets.QWidget):
    def __init__(self, empresa_id):
        super().__init__()
        self.empresa_id = empresa_id
        self.setWindowTitle("Gerenciador de Produtos e Tributação")
        self.setGeometry(300, 150, 850, 600)
        self.setStyleSheet("background-color: #030d18; color: white;")

        self.layout = QtWidgets.QVBoxLayout(self)

        self._criar_barra_pesquisa()
        self._criar_tabela()
        self._criar_botoes()
        self.carregar_dados()

        screen = QtGui.QGuiApplication.screenAt(QtGui.QCursor.pos())
        screen_geometry = screen.availableGeometry() if screen else QtWidgets.QApplication.primaryScreen().availableGeometry()
        center_point = screen_geometry.center()
        self.move(center_point - self.rect().center())

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
            cursor.execute("SELECT codigo, produto, ncm, aliquota, aliquotaRET, categoria_fiscal FROM cadastro_tributacao WHERE empresa_id = %s", (self.empresa_id,))
            self.dados_originais = cursor.fetchall()
            for row_idx, row in enumerate(self.dados_originais):
                self.tabela.insertRow(row_idx)
                for col_idx, valor in enumerate(row):
                    item = QtWidgets.QTableWidgetItem(str(valor))
                    self.tabela.setItem(row_idx, col_idx, item)

            self.tabela.resizeColumnsToContents()

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
        dados = [self.tabela.item(linha, i).text() if self.tabela.item(linha, i) else '' for i in range(6)]
        dados_originais = dict(zip(['codigo', 'produto', 'ncm', 'aliquota', 'aliquotaRET', 'categoria_fiscal'], dados))
        self._abrir_dialogo_edicao("editar", dados, dados_originais)

    def excluir_produto(self):
        linha = self.tabela.currentRow()
        if linha < 0:
            mensagem_aviso("Selecione um produto para excluir.")
            return
        codigo = self.tabela.item(linha, 0).text()
        produto = self.tabela.item(linha, 1).text()
        confirmacao = QtWidgets.QMessageBox.question(self, "Confirmar Exclusão", f"Deseja excluir o produto '{produto}' (código: {codigo})?", QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if confirmacao == QtWidgets.QMessageBox.Yes:
            conexao = conectar_banco()
            cursor = conexao.cursor()
            try:
                cursor.execute("DELETE FROM cadastro_tributacao WHERE empresa_id = %s AND codigo = %s", (self.empresa_id, codigo))
                conexao.commit()
                mensagem_sucesso("Produto excluído com sucesso.")
                self.carregar_dados()
            except Exception as e:
                mensagem_error(f"Erro ao excluir: {e}")
                conexao.rollback()
            finally:
                cursor.close()
                fechar_banco(conexao)

    def _abrir_dialogo_edicao(self, modo, dados=None, dados_originais=None):
        dialogo = QtWidgets.QDialog(self)
        dialogo.setWindowTitle("Editar Produto" if modo == "editar" else "Adicionar Produto")
        dialogo.setFixedSize(420, 360)
        dialogo.setStyleSheet("""
            QLineEdit {
                padding: 6px;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            QLineEdit:focus {
                border-color: #007bff;
            }
            QComboBox {
                padding: 6px;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            QComboBox:focus{
                border-color: #007bff;
            }
            QLabel {
                font-weight: bold;
            }
        """)

        layout = QtWidgets.QVBoxLayout(dialogo)

        form_layout = QtWidgets.QFormLayout()
        form_layout.setVerticalSpacing(12)

        campos = {}
        labels = ['Código', 'Produto', 'NCM', 'Alíquota', 'Alíquota RET', 'Categoria Fiscal']
        placeholders = ['Ex: 12345', 'Nome do produto', 'Ex: 1234.56.78', 'Ex: 18.00', 'Ex: 4.00', '']

        for i, label in enumerate(labels):
            chave = unidecode(label.lower().replace(' ', '_'))

            if label == 'Categoria Fiscal':
                combo = QtWidgets.QComboBox()
                opcoes = ["Regra Geral", "7% Cesta Básica", "12% Cesta Básica", "Bebida Alcoólica"]
                combo.addItems(opcoes)

                if dados:
                    try:
                        combo.setCurrentText(dados[i])
                    except IndexError:
                        pass

                campos[chave] = combo
                form_layout.addRow(f"{label}:", combo)
            else:
                campo = QtWidgets.QLineEdit()
                if dados:
                    campo.setText(dados[i])
                campo.setPlaceholderText(placeholders[i])
                campos[chave] = campo
                form_layout.addRow(f"{label}:", campo)

        lista_campos = [widget for widget in campos.values() if isinstance(widget, QtWidgets.QLineEdit)]
        if lista_campos:
            lista_campos[0].setFocus()

        layout.addLayout(form_layout)

        botoes_layout = QtWidgets.QHBoxLayout()
        btn_ok = QtWidgets.QPushButton("Salvar")
        btn_cancelar = QtWidgets.QPushButton("Cancelar")

        btn_ok.setIcon(QtGui.QIcon.fromTheme("dialog-ok"))
        btn_cancelar.setIcon(QtGui.QIcon.fromTheme("dialog-cancel"))

        btn_ok.clicked.connect(lambda: self._salvar_edicao(dialogo, campos, modo, dados_originais))
        btn_cancelar.clicked.connect(dialogo.reject)

        botoes_layout.addStretch()
        botoes_layout.addWidget(btn_ok)
        botoes_layout.addWidget(btn_cancelar)

        layout.addLayout(botoes_layout)

        dialogo.exec()

    def _salvar_edicao(self, dialogo, campos, modo, dados_originais=None):

        CATEGORIA_MAPA = {
            "Regra Geral": "regraGeral",
            "7% Cesta Básica": "cestaBasica7",
            "12% Cesta Básica": "cestaBasica12",
            "Bebida Alcoólica": "bebidaAlcoolica"
        }

        dados = {}
        for k, widget in campos.items():
            if isinstance(widget, QtWidgets.QComboBox):
                valor_visivel = widget.currentText()
                dados[k] = CATEGORIA_MAPA.get(valor_visivel, valor_visivel)
            else:
                dados[k] = widget.text().strip()

        for campo_nome, valor in dados.items():
            if not valor:
                mensagem_aviso(f"Preencha o campo '{campo_nome.replace('_', ' ').title()}'.")
                return
        for campo in ['aliquota', 'aliquota_ret']:
            valor = dados[campo].strip().replace(',', '.')
            if valor.replace('.', '', 1).isdigit():
                dados[campo] = f"{valor}%"
            else:
                dados[campo] = valor

        conexao = conectar_banco()
        cursor = conexao.cursor()

        try:
            
            MAPA_SQL = {
                'aliquota_ret': 'aliquotaRET',
                'categoria_fiscal': 'categoria_fiscal',
                'aliquota': 'aliquota',
                'codigo': 'codigo',
                'produto': 'produto',
                'ncm': 'ncm'
            }

            if modo.lower() == "editar":
                campos_para_alterar = []
                valores = []
                for campo, valor in dados.items():
                    if dados_originais.get(campo) != valor:
                        campo_sql = MAPA_SQL.get(campo, campo)
                        campos_para_alterar.append(f"{campo_sql} = %s")
                        valores.append(valor)
                if not campos_para_alterar:
                    mensagem_aviso("Nenhum campo foi alterado.")
                    return
                cursor.execute("""
                    SELECT 1 FROM cadastro_tributacao
                    WHERE empresa_id = %s AND codigo = %s AND produto = %s AND ncm = %s
                    AND NOT (codigo = %s AND produto = %s AND ncm = %s)
                """, (
                    self.empresa_id, dados['codigo'], dados['produto'], dados['ncm'],
                    dados_originais['codigo'], dados_originais['produto'], dados_originais['ncm']
                ))
                if cursor.fetchone():
                    mensagem_aviso("Já existe um produto com essa combinação de código, produto e NCM.")
                    return
                query = f"UPDATE cadastro_tributacao SET {', '.join(campos_para_alterar)} WHERE empresa_id = %s AND codigo = %s AND produto = %s AND ncm = %s"
                valores.extend([self.empresa_id, dados_originais['codigo'], dados_originais['produto'], dados_originais['ncm']])
                cursor.execute(query, tuple(valores))
            else:
                cursor.execute("SELECT 1 FROM cadastro_tributacao WHERE empresa_id = %s AND codigo = %s AND produto = %s AND ncm = %s", (self.empresa_id, dados['codigo'], dados['produto'], dados['ncm']))
                if cursor.fetchone():
                    mensagem_aviso("Produto já cadastrado. Verifique os dados.")
                    return
                cursor.execute("""
                    INSERT INTO cadastro_tributacao (empresa_id, codigo, produto, ncm, aliquota, aliquotaRET, categoria_fiscal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (self.empresa_id, dados['codigo'], dados['produto'], dados['ncm'], dados['aliquota'], dados['aliquota_ret'], dados['categoria_fiscal']))
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
