from PySide6 import QtWidgets, QtCore, QtGui
from ui.cadastroEmpresa import EmpresaCadastro
from ui.telaPrincipal import MainWindow
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from utils.icone import usar_icone
from db.conexao import conectar_banco, tabela_empresa

class EmpresaWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Apurado de ICMS - Empresas')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.banco_empresas = 'empresas_db'
        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(10)

        self.label_titulo = QtWidgets.QLabel('Escolha ou cadastre uma empresa')
        self.label_titulo.setStyleSheet("font-size: 20px; font-weight: bold; color: #ffffff;")
        self.label_titulo.setAlignment(QtCore.Qt.AlignCenter)
        self.layout.addStretch()
        self.layout.addWidget(self.label_titulo)

        self.layout.addSpacing(20)

        self.combo_empresas = QtWidgets.QComboBox()
        self.combo_empresas.setStyleSheet("font-size: 20px; padding: 8px; border: 1px solid #ccc; border-radius: 5px; color: white;")
        self.combo_empresas.setFixedWidth(400)
        self.combo_empresas.addItem("Carregando empresas...")
        self.layout.addWidget(self.combo_empresas, alignment=QtCore.Qt.AlignCenter)

        self.layout.addSpacing(20)

        self.entrar_btn = QtWidgets.QPushButton('Entrar')
        self.entrar_btn.setStyleSheet(self._botao_estilo())
        self.entrar_btn.setFixedWidth(400)
        self.entrar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.entrar_btn.setEnabled(False)
        self.entrar_btn.clicked.connect(self.entrar)
        self.layout.addWidget(self.entrar_btn, alignment=QtCore.Qt.AlignCenter)

        self.layout.addSpacing(10)

        self.cadastrar_btn = QtWidgets.QPushButton('Cadastrar Empresa')
        self.cadastrar_btn.setStyleSheet(self._botao_estilo())
        self.cadastrar_btn.setFixedWidth(400)
        self.cadastrar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.cadastrar_btn.clicked.connect(self.cadastrar_empresa)
        self.layout.addWidget(self.cadastrar_btn, alignment=QtCore.Qt.AlignCenter)

        self.layout.addStretch()

        self.loader_thread = EmpresaLoader(self.banco_empresas)
        self.loader_thread.empresas_carregadas.connect(self.preencher_empresas)
        self.loader_thread.erro_ocorrido.connect(self.exibir_erro_empresas)
        self.loader_thread.start()

    def _botao_estilo(self):
        return """
            QPushButton {
                font-size: 20px;
                font-weight: bold;
                padding: 10px;
                background-color: #001F3F;
                color: white;
            }
            QPushButton:hover {
                background-color: #005588;
                color: white;
            }
            QPushButton:disabled {
                background-color: #555555;
                color: #cccccc;
            }
        """

    def entrar(self):
        empresa = self.combo_empresas.currentText()
        if empresa == "Selecione uma empresa" or not empresa:
            mensagem_aviso("Selecione uma empresa.")
            return
        self.janela_principal = MainWindow(empresa)
        usar_icone(self.janela_principal)
        self.janela_principal.showMaximized()
        self.close()

    def cadastrar_empresa(self):
        self.empresa_cadastro = EmpresaCadastro(self.banco_empresas)
        usar_icone(self.empresa_cadastro)
        self.empresa_cadastro.showMaximized()
        self.close()

    def preencher_empresas(self, lista_empresas):
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Selecione uma empresa")
        self.combo_empresas.addItems(lista_empresas)
        self.combo_empresas.model().item(0).setEnabled(False)
        self.entrar_btn.setEnabled(True)

    def exibir_erro_empresas(self, erro):
        QtWidgets.QMessageBox.critical(self, "Erro", f"Erro ao carregar empresas: {erro}")
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Erro ao carregar")
        self.entrar_btn.setEnabled(False)

class EmpresaLoader(QtCore.QThread):
    empresas_carregadas = QtCore.Signal(list)
    erro_ocorrido = QtCore.Signal(str)

    def __init__(self, banco_empresas):
        super().__init__()
        self.banco_empresas = banco_empresas

    def run(self):
        try:
            conexao = conectar_banco(self.banco_empresas)
            tabela_empresa(conexao)
            cursor = conexao.cursor()
            cursor.execute(f"USE {self.banco_empresas}")
            cursor.execute("SELECT razao_social FROM empresas ORDER BY razao_social ASC")
            empresas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conexao.close()
            self.empresas_carregadas.emit(empresas)
        except Exception as e:
            self.erro_ocorrido.emit(str(e))
