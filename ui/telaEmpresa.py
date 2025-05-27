from PySide6 import QtWidgets, QtGui, QtCore
from ui.cadastroEmpresa import EmpresaCadastro
from ui.telaPrincipal import MainWindow
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from utils.icone import usar_icone
from db.conexao import conectar_banco, fechar_banco
from db.criarTabelas import criar_tabelas_principais  # ✅ NOVO

class EmpresaWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Apurado de ICMS - Empresas')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.banco_empresas = 'empresas_db'

        # ✅ Verifica e cria o banco e as tabelas necessárias ao iniciar a tela
        self._verificar_estrutura_banco()

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

        # ✅ Carregar empresas após garantir que estrutura existe
        self._carregar_empresas()

    def _verificar_estrutura_banco(self):
        try:
            conexao = conectar_banco()
            if conexao:
                criar_tabelas_principais()  # Garante estrutura
                fechar_banco(conexao)
        except Exception as e:
            mensagem_error(f"Erro ao preparar estrutura do banco: {e}")

    def _carregar_empresas(self):
        try:
            conexao = conectar_banco()
            cursor = conexao.cursor()
            cursor.execute("SELECT razao_social FROM empresas ORDER BY razao_social ASC")
            empresas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            fechar_banco(conexao)

            self.combo_empresas.clear()
            self.combo_empresas.addItem("Selecione uma empresa")
            self.combo_empresas.addItems(empresas)
            self.combo_empresas.model().item(0).setEnabled(False)
            self.entrar_btn.setEnabled(True)
        except Exception as e:
            self.exibir_erro_empresas(str(e))

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
        nome_empresa = self.combo_empresas.currentText()
        if nome_empresa == "Selecione uma empresa" or not nome_empresa:
            mensagem_aviso("Selecione uma empresa.")
            return

        conexao = conectar_banco()
        cursor = conexao.cursor()
        cursor.execute("SELECT id FROM empresas WHERE razao_social = %s", (nome_empresa,))
        resultado = cursor.fetchone()
        cursor.close()
        fechar_banco(conexao)

        if not resultado:
            mensagem_error("Empresa não encontrada na base de dados.")
            return

        empresa_id = resultado[0]

        self.janela_principal = MainWindow(nome_empresa, empresa_id)
        usar_icone(self.janela_principal)
        self.janela_principal.showMaximized()
        self.close()

    def cadastrar_empresa(self):
        self.empresa_cadastro = EmpresaCadastro(self.banco_empresas)
        usar_icone(self.empresa_cadastro)
        self.empresa_cadastro.showMaximized()
        self.close()

    def exibir_erro_empresas(self, erro):
        QtWidgets.QMessageBox.critical(self, "Erro", f"Erro ao carregar empresas: {erro}")
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Erro ao carregar")
        self.entrar_btn.setEnabled(False)
