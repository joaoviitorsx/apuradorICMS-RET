from PySide6 import QtWidgets, QtCore, QtGui
from ui.telaEmpresa import EmpresaWindow
from utils.mensagem import mensagem_error, mensagem_sucesso
from utils.icone import usar_icone
import re
from db.conexao import conectar_banco, tabela_empresa

class EmpresaCadastro(QtWidgets.QWidget):
    def __init__(self, banco_empresas):
        super().__init__()
        self.setWindowTitle('Apurado de ICMS - Cadastro de Empresas')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.banco_empresas = banco_empresas

        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(10)

        self.title_label = QtWidgets.QLabel('Cadastro de Empresa')
        self.title_label.setAlignment(QtCore.Qt.AlignLeft)
        self.title_label.setStyleSheet("font-size: 36px; font-weight: bold; color: #001F3F;")

        self.cnpj_label = QtWidgets.QLabel('CNPJ:')
        self.cnpj_label.setStyleSheet("font-size: 20px; font-weight: bold; color: #000000;")
        self.cnpj_input = QtWidgets.QLineEdit()
        self.cnpj_input.setInputMask('99.999.999/9999-99')
        self.cnpj_input.setPlaceholderText('Digite o CNPJ')
        self.cnpj_input.setStyleSheet("font-size: 20px; padding: 8px; border: 1px solid #ccc; border-radius: 5px; color: #000000;")

        self.razao_social_label = QtWidgets.QLabel('Razão Social:')
        self.razao_social_label.setStyleSheet("font-size: 20px; font-weight: bold; color: #000000;")
        self.razao_social_input = QtWidgets.QLineEdit()
        self.razao_social_input.setPlaceholderText('Digite a razão social')
        self.razao_social_input.setStyleSheet("font-size: 20px; padding: 8px; border: 1px solid #ccc; border-radius: 5px; color: #000000;")

        self.btn_cadastrar_empresa = QtWidgets.QPushButton('Cadastrar')
        self.btn_cadastrar_empresa.setStyleSheet(self._botao_estilo())
        self.btn_cadastrar_empresa.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.btn_cadastrar_empresa.clicked.connect(self.cadastrar_empresa)

        self.voltar_btn = QtWidgets.QPushButton('Voltar')
        self.voltar_btn.setStyleSheet(self._botao_estilo())
        self.voltar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.voltar_btn.clicked.connect(self.voltar)

        group_box = QtWidgets.QGroupBox()
        group_box.setStyleSheet("background-color: #FFFFFF; padding: 20px; border-radius: 10px;")
        group_box.setMaximumWidth(500)

        group_box_layout = QtWidgets.QVBoxLayout(group_box)
        group_box_layout.addWidget(self.title_label)
        group_box_layout.addWidget(self.cnpj_label)
        group_box_layout.addWidget(self.cnpj_input)
        group_box_layout.addWidget(self.razao_social_label)
        group_box_layout.addWidget(self.razao_social_input)
        group_box_layout.addWidget(self.btn_cadastrar_empresa)
        group_box_layout.addWidget(self.voltar_btn)

        center_layout = QtWidgets.QVBoxLayout()
        center_layout.addStretch()
        center_layout.addWidget(group_box, alignment=QtCore.Qt.AlignCenter)
        center_layout.addStretch()

        self.layout.addLayout(center_layout)

    def _botao_estilo(self):
        return """
            QPushButton {
                font-size: 20px;
                font-weight: bold;
                padding: 10px;
                background-color: #001F3F;
                color: #ffffff;
            }
            QPushButton:hover {
                background-color: #005588;
            }
        """

    def cadastrar_empresa(self):
        cnpj = self.cnpj_input.text().strip()
        razao_social = self.razao_social_input.text().strip()

        cnpj_numeros = re.sub(r'\D', '', cnpj)
        razao_formatada = razao_social.replace(" ", "_")

        if not razao_social or len(cnpj) != 18:
            mensagem_error("Preencha todos os campos corretamente.")
            return

        if re.match(r'^[\d_]', razao_formatada) or not re.match(r'^[a-zA-Z0-9_]+$', razao_formatada):
            mensagem_error("Razão Social inválida para nome de banco.")
            return

        # Inicia o processo de cadastro em segundo plano
        self.worker = CadastroEmpresaWorker(self.banco_empresas, cnpj_numeros, razao_social)
        self.worker.cadastro_finalizado.connect(self.cadastro_sucesso)
        self.worker.erro_ocorrido.connect(self.cadastro_erro)
        self.worker.start()

    def cadastro_sucesso(self, mensagem):
        mensagem_sucesso(mensagem)

    def cadastro_erro(self, erro):
        mensagem_error(f"Erro ao cadastrar: {erro}")

    def voltar(self):
        self.empresas = EmpresaWindow()
        usar_icone(self.empresas)
        self.empresas.showMaximized()
        self.close()


class CadastroEmpresaWorker(QtCore.QThread):
    cadastro_finalizado = QtCore.Signal(str)
    erro_ocorrido = QtCore.Signal(str)

    def __init__(self, banco, cnpj, razao_social):
        super().__init__()
        self.banco = banco
        self.cnpj = cnpj
        self.razao = razao_social

    def run(self):
        try:
            conexao = conectar_banco(self.banco)
            tabela_empresa(conexao)
            cursor = conexao.cursor()
            cursor.execute(f"USE {self.banco}")
            cursor.execute("INSERT INTO empresas (cnpj, razao_social) VALUES (%s, %s)", (self.cnpj, self.razao))
            conexao.commit()
            cursor.close()
            conexao.close()
            self.cadastro_finalizado.emit("Empresa cadastrada com sucesso.")
        except Exception as e:
            self.erro_ocorrido.emit(str(e))
