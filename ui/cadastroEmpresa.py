from PySide6 import QtWidgets, QtGui, QtCore
from db.conexao import conectar_banco
from utils.icone import usar_icone
from utils.mensagem import mensagem_error, mensagem_sucesso
import re

class EmpresaCadastro(QtWidgets.QWidget):
    def __init__(self, banco_empresas):
        super().__init__()
        self.setWindowTitle('Apurado de ICMS - Cadastro de Empresas')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.banco_empresas = banco_empresas

        self.layout = QtWidgets.QVBoxLayout()
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
        self.btn_cadastrar_empresa.setStyleSheet(self._estilo_botao())
        self.btn_cadastrar_empresa.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.btn_cadastrar_empresa.clicked.connect(self.cadastrar_empresa)

        self.voltar_btn = QtWidgets.QPushButton('Voltar')
        self.voltar_btn.setStyleSheet(self._estilo_botao())
        self.voltar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.voltar_btn.clicked.connect(self.voltar)

        group_box = QtWidgets.QGroupBox()
        group_box.setStyleSheet("background-color: #FFFFFF; padding: 20px; border-radius: 10px;")
        group_box.setMaximumWidth(500)
        group_box.setMinimumWidth(500)

        group_box_layout = QtWidgets.QVBoxLayout()
        group_box_layout.setSpacing(10)
        group_box_layout.setContentsMargins(10, 10, 10, 10)
        group_box_layout.addWidget(self.title_label)
        group_box_layout.addWidget(self.cnpj_label)
        group_box_layout.addWidget(self.cnpj_input)
        group_box_layout.addWidget(self.razao_social_label)
        group_box_layout.addWidget(self.razao_social_input)
        group_box_layout.addWidget(self.btn_cadastrar_empresa)
        group_box_layout.addWidget(self.voltar_btn)
        group_box.setLayout(group_box_layout)

        center_layout = QtWidgets.QVBoxLayout()
        center_layout.addStretch()
        center_layout.addWidget(group_box, alignment=QtCore.Qt.AlignCenter)
        center_layout.addStretch()

        main_layout = QtWidgets.QVBoxLayout()
        main_layout.addLayout(center_layout)

        self.setLayout(main_layout)

    def _estilo_botao(self):
        return """QPushButton {
            font-size: 20px;
            font-weight: bold;
            padding: 10px;
            background-color: #001F3F;
            color: #ffffff;
        }
        QPushButton:hover {
            background-color: #005588;
            color:#ffffff;
        }"""

    def cadastrar_empresa(self):
        cnpj = self.cnpj_input.text().strip()
        razao_social = self.razao_social_input.text().strip()

        cnpj_so_numeros = re.sub(r'\D', '', cnpj)
        razao_social_modificada = razao_social.replace(" ", "_")

        if re.match(r'^[\d_]', razao_social_modificada):
            mensagem_error("O nome do banco de dados não pode começar com um número ou underline.")
            return

        if not re.match(r'^[a-zA-Z0-9_]+$', razao_social_modificada):
            mensagem_error("O nome do banco de dados só pode conter letras, números e underline.")
            return

        if len(cnpj) != 18:
            mensagem_error("CNPJ inválido.")
            return

        if len(razao_social) < 1:
            mensagem_error("Razão social inválida.")
            return

        try:
            conexao = conectar_banco(self.banco_empresas)
            cursor = conexao.cursor()
            cursor.execute(f"USE {self.banco_empresas}")
            cursor.execute("INSERT INTO empresas (cnpj, razao_social) VALUES (%s, %s)", (cnpj_so_numeros, razao_social))
            conexao.commit()
            mensagem_sucesso("Empresa cadastrada com sucesso.")
        except Exception as e:
            mensagem_error(f"Ocorreu um erro ao cadastrar a empresa: {e}")
        finally:
            if conexao.is_connected():
                cursor.close()
                conexao.close()

    def voltar(self):
        from ui.telaEmpresa import EmpresaWindow
        self.empresas = EmpresaWindow()
        usar_icone(self.empresas)
        self.empresas.showMaximized()
        self.close()