import asyncio
from PySide6 import QtWidgets, QtGui, QtCore
from utils.icone import usar_icone
from services.tributacaoService import enviar_tributacao
from services.spedService.carregamento import processar_sped
from services.spedService import processar_sped
from services.exportacaoService import exportar_resultado

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, empresa):
        super().__init__()
        self.setWindowTitle(f'Apurado de ICMS Varejo - Assertivus Contábil - {empresa}')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.empresa = empresa
        self.empresa_sem_espacos = self.empresa.replace(" ", "_")

        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QtWidgets.QVBoxLayout(self.central_widget)

        self.frame_db = QtWidgets.QGroupBox('Empresa')
        self.frame_layout = QtWidgets.QVBoxLayout(self.frame_db)

        self.label_nome_banco = QtWidgets.QLabel(f'{self.empresa}')
        self.label_nome_banco.setStyleSheet('font-size: 20px; font-weight: bold; color: #ffffff; font-family: Arial;')
        self.frame_layout.addWidget(self.label_nome_banco)

        self.layout.addWidget(self.frame_db)

        self._criar_botoes()

        self.label_arquivo = QtWidgets.QLabel('Nenhum arquivo selecionado')
        self.layout.addWidget(self.label_arquivo)

        self.stack_layout = QtWidgets.QStackedLayout()
        self.layout.addLayout(self.stack_layout)

        self.imagem_placeholder = QtWidgets.QLabel()
        self.imagem_placeholder.setPixmap(QtGui.QPixmap("images/logo.png").scaled(300, 300, QtCore.Qt.KeepAspectRatio))
        self.imagem_placeholder.setAlignment(QtCore.Qt.AlignCenter)
        self.stack_layout.addWidget(self.imagem_placeholder)

        self.progress_bar = QtWidgets.QProgressBar()
        self.layout.addWidget(self.progress_bar)

        self._criar_seletor_mes_ano()

    def _criar_botoes(self):
        botao_frame = QtWidgets.QHBoxLayout()
        self.layout.addLayout(botao_frame)

        botoes_info = [
            ("Enviar Tributação", self._enviar_tributacao),
            ("Inserir Sped", self._processar_sped),
        ]

        for texto, comando in botoes_info:
            btn = QtWidgets.QPushButton(texto)
            btn.setFont(QtGui.QFont("Arial", 14))
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #001F3F;
                    color: white;
                    border: none;
                    padding: 5px 10px;
                    border-radius: 5px;
                }
                QPushButton:hover {
                    background-color: #2E236C;
                }
            """)
            btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            btn.clicked.connect(comando)
            botao_frame.addWidget(btn)

    def _enviar_tributacao(self):
        enviar_tributacao(self.empresa_sem_espacos, self.progress_bar)

    def _processar_sped(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            processar_sped(self.empresa_sem_espacos, self.progress_bar, self.label_arquivo)
        )
        loop.close()

    def _criar_seletor_mes_ano(self):
        mes_frame = QtWidgets.QHBoxLayout()
        self.layout.addLayout(mes_frame)

        self.mes_var = QtWidgets.QComboBox()
        self.mes_var.addItems(['Escolha o mês', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'])
        self.mes_var.setStyleSheet("font-size: 16px; padding: 5px; color: #000000; background-color: #ffffff;")
        mes_frame.addWidget(self.mes_var)

        self.ano_var = QtWidgets.QComboBox()
        ano_atual = QtCore.QDate.currentDate().year()
        self.ano_var.addItems(['Escolha o ano', str(ano_atual-2), str(ano_atual-1), str(ano_atual), str(ano_atual+1)])
        self.ano_var.setStyleSheet("font-size: 16px; padding: 5px; color: #000000; background-color: #ffffff;")
        mes_frame.addWidget(self.ano_var)

        btn_baixar_tabela = QtWidgets.QPushButton("Baixar Tabela")
        btn_baixar_tabela.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                padding: 5px;
                border-radius: 5px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #218838;
            }
        """)
        btn_baixar_tabela.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        btn_baixar_tabela.clicked.connect(self._baixar_tabela)
        mes_frame.addWidget(btn_baixar_tabela)

    def _baixar_tabela(self):
        mes = self.mes_var.currentText()
        ano = self.ano_var.currentText()
        if mes == "Escolha o mês" or ano == "Escolha o ano":
            from utils.mensagem import mensagem_aviso
            mensagem_aviso("Selecione um mês e um ano válidos.")
            return
        exportar_resultado(self.empresa_sem_espacos, mes, ano, self.progress_bar)