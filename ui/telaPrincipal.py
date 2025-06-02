import asyncio
from PySide6 import QtWidgets, QtGui, QtCore
from PySide6.QtWidgets import QMessageBox, QDialog
from utils.icone import usar_icone
from services.tributacaoService import enviar_tributacao
from services.spedService.carregamento import iniciar_processamento_sped
from services.spedService import sinal_popup
from ui.popupAliquota import PopupAliquota
from services.exportacaoService import exportar_resultado
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from services.spedService.verificacoes import sinal_popup, verificar_e_abrir_popup_aliquota

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, empresa, empresa_id):
        super().__init__()
        self.setWindowTitle(f'Apurado de ICMS Varejo - Assertivus Contábil - {empresa}')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.empresa = empresa             
        self.empresa_id = empresa_id        

        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QtWidgets.QVBoxLayout(self.central_widget)

        self._setup_empresa_header()
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

        def abrir_popup_aliquota(empresa_id, janela_pai=None):
            popup = PopupAliquota(empresa_id, janela_pai)
            resultado = popup.exec()
            sinal_popup.resultado_popup = resultado
            if sinal_popup.event_loop and sinal_popup.event_loop.isRunning():
                sinal_popup.event_loop.quit()
                mensagem_aviso("Popup fechado, verifique as alíquotas preenchidas.", parent=janela_pai)

        sinal_popup.abrir_popup_signal.connect(abrir_popup_aliquota)

        self._criar_seletor_mes_ano()
        
    def _setup_empresa_header(self):
        self.frame_db = QtWidgets.QGroupBox('Empresa')
        self.frame_layout = QtWidgets.QVBoxLayout(self.frame_db)

        self.label_empresa = QtWidgets.QLabel(f'{self.empresa} (ID: {self.empresa_id})')
        self.label_empresa.setStyleSheet('font-size: 20px; font-weight: bold; color: #ffffff; font-family: Arial;')
        self.frame_layout.addWidget(self.label_empresa)

        self.layout.addWidget(self.frame_db)

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
        enviar_tributacao(self.empresa_id, self.progress_bar)

    def _processar_sped(self):
        self.progress_bar.setValue(0)
        iniciar_processamento_sped(self.empresa_id, self.progress_bar, self.label_arquivo, self)

    def _criar_seletor_mes_ano(self):
        mes_frame = QtWidgets.QHBoxLayout()
        self.layout.addLayout(mes_frame)

        self.mes_var = QtWidgets.QComboBox()
        self.mes_var.addItems(['Escolha o mês'] + [f"{i:02}" for i in range(1, 13)])
        self.mes_var.setStyleSheet("font-size: 16px; padding: 5px; color: #000000; background-color: #ffffff;")
        mes_frame.addWidget(self.mes_var)

        self.ano_var = QtWidgets.QComboBox()
        ano_atual = QtCore.QDate.currentDate().year()
        self.ano_var.addItems(['Escolha o ano'] + [str(ano_atual - i) for i in range(0, 4)][::-1])
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
            mensagem_aviso("Selecione um mês e um ano válidos.")
            return

        self.progress_bar.setValue(0)
        exportar_resultado(self.empresa_id, mes, ano, self.progress_bar)
        self.progress_bar.setValue(0)

