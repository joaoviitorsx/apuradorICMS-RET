from PySide6.QtWidgets import QDialog, QLabel, QPushButton, QVBoxLayout, QFrame, QApplication
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QScreen
import winsound
class PopupMensagem(QDialog):
    def __init__(self, titulo, mensagem, cor_fundo="#1e1e2f", parent=None, duracao=0):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.Dialog)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setModal(True)

        layout_externo = QVBoxLayout(self)
        layout_externo.setContentsMargins(0, 0, 0, 0)
        layout_externo.setAlignment(Qt.AlignCenter)

        card = QFrame()
        card.setStyleSheet(f"""
            QFrame {{
                background-color: {cor_fundo};
                border-radius: 12px;
                min-width: 400px;
                max-width: 600px;
                padding: 30px;
            }}
        """)
        layout_card = QVBoxLayout(card)
        layout_card.setSpacing(20)
        layout_card.setAlignment(Qt.AlignCenter)

        label_titulo = QLabel(titulo)
        label_titulo.setObjectName("titulo")
        label_titulo.setAlignment(Qt.AlignCenter)
        label_titulo.setStyleSheet("font-size: 18px; font-weight: bold; color: #ffc107;")

        label_msg = QLabel(mensagem)
        label_msg.setObjectName("mensagem")
        label_msg.setAlignment(Qt.AlignCenter)
        label_msg.setWordWrap(True)
        label_msg.setStyleSheet("font-size: 15px; color: #ffffff;")

        botao = QPushButton("OK")
        botao.setStyleSheet("""
            QPushButton {
                background-color: #0056b3;
                color: white;
                border-radius: 6px;
                font-weight: bold;
                padding: 12px 22px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #006fe6;
            }
        """)
        botao.clicked.connect(self.accept)
        botao.setCursor(Qt.PointingHandCursor)

        layout_card.addWidget(label_titulo)
        layout_card.addWidget(label_msg)
        layout_card.addWidget(botao, alignment=Qt.AlignCenter)

        layout_externo.addStretch()
        layout_externo.addWidget(card, alignment=Qt.AlignCenter)
        layout_externo.addStretch()

        self.resize(600, 300)
        self.centralizar()

        if duracao > 0:
            QTimer.singleShot(duracao, self.close)

    def centralizar(self):
        if self.parent() and isinstance(self.parent(), QFrame):
            parent_geom = self.parent().frameGeometry()
            x = parent_geom.x() + (parent_geom.width() - self.width()) // 2
            y = parent_geom.y() + (parent_geom.height() - self.height()) // 2
            self.move(x, y)
        else:
            screen = QApplication.primaryScreen()
            screen_rect = screen.availableGeometry()
            self.move(
                screen_rect.center().x() - self.width() // 2,
                screen_rect.center().y() - self.height() // 2
            )

    @staticmethod
    def sucesso(mensagem, parent=None):
        PopupMensagem("✅ Sucesso", mensagem, "#1e1e2f", parent).exec_()
    
    @staticmethod
    def erro(mensagem, parent=None):
        winsound.MessageBeep(winsound.MB_ICONHAND)
        PopupMensagem("❌ Erro", mensagem, "#3c1e1e", parent).exec_()
    
    @staticmethod
    def aviso(mensagem, parent=None):
        winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        PopupMensagem("⚠️ Aviso", mensagem, "#3c341e", parent).exec_()


def mensagem_sucesso(msg, parent=None):
    PopupMensagem.sucesso(msg, parent)

def mensagem_error(msg, parent=None):
    PopupMensagem.erro(msg, parent)

def mensagem_aviso(msg, parent=None):
    PopupMensagem.aviso(msg, parent)

def mensagem_segura(func, texto, parent=None):
    QTimer.singleShot(0, lambda: func(texto, parent=parent))