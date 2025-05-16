import os
import sys
from PySide6 import QtGui

def usar_icone(janela):
    caminho_png = os.path.join("images", "icone.png")
    caminho_ico = os.path.join("images", "icone.ico")

    # Usa .ico no Windows e .png no Linux/macOS
    if sys.platform == "win32" and os.path.exists(caminho_ico):
        janela.setWindowIcon(QtGui.QIcon(caminho_ico))
    elif os.path.exists(caminho_png):
        janela.setWindowIcon(QtGui.QIcon(caminho_png))
    else:
        print("[ERRO] Ícone não encontrado nos caminhos esperados.")
