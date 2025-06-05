import sys
import time
import signal
from PySide6 import QtWidgets
from PySide6.QtCore import QCoreApplication
from ui.telaEmpresa import EmpresaWindow
from utils.icone import usar_icone

def main():
    inicio = time.time()

    app = QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(lambda: print("Sistema encerrando..."))

    janela = EmpresaWindow()
    usar_icone(janela)
    janela.showMaximized()

    fim = time.time()
    print(f"[DEBUG] Tempo de abertura da janela inicial: {fim - inicio:.2f} segundos")

    sys.exit(app.exec())

def sinal_encerramento(sig, frame):
    print("Encerrando o sistema por sinal (Ctrl+C ou kill)...")
    QCoreApplication.quit()

signal.signal(signal.SIGINT, sinal_encerramento)

if __name__ == '__main__':
    main()
