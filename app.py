import sys
from PySide6 import QtWidgets
from ui.telaEmpresa import EmpresaWindow
from utils.icone import usar_icone

def main():
    app = QtWidgets.QApplication(sys.argv)
    janela = EmpresaWindow()
    usar_icone(janela)
    janela.showMaximized()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
