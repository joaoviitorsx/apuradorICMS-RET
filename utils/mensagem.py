from utils.icone import usar_icone
from PySide6 import QtWidgets, QtGui, QtCore

def _mostrar_mensagem(tipo: str, mensagem: str):
    msg_box = QtWidgets.QMessageBox()

    if tipo == 'erro':
        msg_box.setIcon(QtWidgets.QMessageBox.Critical)
        msg_box.setWindowTitle("Erro")
    elif tipo == 'sucesso':
        msg_box.setIcon(QtWidgets.QMessageBox.Information)
        msg_box.setWindowTitle("Sucesso")
    elif tipo == 'aviso':
        msg_box.setIcon(QtWidgets.QMessageBox.Warning)
        msg_box.setWindowTitle("Aviso")
    
    msg_box.setText(mensagem)
    msg_box.setStyleSheet("background-color: #001F3F; color: #ffffff; font-size: 16px; font-weight: bold;")
    usar_icone(msg_box)
    msg_box.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
    msg_box.exec()

def mensagem_error(mensagem: str):
    _mostrar_mensagem('erro', mensagem)

def mensagem_sucesso(mensagem: str):
    _mostrar_mensagem('sucesso', mensagem)

def mensagem_aviso(mensagem: str):
    _mostrar_mensagem('aviso', mensagem)
