from utils.icone import usar_icone
from PySide6 import QtWidgets, QtGui, QtCore
from PySide6.QtWidgets import QMessageBox
import os

def mensagem_error(mensagem, parent=None):
    msg_erro = QtWidgets.QMessageBox(parent)
    msg_erro.setIcon(QtWidgets.QMessageBox.Critical)
    msg_erro.setWindowTitle("Erro")
    msg_erro.setText(mensagem)
    msg_erro.setStyleSheet("background-color: #001F3F; color: #ffffff; font-size: 16px; font-weight: bold;")
    usar_icone(msg_erro) 
    msg_erro.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))    
    msg_erro.exec()

def mensagem_sucesso(mensagem, parent=None):
    msg_sucesso = QtWidgets.QMessageBox(parent)
    msg_sucesso.setIcon(QtWidgets.QMessageBox.Information)
    msg_sucesso.setWindowTitle("Sucesso")
    msg_sucesso.setText(mensagem)
    msg_sucesso.setStyleSheet("background-color: #001F3F; color: #ffffff; font-size: 16px; font-weight: bold;")
    usar_icone(msg_sucesso) 
    msg_sucesso.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))    
    msg_sucesso.exec()

def mensagem_aviso(mensagem, parent=None):
    msg_aviso = QtWidgets.QMessageBox(parent)
    msg_aviso.setIcon(QtWidgets.QMessageBox.Warning)
    msg_aviso.setWindowTitle("Aviso")
    msg_aviso.setText(mensagem)
    msg_aviso.setStyleSheet("background-color: #001F3F; color: #ffffff; font-size: 16px; font-weight: bold;")
    usar_icone(msg_aviso) 
    msg_aviso.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))    
    msg_aviso.exec()