from PySide6 import QtWidgets, QtGui, QtCore
import sys
import traceback

_app_instance = None

def set_app_instance(app):
    global _app_instance
    _app_instance = app

def mensagem_error(texto):
    print(f"[ERROR] {texto}")
    _exibir_na_thread_principal("Erro", texto, QtWidgets.QMessageBox.Critical)

def mensagem_sucesso(texto):
    print(f"[SUCESSO] {texto}")
    _exibir_na_thread_principal("Sucesso", texto, QtWidgets.QMessageBox.Information)

def mensagem_aviso(texto):
    print(f"[AVISO] {texto}")
    _exibir_na_thread_principal("Aviso", texto, QtWidgets.QMessageBox.Warning)

def _exibir_na_thread_principal(titulo, texto, icone):
    try:
        if QtCore.QThread.currentThread() == QtWidgets.QApplication.instance().thread():
            _mostrar_mensagem(titulo, texto, icone)
        else:
            app = QtWidgets.QApplication.instance()
            if app:
                QtCore.QTimer.singleShot(0, lambda: _mostrar_mensagem_segura(titulo, texto, icone))
            else:
                print(f"[ERRO] Não foi possível obter a instância do QApplication")
    except Exception as e:
        print(f"[ERRO CRÍTICO] Falha ao agendar mensagem: {e}")
        print(f"Mensagem que seria exibida: [{titulo}] {texto}")
        print(traceback.format_exc())
        
        try:
            app = QtWidgets.QApplication.instance()
            if app:
                QtCore.QTimer.singleShot(100, lambda: _mostrar_mensagem_segura(titulo, texto, icone))
        except Exception as e2:
            print(f"[ERRO FATAL] Segunda falha ao agendar mensagem: {e2}")
            print(traceback.format_exc())

def _mostrar_mensagem_segura(titulo, texto, icone):
    try:
        _mostrar_mensagem(titulo, texto, icone)
    except Exception as e:
        print(f"[ERRO FATAL] Impossível exibir mensagem: {e}")
        print(traceback.format_exc())

def _mostrar_mensagem(titulo, texto, icone):
    try:
        msg = QtWidgets.QMessageBox()
        msg.setIcon(icone)
        msg.setWindowTitle(titulo)
        msg.setText(texto)
        msg.setStyleSheet("background-color: #001F3F; color: #ffffff; font-size: 16px; font-weight: bold;")
        msg.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        msg.setWindowFlags(msg.windowFlags() | QtCore.Qt.WindowStaysOnTopHint)
        
        msg.exec()
    except Exception as e:
        print(f"[ERRO UI] Falha ao mostrar caixa de diálogo: {e}")
        print(traceback.format_exc())

def notificacao(texto, duracao=3000):
    print(f"[NOTIFICAÇÃO] {texto}")
    try:
        app = QtWidgets.QApplication.instance()
        if app:
            QtCore.QTimer.singleShot(0, lambda: _exibir_notificacao(texto, duracao))
        else:
            print(f"[ERRO] Não foi possível obter a instância do QApplication para notificação")
    except Exception as e:
        print(f"[ERRO NOTIFICAÇÃO] Falha ao mostrar notificação: {e}")
        print(traceback.format_exc())

def _exibir_notificacao(texto, duracao):
    try:
        notif = QtWidgets.QLabel(texto)
        notif.setStyleSheet("""
            background-color: rgba(0, 31, 63, 0.9); 
            color: white; 
            font-size: 14px; 
            font-weight: bold;
            padding: 10px;
            border-radius: 5px;
        """)
        notif.setWindowFlags(QtCore.Qt.FramelessWindowHint | QtCore.Qt.WindowStaysOnTopHint)
        notif.setAlignment(QtCore.Qt.AlignCenter)

        if _app_instance:
            main_window = None
            for widget in _app_instance.topLevelWidgets():
                if isinstance(widget, QtWidgets.QMainWindow):
                    main_window = widget
                    break
            if main_window:
                geom = main_window.geometry()
                notif.resize(300, 80)
                notif.move(geom.x() + geom.width() - 320, geom.y() + geom.height() - 100)

        notif.show()
        QtCore.QTimer.singleShot(duracao, notif.deleteLater)
    except Exception as e:
        print(f"[ERRO NOTIFICAÇÃO] Falha ao exibir notificação: {e}")
        print(traceback.format_exc())