import os
import sys
import requests
from PIL import Image
from PySide6 import QtGui

def baixar_icone(url: str, caminho_png: str, caminho_ico: str):
    try:
        if not os.path.exists(caminho_png) or not os.path.exists(caminho_ico):
            print("[DEBUG] Baixando ícone...")
            response = requests.get(url, timeout=5)
            response.raise_for_status()

            with open(caminho_png, 'wb') as f:
                f.write(response.content)

            img = Image.open(caminho_png)
            img.save(caminho_ico, format="ICO")
        else:
            print("[DEBUG] Ícone já existe em cache.")
    except Exception as e:
        print(f"[ERRO] Falha ao baixar ou converter ícone: {e}")

def usar_icone(janela):
    url_icone = "https://assertivuscontabil.com.br/wp-content/uploads/2023/11/76.png"
    pasta_icones = "images"
    caminho_png = os.path.join(pasta_icones, "icone.png")
    caminho_ico = os.path.join(pasta_icones, "icone.ico")

    if not os.path.exists(pasta_icones):
        os.makedirs(pasta_icones)

    baixar_icone(url_icone, caminho_png, caminho_ico)

    if sys.platform == "win32" and os.path.exists(caminho_ico):
        janela.setWindowIcon(QtGui.QIcon(caminho_ico))
    elif os.path.exists(caminho_png):
        janela.setWindowIcon(QtGui.QIcon(caminho_png))
    else:
        print("[ERRO] Ícone não pôde ser carregado.")
