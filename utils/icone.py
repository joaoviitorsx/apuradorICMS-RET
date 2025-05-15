import os
import sys
import requests
from PIL import Image
from PySide6 import QtGui
def baixar_icone(url, caminho):
    dir_name = os.path.dirname(caminho)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    
    resposta = requests.get(url)
    with open(caminho, 'wb') as arquivo:
        arquivo.write(resposta.content)

def usar_icone(janela):
        url_icone = "https://assertivuscontabil.com.br/wp-content/uploads/2023/11/76.png"  
        caminho_icone = "images/icone.png" 
        
        baixar_icone(url_icone, caminho_icone)
        icon = Image.open(caminho_icone)
        icon.save(os.path.splitext(caminho_icone)[0] + '.ico', format="ICO")
        
        if os.path.exists(caminho_icone):
            if sys.platform == "win32":
                janela.setWindowIcon(QtGui.QIcon(os.path.splitext(caminho_icone)[0] + '.ico'))
            else:
                janela.setWindowIcon(QtGui.QIcon(caminho_icone))
        else:
            print(f"Erro: Arquivo de ícone não encontrado em {caminho_icone}")