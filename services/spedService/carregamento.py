import asyncio
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso
import threading
from .salvamento import salvar_no_banco

def processar_sped_thread(nome_banco, progress_bar, label_arquivo, caminhos):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(processar_sped(nome_banco, progress_bar, label_arquivo, caminhos))
    loop.close()

def iniciar_processamento_sped(nome_banco, progress_bar, label_arquivo):
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.")
        return
    thread = threading.Thread(target=processar_sped_thread, args=(nome_banco, progress_bar, label_arquivo, caminhos))
    thread.start()


async def processar_sped(nome_banco, progress_bar, label_arquivo, caminhos):
    import time
    inicio = time.time()
    progress_bar.setValue(0)
    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        return

    cursor = conexao.cursor()
    cursor.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor.fetchone():
        mensagem_error("Tributação não encontrada. Envie primeiramente a tributação.")
        return
    cursor.close()

    total = len(caminhos)
    try:
        cursor = conexao.cursor()
        for i, caminho in enumerate(caminhos):
            try:
                with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                    conteudo = arquivo.read()
                    progress_bar.setValue(2)
                    conteudo_processado = process_data(conteudo)

                    label_arquivo.setText(f'Processando arquivo {i+1}/{total}')
                    await salvar_no_banco(conteudo_processado, cursor, nome_banco, progress_bar)

                    fim = time.time()
                    print(f"[DEBUG] Tempo total de processamento SPED: {fim - inicio:.2f} segundos")

            except Exception as e:
                mensagem_error(f"Erro ao processar o arquivo {caminho}: {e}")
                return

        mensagem_sucesso("Arquivos processados com sucesso.")
    except Exception as e:
        mensagem_error(f"Erro inesperado: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)
