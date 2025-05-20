import asyncio
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
import threading
import os
from .salvamento import salvar_no_banco_em_lote
import math

sem_limite = asyncio.Semaphore(3)

def processar_sped_thread(nome_banco, progress_bar, label_arquivo, caminhos, janela=None):
    print(f"[DEBUG] Iniciando thread de processamento SPED com {len(caminhos)} arquivo(s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(processar_sped(nome_banco, progress_bar, label_arquivo, caminhos, janela))
    print(f"[DEBUG] Thread de processamento SPED finalizada")
    loop.close()

def iniciar_processamento_sped(nome_banco, progress_bar, label_arquivo, janela=None):
    print(f"[DEBUG] Solicitando seleção de arquivos SPED...")
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.", parent=janela)
        print(f"[DEBUG] Nenhum arquivo selecionado.")
        return
    print(f"[DEBUG] {len(caminhos)} arquivo(s) selecionado(s):")
    for i, caminho in enumerate(caminhos):
        print(f"[DEBUG]   {i+1}. {os.path.basename(caminho)} ({os.path.getsize(caminho)/1024:.1f} KB)")
    thread = threading.Thread(target=processar_sped_thread, args=(nome_banco, progress_bar, label_arquivo, caminhos, janela))
    thread.start()
    print(f"[DEBUG] Thread de processamento iniciada")

async def processar_sped(nome_banco, progress_bar, label_arquivo, caminhos, janela):
    import time
    inicio_total = time.time()
    progress_bar.setValue(0)
    print(f"[DEBUG] Iniciando processamento de {len(caminhos)} arquivo(s) SPED...")

    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.", parent=janela)
        return

    cursor = conexao.cursor()
    cursor.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor.fetchone():
        mensagem_error("Tributação não encontrada. Envie primeiramente a tributação.", parent=janela)
        return
    cursor.close()

    total = len(caminhos)
    progresso_por_arquivo = math.ceil(100 / total) if total > 0 else 100

    try:
        tasks = []
        for i, caminho in enumerate(caminhos):
            tasks.append(
                processar_arquivo(
                    caminho, nome_banco, progress_bar, label_arquivo, i + 1, total, progresso_por_arquivo, janela
                )
            )
        await asyncio.gather(*tasks)

        mensagem_sucesso("Todos os arquivos SPED foram processados com sucesso.", parent=janela)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        mensagem_error(f"Erro inesperado durante o processamento: {e}", parent=janela)
    finally:
        fechar_banco(conexao)
        progress_bar.setValue(100)
        await asyncio.sleep(0.5)
        progress_bar.setValue(0)
        label_arquivo.setText("Processamento finalizado.")

async def processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, indice, total, progresso_por_arquivo, janela):
    async with sem_limite:
        import time
        inicio = time.time()
        nome_arquivo = os.path.basename(caminho)
        label_arquivo.setText(f"Processando arquivo {indice}/{total}: {nome_arquivo}")

        try:
            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read()

            conteudo_processado = process_data(conteudo)

            conexao = conectar_banco(nome_banco)
            cursor = conexao.cursor()

            sucesso = await salvar_no_banco_em_lote(conteudo_processado, cursor, nome_banco)
            conexao.commit()
            cursor.close()
            fechar_banco(conexao)

            if sucesso:
                mensagem_sucesso(f"Arquivo {nome_arquivo} processado com sucesso.", parent=janela)
            else:
                mensagem_error(f"Falha ao salvar dados do arquivo {nome_arquivo}.", parent=janela)

            progresso_atual = min(indice * progresso_por_arquivo, 100)
            progress_bar.setValue(progresso_atual)

        except Exception as e:
            import traceback
            print(traceback.format_exc())
            mensagem_error(f"Erro ao processar o arquivo {nome_arquivo}: {e}", parent=janela)
