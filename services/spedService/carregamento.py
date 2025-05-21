import asyncio
import threading
import os
import math
from PySide6.QtWidgets import QFileDialog
from PySide6.QtCore import QObject, Signal

from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from .salvamento import salvar_no_banco_em_lote

sem_limite = asyncio.Semaphore(3)

class Mensageiro(QObject):
    sinal_sucesso = Signal(str)
    sinal_erro = Signal(str)

def processar_sped_thread(nome_banco, progress_bar, label_arquivo, caminhos, janela=None, mensageiro=None):
    print(f"[DEBUG] Iniciando thread de processamento SPED com {len(caminhos)} arquivo(s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    result, mensagem_final = loop.run_until_complete(
        processar_sped(nome_banco, progress_bar, label_arquivo, caminhos, janela)
    )

    print(f"[DEBUG] Thread de processamento SPED finalizada")

    if mensagem_final and mensageiro:
        if result:
            print("[DEBUG] Emitindo sinal de sucesso")
            mensageiro.sinal_sucesso.emit(mensagem_final)
        else:
            print("[DEBUG] Emitindo sinal de erro")
            mensageiro.sinal_erro.emit(mensagem_final)

    progress_bar.setValue(0)
    loop.close()


def iniciar_processamento_sped(nome_banco, progress_bar, label_arquivo, janela=None):
    print(f"[DEBUG] Solicitando seleção de arquivos SPED...")
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.", parent=janela)
        print(f"[DEBUG] Nenhum arquivo selecionado.")
        return

    mensageiro = Mensageiro()
    mensageiro.sinal_sucesso.connect(lambda texto: mensagem_sucesso(texto, parent=janela))
    mensageiro.sinal_erro.connect(lambda texto: mensagem_error(texto, parent=janela))

    print(f"[DEBUG] {len(caminhos)} arquivo(s) selecionado(s):")
    for i, caminho in enumerate(caminhos):
        print(f"[DEBUG]   {i+1}. {os.path.basename(caminho)} ({os.path.getsize(caminho)/1024:.1f} KB)")

    thread = threading.Thread(target=processar_sped_thread, args=(nome_banco, progress_bar, label_arquivo, caminhos, janela, mensageiro))
    thread.start()
    print(f"[DEBUG] Thread de processamento SPED iniciada")

async def processar_sped(nome_banco, progress_bar, label_arquivo, caminhos, janela):
    import time
    inicio_total = time.time()
    progress_bar.setValue(0)
    print(f"[DEBUG] Iniciando processamento de {len(caminhos)} arquivo(s) SPED...")

    conexao = conectar_banco(nome_banco)
    if not conexao:
        return False, "Erro ao conectar ao banco"

    cursor = conexao.cursor()
    cursor.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor.fetchone():
        cursor.close()
        fechar_banco(conexao)
        return False, "Tributação não encontrada. Envie primeiro a tributação."

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
        resultados = await asyncio.gather(*tasks)
        sucesso_total = all(r[0] for r in resultados)
        mensagens = [r[1] for r in resultados if r[1]]
        mensagem_final = "\n".join(mensagens)
        return sucesso_total, mensagem_final

    except Exception as e:
        import traceback
        print("[ERRO] Falha no processar_sped:", traceback.format_exc())
        return False, f"Erro inesperado durante o processamento: {e}"

    finally:
        fechar_banco(conexao)
        progress_bar.setValue(100)
        await asyncio.sleep(0.5)
        progress_bar.setValue(0)
        label_arquivo.setText("Processamento finalizado.")

async def processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, indice, total, progresso_por_arquivo, janela):
    async with sem_limite:
        import time
        nome_arquivo = os.path.basename(caminho)
        label_arquivo.setText(f"Processando arquivo {indice}/{total}: {nome_arquivo}")

        try:
            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read()

            conteudo_processado = process_data(conteudo)

            conexao = conectar_banco(nome_banco)
            cursor = conexao.cursor()

            mensagem = await salvar_no_banco_em_lote(conteudo_processado, cursor, nome_banco)

            conexao.commit()
            cursor.close()
            fechar_banco(conexao)

            progresso_atual = min(indice * progresso_por_arquivo, 100)
            progress_bar.setValue(progresso_atual)

            if isinstance(mensagem, str) and not mensagem.lower().startswith(("falha", "erro")):
                return True, mensagem
            else:
                return False, mensagem or "Erro desconhecido durante o salvamento."

        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return False, f"Erro ao processar o arquivo {nome_arquivo}: {e}"
