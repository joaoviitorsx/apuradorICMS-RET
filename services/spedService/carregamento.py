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
from .pos_processamento import etapas_pos_processamento
from services.fornecedorService import mensageiro as mensageiro_fornecedor

sem_limite = asyncio.Semaphore(3)

class Mensageiro(QObject):
    sinal_sucesso = Signal(str)
    sinal_erro = Signal(str)

def processar_sped_thread(empresa_id, progress_bar, label_arquivo, caminhos, janela=None, mensageiro=None):
    print(f"[DEBUG] Iniciando thread de processamento SPED com {len(caminhos)} arquivo(s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    result, mensagem_final = loop.run_until_complete(
        processar_sped(empresa_id, progress_bar, label_arquivo, caminhos, janela)
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

def iniciar_processamento_sped(empresa_id, progress_bar, label_arquivo, janela=None):
    print(f"[DEBUG] Solicitando seleção de arquivos SPED...")
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.", parent=janela)
        print(f"[DEBUG] Nenhum arquivo selecionado.")
        return

    mensageiro = Mensageiro()
    mensageiro.sinal_sucesso.connect(lambda texto: mensagem_sucesso(texto, parent=janela))
    mensageiro.sinal_erro.connect(lambda texto: mensagem_error(texto, parent=janela))
    mensageiro_fornecedor.sinal_log.connect(lambda texto: mensagem_sucesso(texto, parent=janela))
    mensageiro_fornecedor.sinal_erro.connect(lambda texto: mensagem_error(texto, parent=janela))

    print(f"[DEBUG] {len(caminhos)} arquivo(s) selecionado(s):")
    for i, caminho in enumerate(caminhos):
        print(f"[DEBUG]   {i+1}. {os.path.basename(caminho)} ({os.path.getsize(caminho)/1024:.1f} KB)")

    thread = threading.Thread(
        target=processar_sped_thread,
        args=(empresa_id, progress_bar, label_arquivo, caminhos, janela, mensageiro)
    )
    thread.start()
    print(f"[DEBUG] Thread de processamento SPED iniciada")

async def processar_sped(empresa_id, progress_bar, label_arquivo, caminhos, janela=None):
    progress_bar.setValue(0)
    print(f"[DEBUG] Iniciando processamento de {len(caminhos)} arquivo(s) SPED...")

    conexao_cheque = conectar_banco()
    if not conexao_cheque:
        return False, "Erro ao conectar ao banco"

    cursor_cheque = conexao_cheque.cursor()
    cursor_cheque.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor_cheque.fetchone():
        cursor_cheque.close()
        fechar_banco(conexao_cheque)
        return False, "Tributação não encontrada. Envie primeiro a tributação."
    cursor_cheque.close()
    fechar_banco(conexao_cheque)

    total = len(caminhos)
    progresso_por_arquivo = math.ceil(100 / total) if total > 0 else 100
    dados_gerais = []

    try:
        for i, caminho in enumerate(caminhos):
            nome_arquivo = os.path.basename(caminho)
            label_arquivo.setText(f"Processando arquivo {i+1}/{total}: {nome_arquivo}")

            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read().strip()

            print(f"[DEBUG] Lendo: {nome_arquivo}")
            print(f"[DEBUG] Tamanho conteúdo bruto: {len(conteudo)}")

            conteudo_processado = process_data(conteudo)
            print(f"[DEBUG] Resultado process_data: Tipo={type(conteudo_processado)}, Tamanho={len(conteudo_processado) if isinstance(conteudo_processado, list) else 'N/A'}")

            if isinstance(conteudo_processado, str):
                linhas = conteudo_processado.strip().splitlines()
            elif isinstance(conteudo_processado, list):
                linhas = conteudo_processado
            else:
                linhas = []

            print(f"[DEBUG] Adicionando {len(linhas)} linhas do arquivo {nome_arquivo}")
            dados_gerais.extend(linhas)

            progresso_atual = min((i + 1) * progresso_por_arquivo, 100)
            progress_bar.setValue(progresso_atual)
            await asyncio.sleep(0.1)

        conexao = conectar_banco()
        cursor = conexao.cursor()

        mensagem = await salvar_no_banco_em_lote(dados_gerais, cursor, conexao, empresa_id)
        conexao.commit()
        cursor.close()
        fechar_banco(conexao)

        if isinstance(mensagem, str) and not mensagem.lower().startswith(("falha", "erro")):
            await etapas_pos_processamento(empresa_id, progress_bar, janela_pai=janela)
            return True, mensagem
        else:
            return False, mensagem or "Erro durante salvamento em lote."

    except Exception as e:
        import traceback
        print("[ERRO] Falha no processar_sped:", traceback.format_exc())
        return False, f"Erro inesperado durante o processamento: {e}"

    finally:
        try:
            fechar_banco(conexao)
        except:
            pass
        progress_bar.setValue(100)
        await asyncio.sleep(0.5)
        progress_bar.setValue(0)
        label_arquivo.setText("Processamento finalizado.")