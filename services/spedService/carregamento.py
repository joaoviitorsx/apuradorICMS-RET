import asyncio
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso
import threading
from .salvamento import salvar_no_banco_em_lote

sem_limite = asyncio.Semaphore(3)

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
    inicio_total = time.time()
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
    progresso_por_arquivo = int(100 / total) if total > 0 else 100

    try:
        tasks = []
        for i, caminho in enumerate(caminhos):
            tasks.append(processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, i + 1, total, progresso_por_arquivo))
        await asyncio.gather(*tasks)

        mensagem_sucesso("Todos os arquivos SPED foram processados com sucesso.")
        fim_total = time.time()
        print(f"[DEBUG] Tempo total de processamento SPED: {fim_total - inicio_total:.2f} segundos")

    except Exception as e:
        mensagem_error(f"Erro inesperado: {e}")
    finally:
        fechar_banco(conexao)


async def processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, indice, total, progresso_por_arquivo):
    async with sem_limite:
        import time
        inicio = time.time()
        label_arquivo.setText(f'Processando arquivo {indice}/{total}')
        try:
            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read()
                conteudo_processado = process_data(conteudo)

            conexao = conectar_banco(nome_banco)
            cursor = conexao.cursor()
            await salvar_no_banco_em_lote(conteudo_processado, cursor, nome_banco)
            conexao.commit()
            cursor.close()
            fechar_banco(conexao)

            fim = time.time()
            print(f"[DEBUG] Arquivo {indice}/{total} processado em: {fim - inicio:.2f} segundos")

            progresso_atual = min(indice * progresso_por_arquivo, 100)
            progress_bar.setValue(progresso_atual)

        except Exception as e:
            mensagem_error(f"Erro ao processar o arquivo {caminho}: {e}")
