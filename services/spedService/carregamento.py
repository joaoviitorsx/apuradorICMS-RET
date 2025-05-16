import asyncio
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso
import threading
import os
from .salvamento import salvar_no_banco_em_lote

sem_limite = asyncio.Semaphore(3)

def processar_sped_thread(nome_banco, progress_bar, label_arquivo, caminhos):
    print(f"[DEBUG] Iniciando thread de processamento SPED com {len(caminhos)} arquivo(s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(processar_sped(nome_banco, progress_bar, label_arquivo, caminhos))
    print(f"[DEBUG] Thread de processamento SPED finalizada")
    loop.close()

def iniciar_processamento_sped(nome_banco, progress_bar, label_arquivo):
    print(f"[DEBUG] Solicitando seleção de arquivos SPED...")
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.")
        print(f"[DEBUG] Nenhum arquivo selecionado.")
        return
    print(f"[DEBUG] {len(caminhos)} arquivo(s) selecionado(s):")
    for i, caminho in enumerate(caminhos):
        print(f"[DEBUG]   {i+1}. {os.path.basename(caminho)} ({os.path.getsize(caminho)/1024:.1f} KB)")
    thread = threading.Thread(target=processar_sped_thread, args=(nome_banco, progress_bar, label_arquivo, caminhos))
    thread.start()
    print(f"[DEBUG] Thread de processamento iniciada")

async def processar_sped(nome_banco, progress_bar, label_arquivo, caminhos):
    import time
    inicio_total = time.time()
    print(f"[DEBUG] Iniciando processamento de {len(caminhos)} arquivo(s) SPED. Hora de início: {time.strftime('%H:%M:%S')}")
    progress_bar.setValue(0)
    
    print(f"[DEBUG] Conectando ao banco de dados '{nome_banco}'...")
    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        print(f"[DEBUG] ERRO: Falha na conexão com o banco '{nome_banco}'")
        return

    print(f"[DEBUG] Verificando tabela de tributação...")
    cursor = conexao.cursor()
    cursor.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor.fetchone():
        mensagem_error("Tributação não encontrada. Envie primeiramente a tributação.")
        print(f"[DEBUG] ERRO: Tabela 'cadastro_tributacao' não encontrada no banco")
        return
    cursor.close()

    total = len(caminhos)
    progresso_por_arquivo = int(100 / total) if total > 0 else 100
    print(f"[DEBUG] Progresso por arquivo: {progresso_por_arquivo}%")

    try:
        print(f"[DEBUG] Criando {total} tarefa(s) de processamento...")
        tasks = []
        for i, caminho in enumerate(caminhos):
            print(f"[DEBUG] Adicionando tarefa para arquivo {i+1}/{total}: {os.path.basename(caminho)}")
            tasks.append(processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, i + 1, total, progresso_por_arquivo))
        
        print(f"[DEBUG] Iniciando processamento paralelo de {len(tasks)} tarefa(s)")
        await asyncio.gather(*tasks)

        mensagem_sucesso("Todos os arquivos SPED foram processados com sucesso.")
        fim_total = time.time()
        tempo_total = fim_total - inicio_total
        print(f"[DEBUG] Tempo total de processamento SPED: {tempo_total:.2f} segundos ({tempo_total/60:.2f} minutos)")
        print(f"[DEBUG] Média por arquivo: {tempo_total/total:.2f} segundos")
        print(f"[DEBUG] Processamento finalizado às {time.strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"[DEBUG] ERRO CRÍTICO durante o processamento: {type(e).__name__}: {e}")
        import traceback
        print(f"[DEBUG] Traceback: {traceback.format_exc()}")
        mensagem_error(f"Erro inesperado: {e}")
    finally:
        print(f"[DEBUG] Fechando conexão com o banco de dados")
        fechar_banco(conexao)


async def processar_arquivo(caminho, nome_banco, progress_bar, label_arquivo, indice, total, progresso_por_arquivo):
    async with sem_limite:
        import time
        inicio = time.time()
        nome_arquivo = os.path.basename(caminho)
        print(f"[DEBUG] === INICIANDO ARQUIVO {indice}/{total}: {nome_arquivo} ===")
        label_arquivo.setText(f'Processando arquivo {indice}/{total}')
        try:
            print(f"[DEBUG] Lendo conteúdo do arquivo {indice}/{total}...")
            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read()
                tamanho = len(conteudo)
                linhas = conteudo.count('\n') + 1
                print(f"[DEBUG] Arquivo {indice}/{total} lido: {tamanho/1024:.1f} KB, {linhas} linhas")
                
                print(f"[DEBUG] Processando dados do arquivo {indice}/{total}...")
                start_proc = time.time()
                conteudo_processado = process_data(conteudo)
                print(f"[DEBUG] Dados processados em {time.time() - start_proc:.2f} segundos")

            print(f"[DEBUG] Conectando ao banco para arquivo {indice}/{total}...")
            conexao = conectar_banco(nome_banco)
            cursor = conexao.cursor()
            
            print(f"[DEBUG] Salvando no banco os dados do arquivo {indice}/{total}...")
            start_save = time.time()
            await salvar_no_banco_em_lote(conteudo_processado, cursor, nome_banco)
            print(f"[DEBUG] Dados salvos em {time.time() - start_save:.2f} segundos")
            
            print(f"[DEBUG] Commit das alterações do arquivo {indice}/{total}...")
            conexao.commit()
            cursor.close()
            fechar_banco(conexao)

            fim = time.time()
            tempo_proc = fim - inicio
            print(f"[DEBUG] Arquivo {indice}/{total} processado em: {tempo_proc:.2f} segundos")

            progresso_atual = min(indice * progresso_por_arquivo, 100)
            print(f"[DEBUG] Atualizando barra de progresso: {progresso_atual}%")
            progress_bar.setValue(progresso_atual)
            print(f"[DEBUG] === FINALIZADO ARQUIVO {indice}/{total}: {nome_arquivo} ===")

        except Exception as e:
            print(f"[DEBUG] ERRO ao processar o arquivo {indice}/{total}: {type(e).__name__}: {e}")
            import traceback
            print(f"[DEBUG] Traceback: {traceback.format_exc()}")
            mensagem_error(f"Erro ao processar o arquivo {caminho}: {e}")