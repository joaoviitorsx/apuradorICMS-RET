from db.conexao import conectar_banco, fechar_banco
from services.fornecedorService import comparar_adicionar_atualizar_fornecedores
from services.spedService.tributacao import criar_e_preencher_c170nova, atualizar_cadastro_tributacao
from services.spedService.relatorio_popup import tela_popup
from services.spedService.atualizacoes import (
    atualizar_ncm,
    atualizar_aliquota,
    atualizar_aliquota_simples,
    atualizar_resultado
)
from services.spedService.clonagem import clonar_tabela_c170

async def etapas_pos_processamento(nome_banco, progress_bar):
    progress_bar.setValue(40)
    await comparar_adicionar_atualizar_fornecedores(nome_banco)
    progress_bar.setValue(50)
    await criar_e_preencher_c170nova(nome_banco)
    progress_bar.setValue(55)
    await atualizar_cadastro_tributacao(nome_banco)
    progress_bar.setValue(60)
    await clonar_tabela_c170(nome_banco)
    progress_bar.setValue(70)
    await atualizar_ncm(nome_banco)

    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()
    cursor.execute("SELECT dt_ini FROM `0000` ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    cursor.close()
    fechar_banco(conexao)

    periodo = f"{row[0][2:4]}/{row[0][4:]}" if row and len(row[0]) >= 6 else "00/0000"
    print(f"[DEBUG] Período detectado no pós-processamento: {periodo}")
    
    progress_bar.setValue(85)
    await atualizar_aliquota(nome_banco, periodo)
    progress_bar.setValue(90)
    await atualizar_aliquota_simples(nome_banco, periodo)
    progress_bar.setValue(95)
    await atualizar_resultado(nome_banco)
    progress_bar.setValue(100)
