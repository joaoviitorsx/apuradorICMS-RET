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
    print("[POS] Iniciando etapas de pós-processamento...")

    progress_bar.setValue(40)
    await comparar_adicionar_atualizar_fornecedores(nome_banco)
    print("[POS] Fornecedores atualizados.")

    progress_bar.setValue(50)
    await criar_e_preencher_c170nova(nome_banco)
    print("[POS] Tabela c170nova criada e preenchida.")

    progress_bar.setValue(55)
    await clonar_tabela_c170(nome_banco)
    print("[POS] Tabela c170_clone criada com sucesso.")

    progress_bar.setValue(60)
    await atualizar_cadastro_tributacao(nome_banco)
    print("[POS] Tabela cadastro_tributacao atualizada com base em c170_clone.")

    progress_bar.setValue(65)
    await atualizar_aliquota(nome_banco)
    print("[POS] Alíquotas atualizadas com base em cadastro_tributacao.")

    progress_bar.setValue(70)
    await atualizar_ncm(nome_banco)
    print("[POS] NCMs atualizados com base na tabela 0200.")

    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()
    cursor.execute("SELECT dt_ini FROM `0000` ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    cursor.close()
    fechar_banco(conexao)

    periodo = f"{row[0][2:4]}/{row[0][4:]}" if row and len(row[0]) >= 6 else "00/0000"
    print(f"[POS] Período detectado no pós-processamento: {periodo}")
    
    progress_bar.setValue(85)
    await atualizar_aliquota_simples(nome_banco, periodo)
    print("[POS] Alíquotas do Simples Nacional ajustadas.")

    progress_bar.setValue(95)
    await atualizar_resultado(nome_banco)
    print("[POS] Campo resultado calculado com base em vl_item e aliquota.")

    progress_bar.setValue(100)
    print("[POS] Pós-processamento concluído.")
