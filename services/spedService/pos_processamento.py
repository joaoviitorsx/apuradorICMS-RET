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
    progress_bar.setValue(80)
    await atualizar_ncm(nome_banco)
    progress_bar.setValue(85)
    await atualizar_aliquota(nome_banco)
    progress_bar.setValue(90)
    await atualizar_aliquota_simples(nome_banco)
    progress_bar.setValue(95)
    await atualizar_resultado(nome_banco)
    progress_bar.setValue(100)