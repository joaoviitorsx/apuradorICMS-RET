from db.conexao import conectar_banco, fechar_banco
from services.fornecedorService import comparar_adicionar_atualizar_fornecedores
from services.spedService.tributacao import criar_e_preencher_c170nova, atualizar_cadastro_tributacao
from services.spedService.atualizacoes import atualizar_ncm, atualizar_aliquota, atualizar_aliquota_simples,atualizar_resultado
from services.spedService.clonagem import clonar_tabela_c170
from services.spedService.verificacoes import verificar_e_abrir_popup_aliquota

async def etapas_pos_processamento(empresa_id, progress_bar, janela_pai=None):
    print(f"[POS] Iniciando etapas de pós-processamento para empresa_id={empresa_id}...")

    progress_bar.setValue(40)
    await comparar_adicionar_atualizar_fornecedores(empresa_id)
    print("[POS] Fornecedores atualizados.")

    progress_bar.setValue(50)
    await criar_e_preencher_c170nova(empresa_id)
    print("[POS] Tabela c170nova criada e preenchida.")

    progress_bar.setValue(52)
    await atualizar_cadastro_tributacao(empresa_id)
    print("[POS] Tabela cadastro_tributacao atualizada com base em c170nova.")

    progress_bar.setValue(54)
    await verificar_e_abrir_popup_aliquota(empresa_id, janela_pai)
    print("[POS] Popup de alíquotas exibido, se necessário.")

    progress_bar.setValue(60)
    await clonar_tabela_c170(empresa_id)
    print("[POS] Tabela c170_clone criada com sucesso.")

    progress_bar.setValue(70)
    await atualizar_ncm(empresa_id)
    print("[POS] NCMs atualizados com base na tabela 0200.")

    await atualizar_aliquota(empresa_id)
    print("[POS] Alíquotas atualizadas na tabela c170_clone.")
    
    conexao = conectar_banco()
    cursor = conexao.cursor()
    cursor.execute("SELECT dt_ini FROM `0000` WHERE empresa_id = %s ORDER BY id DESC LIMIT 1", (empresa_id,))
    row = cursor.fetchone()
    cursor.close()
    fechar_banco(conexao)

    periodo = f"{row[0][2:4]}/{row[0][4:]}" if row and len(row[0]) >= 6 else "00/0000"
    print(f"[POS] Período detectado no pós-processamento: {periodo}")

    progress_bar.setValue(85)
    await atualizar_aliquota_simples(empresa_id, periodo)
    print("[POS] Alíquotas do Simples Nacional ajustadas.")

    progress_bar.setValue(90)
    await atualizar_resultado(empresa_id)
    print("[POS] Campo resultado calculado com base em vl_item e aliquota.")

    progress_bar.setValue(100)
    print("[POS] Pós-processamento concluído.")