from PySide6.QtCore import QTimer
from db.conexao import conectar_banco, fechar_banco
from services.fornecedorService import comparar_adicionar_atualizar_fornecedores
from services.spedService.tributacao import criar_e_preencher_c170nova
from services.spedService.atualizacoes import atualizar_aliquota, atualizar_aliquota_simples, atualizar_resultado, atualizar_aliquotaRET, atualizar_resultadoRET
from services.spedService.clonagem import clonar_tabela_c170nova
from services.spedService.verificacoes import verificar_e_abrir_popup_aliquota, preencherTributacao

async def etapas_pos_processamento(empresa_id, progress_bar, janela_pai=None):
    print(f"[POS] Iniciando etapas de pós-processamento para empresa_id={empresa_id}...")

    progress_bar.setValue(40)
    await comparar_adicionar_atualizar_fornecedores(empresa_id)
    print("[POS] Fornecedores atualizados.")

    progress_bar.setValue(50)
    await criar_e_preencher_c170nova(empresa_id)
    print("[POS] Tabela c170nova criada e preenchida.")

    progress_bar.setValue(52)
    await preencherTributacao(empresa_id, janela_pai)
    print("[POS] Cadastro de tributação preenchido com base na tabela 0200.")

    progress_bar.setValue(54)
    await verificar_e_abrir_popup_aliquota(empresa_id, janela_pai)
    print("[POS] Popup de alíquotas exibido, se necessário.")

    progress_bar.setValue(60)
    await clonar_tabela_c170nova(empresa_id)
    print("[POS] Tabela c170_clone criada com sucesso.")

    await atualizar_aliquota(empresa_id)
    print("[POS] Alíquotas atualizadas na tabela c170_clone.")

    await atualizar_aliquotaRET(empresa_id)
    print("[POS] Alíquotas RET atualizadas na tabela c170_clone.")

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

    progress_bar.setValue(95)
    await atualizar_resultadoRET(empresa_id)
    print("[POS] Campo resultadoRET calculado com base em vl_item e aliquotaRET.")
    
    progress_bar.setValue(100)
    print("[POS] Pós-processamento concluído.")

