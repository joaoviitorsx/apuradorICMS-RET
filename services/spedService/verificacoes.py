from db.conexao import conectar_banco, fechar_banco
from ui.popupAliquota import PopupAliquota
from PySide6.QtWidgets import QApplication

def verificar_e_preencher_aliquotas(empresa_id, janela_pai=None):
    conexao = conectar_banco()
    cursor = conexao.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM cadastro_tributacao 
        WHERE (aliquota IS NULL OR aliquota = '') AND empresa_id = %s
    """, (empresa_id,))
    total_null = cursor.fetchone()[0]
    cursor.close()
    fechar_banco(conexao)

    if total_null > 0:
        print(f"[VERIFICAÇÃO] {total_null} produtos com alíquota nula detectados.")
        popup = PopupAliquota(empresa_id, janela_pai)
        popup.exec()
    else:
        print("[VERIFICAÇÃO] Nenhuma alíquota pendente.")
