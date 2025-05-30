from PySide6.QtWidgets import QApplication
from db.conexao import conectar_banco, fechar_banco
from ui.popupAliquota import PopupAliquota

def verificar_e_abrir_popup_aliquota(empresa_id, parent=None):
    conexao = conectar_banco()
    cursor = conexao.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM cadastro_tributacao
        WHERE empresa_id = %s AND (aliquota IS NULL OR aliquota = '')
    """, (empresa_id,))
    (quantidade_nulas,) = cursor.fetchone()

    cursor.close()
    fechar_banco(conexao)

    if quantidade_nulas > 0:
        print(f"[INFO] Existem {quantidade_nulas} alíquotas nulas. Abrindo popup para preenchimento...")
        popup = PopupAliquota(empresa_id, parent=parent)
        resultado = popup.exec()

        if resultado == PopupAliquota.Accepted:
            print("[INFO] Alíquotas preenchidas com sucesso.")
            return True
        else:
            print("[INFO] Preenchimento de alíquotas foi cancelado pelo usuário.")
            return False

    print("[INFO] Nenhuma alíquota nula encontrada.")
    return False
