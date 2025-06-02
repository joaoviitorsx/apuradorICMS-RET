from PySide6.QtWidgets import QApplication, QMessageBox, QDialog
from PySide6.QtCore import QObject, Signal, QEventLoop
from db.conexao import conectar_banco, fechar_banco
from ui.popupAliquota import PopupAliquota

class SinalPopup(QObject):
    abrir_popup_signal = Signal(int, object)

    def __init__(self):
        super().__init__()
        self.resultado_popup = None
        self.event_loop = None

sinal_popup = SinalPopup()

async def verificar_e_abrir_popup_aliquota(empresa_id, janela_pai=None):
    print(f"[INFO] Verificando alíquotas nulas para empresa_id={empresa_id}...")

    conexao = conectar_banco()
    cursor = conexao.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM cadastro_tributacao
        WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
    """, (empresa_id,))
    count = cursor.fetchone()[0]
    cursor.close()
    fechar_banco(conexao)

    print("[INFO] Conexão com banco encerrada.")

    if count > 0:
        print(f"[INFO] Existem {count} alíquotas nulas. Solicitando preenchimento via popup...")

        sinal_popup.event_loop = QEventLoop()
        sinal_popup.resultado_popup = None
        sinal_popup.abrir_popup_signal.emit(empresa_id, janela_pai)
        sinal_popup.event_loop.exec()

    else:
        print("[INFO] Nenhuma alíquota nula encontrada.")

async def preencherTributacao(empresa_id, parent=None):
    print(f"[VERIFICAÇÃO] Iniciando verificação e preenchimento de cadastro_tributacao com produtos da 0200 para empresa_id={empresa_id}...")
    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO cadastro_tributacao (empresa_id, codigo, produto, ncm)
            SELECT DISTINCT
                o.empresa_id,
                o.cod_item,
                o.descr_item,
                o.cod_ncm
            FROM `0200` o
            JOIN c170 c ON c.cod_item = o.cod_item AND c.empresa_id = o.empresa_id
            JOIN c100 cc ON cc.id = c.id_c100
            JOIN (
                SELECT cod_part FROM cadastro_fornecedores
                WHERE decreto = 'Não' AND uf = 'CE' AND empresa_id = %s
            ) f ON cc.cod_part = f.cod_part
            WHERE o.empresa_id = %s
              AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
              AND NOT EXISTS (
                  SELECT 1 FROM cadastro_tributacao t
                  WHERE t.codigo = o.cod_item AND t.empresa_id = o.empresa_id
              )
        """, (empresa_id, empresa_id))

        novos = cursor.rowcount
        conexao.commit()
        print(f"[OK] {novos} novos produtos únicos inseridos na tabela cadastro_tributacao.")

    except Exception as e:
        print(f"[ERRO] Falha durante verificação e preenchimento: {e}")
        conexao.rollback()

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Verificação e preenchimento de cadastro_tributacao concluído.")






