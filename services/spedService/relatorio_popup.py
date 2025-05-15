from fpdf import FPDF
from datetime import datetime
from pathlib import Path
from db.conexao import conectar_banco, fechar_banco
from ui.popupAliquota import PopupAliquota

async def tela_popup(nome_banco, progress_bar):
    conexao = conectar_banco(nome_banco)
    cursor = conexao.cursor()
    cursor.execute("START TRANSACTION")

    cursor.execute("SELECT codigo, produto, ncm FROM cadastro_tributacao WHERE aliquota IS NULL")
    itens_nulos = cursor.fetchall()
    total = len(itens_nulos)

    if total == 0:
        cursor.close()
        fechar_banco(conexao)
        return

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 12)
    pdf.cell(200, 10, txt="Relatório de Itens com Alíquota Nula", ln=True, align="C")

    for codigo, produto, ncm in itens_nulos:
        pdf.set_font("Arial", "B", 12)
        pdf.multi_cell(200, 10, txt=f"\nCódigo: {codigo} | Produto: {produto} | NCM: {ncm}")

        cursor.execute("""
            SELECT c170.filial, c170.cfop, c170.num_doc, c100.dt_doc, c100.dt_e_s
            FROM c170
            JOIN c100 ON c170.num_doc = c100.num_doc
            WHERE c170.cod_item = %s
        """, (codigo,))
        transacoes = cursor.fetchall()

        for filial, cfop, num_doc, dt_doc, dt_e_s in transacoes:
            if isinstance(dt_doc, datetime):
                dt_doc = dt_doc.strftime("%d/%m/%Y")
            elif isinstance(dt_doc, str) and len(dt_doc) == 8:
                dt_doc = f"{dt_doc[:2]}/{dt_doc[2:4]}/{dt_doc[4:]}"
            else:
                dt_doc = "Data inválida"

            if isinstance(dt_e_s, str) and len(dt_e_s) == 8:
                dt_e_s = f"{dt_e_s[:2]}/{dt_e_s[2:4]}/{dt_e_s[4:]}"

            pdf.set_font("Arial", "", 12)
            pdf.multi_cell(200, 10, txt=f"FILIAL:{filial} | CFOP:{cfop} | NUM_DOC:{num_doc} | DT_DOC:{dt_doc} | DT_E_S:{dt_e_s}\n")

    cursor.execute("SELECT cnpj FROM `0000` ORDER BY id DESC LIMIT 1")
    cnpj = cursor.fetchone()[0]
    cursor.execute("SELECT periodo FROM `0000` ORDER BY id DESC LIMIT 1")
    periodo = cursor.fetchone()[0]
    periodo_mes = f"{periodo[3:]}-{periodo[:2]}"

    conexao_emp = conectar_banco("empresas_db")
    cursor_emp = conexao_emp.cursor()
    cursor_emp.execute("SELECT razao_social FROM empresas WHERE LEFT(cnpj, 8) = LEFT(%s, 8) LIMIT 1", (cnpj,))
    razao = cursor_emp.fetchone()[0]

    pasta = Path.home() / "Downloads" / "Super" / "Relatórios" / razao
    pasta.mkdir(parents=True, exist_ok=True)
    caminho_pdf = pasta / f"{razao}-{periodo_mes} Produtos sem Tributação.pdf"
    pdf.output(caminho_pdf)

    cursor_emp.close()
    conexao_emp.close()

    for i, (codigo, produto, ncm) in enumerate(itens_nulos, start=1):
        def salvar_callback(cod, aliq):
            cursor.execute("UPDATE cadastro_tributacao SET aliquota = %s WHERE codigo = %s", (aliq, cod))
            conexao.commit()

        popup = PopupAliquota(codigo, produto, ncm, salvar_callback, i, total)
        popup.exec()

    cursor.execute("""
        UPDATE cadastro_tributacao 
        SET aliquota_antiga = CASE
            WHEN aliquota = '1.54%' THEN '1.40%'
            WHEN aliquota = '2.63%' THEN '2.40%'
            WHEN aliquota = '4%' THEN '3.60%'
            WHEN aliquota = '4.00%' THEN '3.60%'
            WHEN aliquota = '8.13%' THEN '8.13%'
            WHEN aliquota = 'ST' THEN 'ST'
            WHEN aliquota = 'ISENTO' THEN 'ISENTO'
            WHEN aliquota = 'PAUTA' THEN 'PAUTA'
            ELSE 'N/A'
        END
        WHERE aliquota_antiga IS NULL OR aliquota_antiga = ''
    """)
    conexao.commit()
    cursor.close()
    fechar_banco(conexao)