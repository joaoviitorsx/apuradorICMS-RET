from fpdf import FPDF
from datetime import datetime
from db.conexao import conectar_banco, fechar_banco
from PySide6.QtWidgets import QMessageBox

class PDFPrompt(QMessageBox):
    def __init__(self, empresa_id, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Salvar histórico em PDF?")
        self.setText("Deseja salvar um PDF com os dados analisados antes da limpeza?")
        self.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        self.setIcon(QMessageBox.Question)
        self.empresa_id = empresa_id

    def executar(self):
        resposta = QMessageBox.question(
            self.parent,
            "Salvar histórico em PDF?",
            "Deseja salvar um PDF com os dados analisados antes da limpeza?",
            QMessageBox.Yes | QMessageBox.No
        )
        if resposta == QMessageBox.Yes:
            self.nome_pdf = gerar_pdf_historico(self.empresa_id)
            QMessageBox.information(
                self.parent,
                "PDF gerado",
                f"O histórico foi salvo como:\n{self.nome_pdf}"
            )
            from services.spedService.limpeza import limpar_tabelas_temporarias
            limpar_tabelas_temporarias(self.empresa_id)
            print("[PDF] PDF gerado e tabelas temporárias limpas.")

class PDFHistorico(FPDF):
    def header(self):
        self.set_font("Arial", "B", 12)
        self.cell(0, 10, "Histórico de Processamento SPED", ln=True, align="C")
        self.ln(5)

    def add_section_title(self, title):
        self.set_font("Arial", "B", 11)
        self.cell(0, 10, title, ln=True)
        self.ln(2)

    def add_table_summary(self, headers, data):
        self.set_font("Arial", "B", 10)
        for header in headers:
            self.cell(50, 8, header[:15], border=1)
        self.ln()
        self.set_font("Arial", "", 9)
        for row in data:
            for item in row:
                self.cell(50, 8, str(item)[:15], border=1)
            self.ln()
        self.ln(5)

def gerar_pdf_historico(empresa_id):
    pdf = PDFHistorico()
    pdf.add_page()

    data_atual = datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 10, f"Empresa ID: {empresa_id}", ln=True)
    pdf.cell(0, 10, f"Data do relatório: {data_atual}", ln=True)
    pdf.ln(10)

    tabelas = {
        "0000": "Bloco 0000 - Abertura",
        "0150": "Bloco 0150 - Fornecedores",
        "0200": "Bloco 0200 - Produtos",
        "c100": "Bloco C100 - Documentos",
        "c170": "Bloco C170 - Itens das Notas",
        "c170nova": "Tabela C170Nova - Processada"
    }

    conexao = conectar_banco()
    cursor = conexao.cursor()

    for tabela, titulo in tabelas.items():
        cursor.execute(f"SELECT COUNT(*) FROM {tabela} WHERE empresa_id = %s", (empresa_id,))
        total = cursor.fetchone()[0]

        pdf.add_section_title(titulo)
        pdf.set_font("Arial", "", 10)
        pdf.cell(0, 8, f"Total de registros: {total}", ln=True)

        if total > 0 and tabela in ("0200", "c100", "c170"):
            cursor.execute(f"SELECT * FROM {tabela} WHERE empresa_id = %s LIMIT 3", (empresa_id,))
            amostras = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            pdf.add_table_summary(headers[:3], [row[:3] for row in amostras])

    cursor.close()
    fechar_banco(conexao)

    nome_arquivo = f"historico_empresa_{empresa_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    pdf.output(nome_arquivo)
    return nome_arquivo
