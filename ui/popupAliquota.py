import pandas as pd
from unidecode import unidecode
from PySide6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPushButton, QTableWidget, QTableWidgetItem, QHeaderView, QFileDialog, QHBoxLayout, QMessageBox, QApplication
from PySide6.QtCore import Qt
from PySide6 import QtGui
from utils.aliquota_uf import identificar_categoria, obter_aliquota, preencherAliquotaRET
from utils.aliquota import formatar_aliquota, eh_aliquota_numerica
from db.conexao import conectar_banco, fechar_banco
from utils.mensagem import mensagem_sucesso, mensagem_error

class PopupAliquota(QDialog):
    def __init__(self, empresa_id, parent=None):
        super().__init__(parent)
        self.empresa_id = empresa_id
        self.setWindowTitle("Preencher Alíquotas Nulas")
        self.setMinimumSize(800, 600)
        self.setup_ui()

        screen = QtGui.QGuiApplication.screenAt(QtGui.QCursor.pos())
        screen_geometry = screen.availableGeometry() if screen else QApplication.primaryScreen().availableGeometry()

        center_point = screen_geometry.center()
        self.move(center_point - self.rect().center())

    def setup_ui(self):
        self.setStyleSheet("background-color: #030d18; color: white;")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        self.label = QLabel("Preencha as alíquotas nulas antes de prosseguir:")
        self.label.setStyleSheet("font-size: 16px; font-weight: bold;")
        self.label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.label)

        self.tabela = QTableWidget()
        self.tabela.setStyleSheet("background-color: #e1e1e1; color: black; border-radius: 10px;")
        self.tabela.setAlternatingRowColors(True)
        layout.addWidget(self.tabela)

        grupo_botoes = QHBoxLayout()

        self.botao_criar_planilha = QPushButton("Criar Planilha Modelo")
        self.botao_criar_planilha.clicked.connect(self.exportar_planilha_modelo)
        self._estilizar_botao(self.botao_criar_planilha, cor="#007bff", hover="#0054b3")
        grupo_botoes.addWidget(self.botao_criar_planilha)

        self.botao_importar_planilha = QPushButton("Importar Planilha")
        self.botao_importar_planilha.clicked.connect(self.importar_planilha)
        self._estilizar_botao(self.botao_importar_planilha, cor="#007bff", hover="#0054b3")
        grupo_botoes.addWidget(self.botao_importar_planilha)

        layout.addLayout(grupo_botoes)

        self.botao_salvar = QPushButton("Salvar Tudo")
        self.botao_salvar.clicked.connect(self.salvar_dados)
        self._estilizar_botao(self.botao_salvar, cor="#28a745", hover="#166628")
        layout.addWidget(self.botao_salvar, alignment=Qt.AlignCenter)

        self.carregar_dados()

    def carregar_dados(self):
        conexao = conectar_banco()
        cursor = conexao.cursor()

        cursor.execute("""
            SELECT 
            MIN(id) as id,
            MIN(codigo) as codigo,
            produto,
            ncm,
            NULL as aliquota
        FROM cadastro_tributacao
        WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
        GROUP BY produto, ncm
        """, (self.empresa_id,))

        dados = cursor.fetchall()
        cursor.close()
        fechar_banco(conexao)

        self.tabela.setRowCount(len(dados))
        self.tabela.setColumnCount(5)
        self.tabela.setHorizontalHeaderLabels(["ID", "Código", "Produto", "NCM", "Alíquota"])
        self.tabela.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        for row_idx, (id_, codigo, produto, ncm, aliquota) in enumerate(dados):
            self.tabela.setItem(row_idx, 0, QTableWidgetItem(str(id_)))
            self.tabela.setItem(row_idx, 1, QTableWidgetItem(str(codigo)))
            self.tabela.setItem(row_idx, 2, QTableWidgetItem(produto))
            self.tabela.setItem(row_idx, 3, QTableWidgetItem(ncm))
            item_aliquota = QTableWidgetItem(aliquota if aliquota else "")
            item_aliquota.setFlags(item_aliquota.flags() | Qt.ItemIsEditable)
            self.tabela.setItem(row_idx, 4, item_aliquota)

    def salvar_dados(self):
        print("[SALVAR] Iniciando atualização de alíquotas")
        conexao = conectar_banco()
        cursor = conexao.cursor()

        try:
            for row in range(self.tabela.rowCount()):
                produto = self.tabela.item(row, 2).text().strip() if self.tabela.item(row, 2) else ''
                ncm = self.tabela.item(row, 3).text().strip() if self.tabela.item(row, 3) else ''
                aliquota_bruta = self.tabela.item(row, 4).text().strip() if self.tabela.item(row, 4) else ''

                if not produto or not ncm or not aliquota_bruta:
                    print(f"[AVISO] Linha {row} possui dados incompletos. Pulando...")
                    continue

                aliquota_formatada = formatar_aliquota(aliquota_bruta)

                print(f"[DEBUG] Produto: {produto}, NCM: {ncm}, Alíquota informada: {aliquota_formatada}")

                cursor.execute("""
                    UPDATE cadastro_tributacao
                    SET aliquota = %s, categoria_fiscal = '', aliquotaRET = ''
                    WHERE produto = %s AND ncm = %s AND empresa_id = %s
                """, (
                    aliquota_formatada,
                    produto,
                    ncm,
                    self.empresa_id
                ))

            conexao.commit()
            print("[SALVAR] Commit realizado com sucesso.")

            preencherAliquotaRET(self.empresa_id)

            self.label.setText("Alíquotas atualizadas com sucesso.")
            mensagem_sucesso("Dados salvos com sucesso!")
            self.accept()

        except Exception as e:
            conexao.rollback()
            print(f"[ERRO] {e}")
            mensagem_error(f"Erro ao salvar: {e}")
            self.label.setText(f"Erro ao salvar: {e}")

        finally:
            cursor.close()
            fechar_banco(conexao)

    def exportar_planilha_modelo(self):
        caminho, _ = QFileDialog.getSaveFileName(self, "Salvar Planilha Modelo", "Tributacao.xlsx", "Arquivos Excel (*.xlsx)")
        if not caminho:
            return

        dados = []
        for row in range(self.tabela.rowCount()):
            ncm_valor = self.tabela.item(row, 3).text().strip()
            try:
                if ncm_valor and ncm_valor.isdigit():
                    ncm_valor = ncm_valor.zfill(8)
            except:
                pass
                
            dados.append({
                "Código": self.tabela.item(row, 1).text(),
                "Produto": self.tabela.item(row, 2).text(),
                "NCM": ncm_valor,
                "Alíquota": self.tabela.item(row, 4).text()
            })

        df = pd.DataFrame(dados)
        
        with pd.ExcelWriter(caminho, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
            worksheet = writer.sheets['Sheet1']
            for idx, _ in enumerate(df['NCM'], start=2):
                cell = worksheet.cell(row=idx, column=4)
                cell.number_format = '@'

        resposta = QMessageBox.question(
            self,
            "Abrir Planilha",
            "Planilha modelo criada com sucesso.\nDeseja abri-la agora?",
            QMessageBox.Yes | QMessageBox.No
        )

        if resposta == QMessageBox.Yes:
            import os
            os.startfile(caminho)

    def importar_planilha(self):
        caminho, _ = QFileDialog.getOpenFileName(self, "Selecionar Planilha", "", "Arquivos Excel (*.xlsx *.xls)")
        if not caminho:
            return

        try:
            df = pd.read_excel(caminho, dtype=str)

            print(f"Colunas encontradas na planilha: {list(df.columns)}")

            def normalizar(texto):
                from unidecode import unidecode
                return unidecode(str(texto)).strip().lower().replace(" ", "").replace("%", "")

            colunas_norm = {normalizar(col): col for col in df.columns}
            print(f"Colunas normalizadas: {colunas_norm}")

            col_codigo = next((colunas_norm[c] for c in colunas_norm if "codigo" in c or "cod" in c), None)
            col_aliquota = next((colunas_norm[c] for c in colunas_norm if "aliquota" in c), None)

            print(f"Coluna de código identificada: {col_codigo}")
            print(f"Coluna de alíquota identificada: {col_aliquota}")

            if not col_codigo or not col_aliquota:
                QMessageBox.warning(self, "Importação falhou",
                    f"Colunas 'Código' e/ou 'Alíquota' não encontradas na planilha.\n"
                    f"Colunas disponíveis: {', '.join(df.columns)}")
                return

            codigos_planilha = df[[col_codigo, col_aliquota]].dropna()

            codigos_dict = {}
            erros_formato = []

            valores_livres = {"isento", "insento", "st", "substituicao", "substituicao tributaria"}

            for _, row in codigos_planilha.iterrows():
                codigo_bruto = str(row[col_codigo]).strip()
                aliquota_bruta = str(row[col_aliquota]).strip()

                try:
                    num = float(codigo_bruto)
                    codigo = str(int(num)) if num.is_integer() else codigo_bruto
                except ValueError:
                    codigo = codigo_bruto

                aliquota_normalizada = aliquota_bruta.lower().strip().replace(" ", "")

                if aliquota_normalizada in valores_livres:
                    codigos_dict[codigo] = aliquota_bruta.upper()
                    continue

                try:
                    valor_check = aliquota_normalizada.replace("%", "").replace(",", ".")
                    valor_num = float(valor_check)
                
                    if valor_num < 1:
                        valor_formatado = f"{valor_num*100:.2f}%".replace(".", ",")
                    else:
                        valor_formatado = f"{valor_num:.2f}%".replace(".", ",")
                    
                    codigos_dict[codigo] = valor_formatado
                except ValueError:
                    erros_formato.append(f"'{codigo}': '{aliquota_bruta}'")

            print(f"Dicionário de códigos/alíquotas: {codigos_dict}")
            if erros_formato:
                QMessageBox.warning(self, "Alíquotas com formato inválido",
                    f"As seguintes alíquotas não puderam ser convertidas:\n"
                    f"{', '.join(erros_formato[:10])}" +
                    (f"\n(e mais {len(erros_formato)-10})" if len(erros_formato)>10 else ""))

            atualizados = 0
            nao_encontrados = []

            for row in range(self.tabela.rowCount()):
                item_codigo = self.tabela.item(row, 1)
                item_aliquota = self.tabela.item(row, 4)

                if item_codigo and item_aliquota:
                    codigo = str(item_codigo.text()).strip()
                    if codigo in codigos_dict:
                        item_aliquota.setText(codigos_dict[codigo])
                        atualizados += 1
                    else:
                        nao_encontrados.append(codigo)

            mensagem = f"{atualizados} alíquotas atualizadas com sucesso."
            if nao_encontrados:
                mensagem += f"\n\n{len(nao_encontrados)} códigos não encontrados na planilha."
                if len(nao_encontrados) <= 10:
                    mensagem += f"\nCódigos não encontrados: {', '.join(nao_encontrados)}"
                else:
                    mensagem += f"\nPrimeiros 10 códigos não encontrados: {', '.join(nao_encontrados[:10])}..."

            QMessageBox.information(self, "Importação concluída", mensagem)

        except Exception as e:
            import traceback
            print(f"Erro detalhado: {traceback.format_exc()}")
            QMessageBox.critical(self, "Erro ao importar", f"Ocorreu um erro ao importar a planilha:\n{str(e)}")

    def _estilizar_botao(self, botao, cor="#007bff", hover="#0054b3"):
        botao.setCursor(Qt.PointingHandCursor)
        botao.setStyleSheet(f"""
            QPushButton {{
                background-color: {cor};
                color: white;
                font-weight: bold;
                font-size: 14px;
                padding: 8px 16px;
                border-radius: 5px;
            }}
            QPushButton:hover {{
                background-color: {hover};
            }}
        """)
