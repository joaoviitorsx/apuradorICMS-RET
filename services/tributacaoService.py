import pandas as pd
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.aliquota import formatar_aliquota
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms']
}

def mapear_colunas(df):
    colunas_encontradas = {}
    colunas_atuais = [col.lower().strip() for col in df.columns]

    for coluna_padrao, sinonimos in COLUNAS_SINONIMAS.items():
        for nome in sinonimos:
            if nome.lower() in colunas_atuais:
                idx = colunas_atuais.index(nome.lower())
                colunas_encontradas[coluna_padrao] = df.columns[idx]
                break

    return colunas_encontradas if len(colunas_encontradas) == len(COLUNAS_SINONIMAS) else None

def enviar_tributacao(nome_banco, progress_bar):
    progress_bar.setValue(0)
    filename, _ = QFileDialog.getOpenFileName(None, "Enviar Tributação", "", "Arquivos Excel (*.xlsx)")

    if not filename:
        mensagem_aviso("Nenhum arquivo selecionado.")
        return

    progress_bar.setValue(10)
    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        return

    progress_bar.setValue(20)
    cursor = conexao.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cadastro_tributacao (
        id INT AUTO_INCREMENT PRIMARY KEY,
        codigo VARCHAR(20) UNIQUE,
        produto VARCHAR(100),
        ncm VARCHAR(20),
        aliquota VARCHAR(20),
        aliquota_antiga VARCHAR(20),
        data_inicial DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    progress_bar.setValue(30)

    try:
        df = pd.read_excel(filename, dtype=str)
        mapeamento = mapear_colunas(df)

        if not mapeamento:
            mensagem_error("Não foi possível identificar as colunas necessárias na planilha.")
            return

        progress_bar.setValue(40)
        df = df.rename(columns=mapeamento)
        df['ALIQUOTA'] = df['ALIQUOTA'].apply(lambda x: formatar_aliquota(str(x).strip()))
        dados_para_inserir = df[['CODIGO', 'PRODUTO', 'NCM', 'ALIQUOTA']].values.tolist()

        cursor.executemany("""
            INSERT IGNORE INTO cadastro_tributacao (codigo, produto, ncm, aliquota)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            produto = VALUES(produto),
            ncm = VALUES(ncm),
            aliquota = VALUES(aliquota)
        """, dados_para_inserir)
        progress_bar.setValue(80)

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
        """)
        progress_bar.setValue(90)

        conexao.commit()
        progress_bar.setValue(100)
        mensagem_sucesso("Tributação enviada com sucesso!")

    except Exception as e:
        mensagem_error(f"Erro ao processar o arquivo: {str(e)}")
    finally:
        cursor.close()
        fechar_banco(conexao)
