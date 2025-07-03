from PySide6.QtCore import QObject, Signal, QEventLoop
from db.conexao import conectarBanco, fecharBanco

class SinalPopup(QObject):
    abrir_popup_signal = Signal(int, object)

    def __init__(self):
        super().__init__()
        self.resultado_popup = None
        self.event_loop = None
        self.popup_aberto = False

sinal_popup = SinalPopup()

async def verificaoPopupAliquota(empresa_id, janela_pai=None):
    print(f"[INFO] Verificando alíquotas nulas para empresa_id={empresa_id}...")

    if sinal_popup.popup_aberto:
        print("[INFO] Popup já aberto")
        return
    
    conexao = conectarBanco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT MIN(codigo) AS codigo, produto, ncm
                FROM cadastro_tributacao
                WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
                GROUP BY produto, ncm
            ) AS sub
        """, (empresa_id,))
        count = cursor.fetchone()[0]
    finally:
        cursor.close()
        fecharBanco(conexao)

    print("[INFO] Conexão com banco encerrada.")

    if count > 0:
        print(f"[INFO] Existem {count} alíquotas nulas. Solicitando preenchimento via popup...")

        sinal_popup.popup_aberto = True
        sinal_popup.event_loop = QEventLoop()
        sinal_popup.resultado_popup = None
        sinal_popup.abrir_popup_signal.emit(empresa_id, janela_pai)
        sinal_popup.event_loop.exec()
        sinal_popup.popup_aberto = False

    else:
        print("[INFO] Nenhuma alíquota nula encontrada.")

async def preencherTributacao(empresa_id, parent=None, lote_tamanho=3000):
    print(f"[VERIFICAÇÃO] Preenchendo cadastro_tributacao com produtos da empresa_id={empresa_id}")
    conexao = conectarBanco()
    cursor = conexao.cursor()

    total_inseridos = 0
    offset = 0

    try:
        print("[PARTE 1] Carregando dados dos fornecedores")
        cursor.execute("""
            SELECT cod_part, empresa_id, uf, decreto FROM cadastro_fornecedores
            WHERE empresa_id = %s
        """, (empresa_id,))
        fornecedores = {f"{row[0]}_{row[1]}": {"uf": row[2], "decreto": row[3]} for row in cursor.fetchall()}
        print(f"[INFO] {len(fornecedores)} fornecedores carregados")

        print("[PARTE 2] Carregando dados da tabela 0200")
        cursor.execute("""
            SELECT cod_item, empresa_id, descr_item, cod_ncm FROM `0200`
            WHERE empresa_id = %s
        """, (empresa_id,))
        dados_0200 = {f"{row[0]}_{row[1]}": {"descr_item": row[2], "cod_ncm": row[3]} for row in cursor.fetchall()}
        print(f"[INFO] {len(dados_0200)} produtos da 0200 carregados")

        print("[PARTE 3] Carregando produtos existentes no cadastro_tributacao")
        cursor.execute("""
            SELECT empresa_id, codigo, produto, ncm
            FROM cadastro_tributacao
            WHERE empresa_id = %s
        """, (empresa_id,))
        produtos_existentes = set()
        for row in cursor.fetchall():
            empresa_id_db, codigo_db, produto_db, ncm_db = row
            chave = f"{empresa_id_db}-{codigo_db}-{produto_db}-{ncm_db or ''}"
            produtos_existentes.add(chave)
        print(f"[INFO] {len(produtos_existentes)} produtos já cadastrados")

        print("[PARTE 4] Iniciando processamento em lotes")
        produtos_unicos_globais = set()
        
        while True:
            print(f"[LOTE] {offset} - {offset + lote_tamanho}")

            cursor.execute("""
                SELECT 
                    c.cod_item, c.descr_compl, c.empresa_id, cc.cod_part
                FROM c170 c
                JOIN c100 cc ON cc.id = c.id_c100
                WHERE c.empresa_id = %s
                AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116','2101', '2102', '2401', '2403', '2910', '2116')
                LIMIT %s OFFSET %s
            """, (empresa_id, lote_tamanho, offset))

            linhas = cursor.fetchall()
            if not linhas:
                break

            dados_para_inserir = []
            produtos_processados = set()

            for row in linhas:
                cod_item, descr_compl, empresa_id_row, cod_part = row

                chave_forn = f"{cod_part}_{empresa_id_row}"
                forn = fornecedores.get(chave_forn)
                if not forn:
                    continue

                if not ((forn['uf'] == 'CE' and forn['decreto'] == 'Não') or (forn['uf'] != 'CE')):
                    continue

                chave_0200 = f"{cod_item}_{empresa_id_row}"
                ref_0200 = dados_0200.get(chave_0200, {})
                produto = ref_0200.get("descr_item") or descr_compl
                ncm = ref_0200.get("cod_ncm") or ""

                chave_produto = f"{empresa_id_row}-{cod_item}-{produto}-{ncm}"
                
                if chave_produto in produtos_existentes:
                    continue
                
                if chave_produto in produtos_processados or chave_produto in produtos_unicos_globais:
                    continue

                dados_para_inserir.append((empresa_id_row, cod_item, produto, ncm, None, None))
                produtos_processados.add(chave_produto)
                produtos_unicos_globais.add(chave_produto)

            if dados_para_inserir:
                inseridos_lote = 0
                for item in dados_para_inserir:
                    try:
                        cursor.execute("""
                            INSERT IGNORE INTO cadastro_tributacao (
                                empresa_id, codigo, produto, ncm, aliquota, aliquota_antiga
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """, item)
                        if cursor.rowcount > 0:
                            inseridos_lote += 1
                            chave = f"{item[0]}-{item[1]}-{item[2]}-{item[3] or ''}"
                            produtos_existentes.add(chave)
                    except Exception as individual_error:
                        print(f"[ERRO] Falha ao inserir produto: {individual_error}")
                
                conexao.commit()
                total_inseridos += inseridos_lote
                print(f"[LOTE] {inseridos_lote} produtos inseridos")
            else:
                print("[LOTE] Nenhum produto novo para inserir")

            if len(linhas) < lote_tamanho:
                break

            offset += lote_tamanho

        print(f"[OK] {total_inseridos} códigos únicos inseridos na tabela cadastro_tributacao.")

    except Exception as e:
        print(f"[ERRO] Falha ao preencher cadastro_tributacao: {e}")
        conexao.rollback()
        raise

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Preenchimento de cadastro_tributacao concluído.")

    # async def preencherTributacao(empresa_id, parent=None):
    # print(f"[VERIFICAÇÃO] Preenchendo cadastro_tributacao com produtos da empresa_id={empresa_id}")
    #     conexao = conectarBanco()
    #     cursor = conexao.cursor()

    #     try:
    #         cursor.execute("""
    #             INSERT IGNORE INTO cadastro_tributacao (
    #                 empresa_id, codigo, produto, ncm, aliquota, aliquota_antiga
    #             )
    #             SELECT 
    #                 sub.empresa_id,
    #                 sub.cod_item AS codigo,
    #                 sub.produto,
    #                 sub.ncm,
    #                 NULL,
    #                 NULL
    #             FROM (
    #                 SELECT 
    #                     c.empresa_id,
    #                     c.cod_item,
    #                     COALESCE(p.descr_item, c.descr_compl) AS produto,
    #                     p.cod_ncm AS ncm
    #                 FROM c170 c
    #                 JOIN c100 cc 
    #                     ON cc.id = c.id_c100
    #                 JOIN cadastro_fornecedores f 
    #                     ON cc.cod_part = f.cod_part
    #                     AND f.empresa_id = c.empresa_id
    #                 LEFT JOIN `0200` p 
    #                     ON c.cod_item = p.cod_item 
    #                     AND p.empresa_id = c.empresa_id
    #                 WHERE c.empresa_id = %s
    #                 AND c.cfop IN (
    #                         '1101', '1401', '1102', '1403', '1910', '1116',
    #                         '2101', '2102', '2401', '2403', '2910', '2116'
    #                 )
    #                 AND (
    #                         (f.uf = 'CE' AND f.decreto = 'Não')
    #                         OR
    #                         (f.uf <> 'CE')
    #                 )
    #             ) AS sub
    #             WHERE NOT EXISTS (
    #                 SELECT 1 FROM cadastro_tributacao ct
    #                 WHERE ct.empresa_id = sub.empresa_id
    #                 AND ct.codigo = sub.cod_item
    #                 AND ct.produto = sub.produto
    #                 AND ct.ncm = sub.ncm
    #             )
    #         """, (empresa_id,))

    #         novos = cursor.rowcount
    #         conexao.commit()
    #         print(f"[OK] {novos} códigos únicos inseridos na tabela cadastro_tributacao.")

    #     except Exception as e:
    #         print(f"[ERRO] Falha ao preencher cadastro_tributacao: {e}")
    #         conexao.rollback()

    #     finally:
    #         cursor.close()
    #         fecharBanco(conexao)
    #         print("[FIM] Preenchimento de cadastro_tributacao concluído.")