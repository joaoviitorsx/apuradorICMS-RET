import time
import traceback
from utils.siglas import obter_sigla_estado
from PySide6.QtCore import QMetaObject, Qt, QTimer
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from utils.sanitizacao import truncar, corrigir_unidade, corrigir_ind_mov, corrigir_cst_icms,TAMANHOS_MAXIMOS, get_column_index, get_fallback_value,get_fallback_value_by_index, calcular_periodo, validar_estrutura_c170
from services.spedService.atualizacoes import atualizar_aliquota
from db.conexao import conectar_banco, fechar_banco

UNIDADE_PADRAO = "UN"

async def salvar_no_banco_em_lote(conteudo, cursor, conexao, empresa_id, janela=None):
    linhas = conteudo 
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas")

    contadores = {"0000": 0, "0150": 0, "0200": 0, "C100": 0, "C170": 0, "salvos": 0, "erros": 0}
    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    registros_processados = set()

    dt_ini_0000 = None
    filial = None
    num_doc = None

    mapa_documentos = {}

    def inserir_lote(sql, lote, descricao):
        if not lote: return
        try:
            cursor.executemany(sql, lote)
            contadores["salvos"] += len(lote)
        except Exception as e:
            if "Duplicate entry" in str(e):
                salvos = 0
                for item in lote:
                    try:
                        cursor.execute(sql, item)
                        salvos += 1
                    except Exception as e_item:
                        if "Duplicate entry" not in str(e_item):
                            print(f"[ERRO] Falha ao inserir item em {descricao}: {e_item}")
                            contadores["erros"] += 1
                print(f"[PARCIAL] {descricao}: {salvos}/{len(lote)} registros inseridos após tratar duplicidades.")
                contadores["salvos"] += salvos
            else:
                print(f"[ERRO] Falha ao inserir {descricao}: {e}")
                contadores["erros"] += len(lote)

    try:
        for linha in linhas:
            if not linha.strip(): continue
            partes = linha.split('|')[1:-1]

            if linha.startswith("|0000|"):
                partes += [None] * (15 - len(partes))
                dt_ini_0000 = partes[3]
                cnpj = partes[6]
                filial = cnpj[8:12] if cnpj else '0000'
                partes += [filial, calcular_periodo(dt_ini_0000), empresa_id]
                lote_0000.append(partes)
                contadores["0000"] += 1

            elif linha.startswith("|0150|"):
                partes += [None] * (13 - len(partes))
                municipio = partes[7]
                cod_uf = municipio[:2] if municipio else None
                uf = obter_sigla_estado(cod_uf)
                pj_pf = "PF" if partes[4] is None else "PJ"
                partes += [cod_uf, uf, pj_pf, calcular_periodo(dt_ini_0000), empresa_id]
                lote_0150.append(partes)
                contadores["0150"] += 1

            elif linha.startswith("|0200|"):
                partes += [None] * (13 - len(partes))
                partes[1] = truncar(partes[1], TAMANHOS_MAXIMOS['cod_item'])
                partes[2] = truncar(partes[2], TAMANHOS_MAXIMOS['descr_item'])
                partes[5] = truncar(partes[5], TAMANHOS_MAXIMOS['unid'])
                partes.append(calcular_periodo(dt_ini_0000))
                partes.append(empresa_id)
                lote_0200.append(partes)
                contadores["0200"] += 1

            elif linha.startswith("|C100|"):
                partes += [None] * (29 - len(partes))
                ind_oper, cod_part, num_doc, chv_nfe = partes[1], partes[4], partes[7], partes[9]

                if num_doc:
                    mapa_documentos[num_doc] = {
                        "ind_oper": ind_oper,
                        "cod_part": cod_part,
                        "chv_nfe": chv_nfe
                    }

                registro = [calcular_periodo(dt_ini_0000)] + partes + [filial, empresa_id]

                cursor.execute("""
                    INSERT INTO c100 (
                        periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                        dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt, vl_seg,
                        vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis, vl_cofins,
                        vl_pis_st, vl_cofins_st, filial, empresa_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s)
                """, registro)

                id_c100 = cursor.lastrowid

                if num_doc:
                    mapa_documentos[num_doc] = {
                        "ind_oper": ind_oper,
                        "cod_part": cod_part,
                        "chv_nfe": chv_nfe,
                        "id_c100": id_c100
                    }

                contadores["C100"] += 1


            elif linha.startswith("|C170|"):
                partes += [None] * (39 - len(partes))
                if len(partes) < 12: continue
                if not num_doc:
                    print(f"[DEBUG CRÍTICO] num_doc indefinido antes do registro C170: linha={linha}")
                    continue

                # Buscar dados corretos de C100
                dados_doc = mapa_documentos.get(num_doc)
                if not dados_doc:
                    print(f"[WARN] Documento {num_doc} não encontrado no mapa. Ignorando linha C170.")
                    continue

                ind_oper = dados_doc["ind_oper"]
                cod_part = dados_doc["cod_part"]
                chv_nfe = dados_doc["chv_nfe"]

                partes[10] = corrigir_cst_icms(partes[10])
                partes[6] = truncar(corrigir_unidade(partes[6]), TAMANHOS_MAXIMOS['unid'])
                partes[9] = corrigir_ind_mov(partes[9])
                partes[2] = truncar(partes[2], TAMANHOS_MAXIMOS['cod_item'])
                partes[4] = truncar(partes[4], TAMANHOS_MAXIMOS['descr_compl'])
                partes[12] = truncar(partes[12], TAMANHOS_MAXIMOS['cod_nat'])
                partes[37] = truncar(partes[37], TAMANHOS_MAXIMOS['cod_cta'])

                registro_id = f"{filial}_{num_doc}_{partes[2]}"
                if registro_id in registros_processados:
                    continue

                dados = [
                    calcular_periodo(dt_ini_0000),  # 1 periodo
                    partes[0],                      # 2 reg
                    partes[1],                      # 3 num_item
                    partes[2],                      # 4 cod_item
                    partes[4],                      # 5 descr_compl
                    partes[5],                      # 6 qtd
                    partes[6],                      # 7 unid
                    partes[7],                      # 8 vl_item
                    partes[8],                      # 9 vl_desc
                    partes[9],                      # 10 ind_mov
                    partes[10],                     # 11 cst_icms
                    partes[11],                     # 12 cfop
                    partes[12],                     # 13 cod_nat
                    partes[13],                     # 14 vl_bc_icms
                    partes[14],                     # 15 aliq_icms
                    partes[15],                     # 16 vl_icms
                    partes[16],                     # 17 vl_bc_icms_st
                    partes[17],                     # 18 aliq_st
                    partes[18],                     # 19 vl_icms_st
                    partes[19],                     # 20 ind_apur
                    partes[20],                     # 21 cst_ipi
                    partes[21],                     # 22 cod_enq
                    partes[22],                     # 23 vl_bc_ipi
                    partes[23],                     # 24 aliq_ipi
                    partes[24],                     # 25 vl_ipi
                    partes[25],                     # 26 cst_pis
                    partes[26],                     # 27 vl_bc_pis
                    partes[27],                     # 28 aliq_pis
                    partes[28],                     # 29 quant_bc_pis
                    partes[29],                     # 30 aliq_pis_reais
                    partes[30],                     # 31 vl_pis
                    partes[31],                     # 32 cst_cofins
                    partes[32],                     # 33 vl_bc_cofins
                    partes[33],                     # 34 aliq_cofins
                    partes[34],                     # 35 quant_bc_cofins
                    partes[35],                     # 36 aliq_cofins_reais
                    partes[36],                     # 37 vl_cofins
                    partes[37],                     # 38 cod_cta
                    partes[38],                     # 39 vl_abat_nt
                    id_c100,                        # 40 id_c100
                    filial,                         # 41 filial
                    ind_oper,                       # 42 ind_oper
                    cod_part,                       # 43 cod_part
                    num_doc,                        # 44 num_doc
                    chv_nfe,                        # 45 chv_nfe
                    empresa_id                      # 46 empresa_id
                ]

                if len(dados) != 46:
                    print(f"[ERRO] Registro com tamanho inválido: {len(dados)} (esperado: 46)")
                    continue

                if not validar_estrutura_c170(dados):
                    continue

                lote_c170.append(dados)
                registros_processados.add(registro_id)
                contadores["C170"] += 1

        inserir_lote("""
            INSERT INTO `0000` (reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
            ind_perfil, ind_ativ, filial, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0000, "|0000|")

        inserir_lote("""
            INSERT INTO `0150` (reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
            cod_uf, uf, pj_pf, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0150, "|0150|")

        inserir_lote("""
            INSERT INTO `0200` (reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
            ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0200, "|0200|")

        inserir_lote("""
            INSERT INTO c100 (
                periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt, vl_seg,
                vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis, vl_cofins,
                vl_pis_st, vl_cofins_st, filial, empresa_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_c100, "|C100|")

        for i in range(0, len(lote_c170), 100):
            lote = lote_c170[i:i+100]
            #print(f"[DEBUG] Tentando inserir lote C170 {i}-{i+len(lote)}, primeiro registro: {lote[0][:5]}...")
            try:
                cursor.executemany("""
                    INSERT INTO c170 (
                        periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                        ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                        aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                        cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                        vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                        vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, empresa_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                """, lote)
                contadores["salvos"] += len(lote)

                cursor.execute("SELECT COUNT(*) FROM c170 WHERE periodo = %s AND empresa_id = %s",(calcular_periodo(dt_ini_0000), empresa_id))
                total_registros = cursor.fetchone()[0]
                print(f"[DEBUG] Total de registros em c170 após inserção: {total_registros}")
            except Exception as e:
                contadores["erros"] += len(lote)
                print(f"[ERRO] Falha no lote C170 {i}-{i+len(lote)}: {e}")

        conexao.commit()
        print(f"[FINAL] Processamento concluído: {contadores}")
        return f"Processado com sucesso. {contadores['salvos']} itens salvos, {contadores['erros']} com erro."

    except Exception as e:
        print("[FATAL] Erro durante o salvamento:", e)
        print(traceback.format_exc())
        return f"Erro geral ao salvar: {e}"