import time
import traceback
from utils.siglas import obter_sigla_estado
from PySide6.QtCore import QMetaObject, Qt, QTimer
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from utils.sanitizacao import truncar, corrigir_unidade, corrigir_ind_mov, corrigir_cst_icms,TAMANHOS_MAXIMOS, get_column_index, get_fallback_value, get_fallback_value_by_index,calcular_periodo, validar_estrutura_c170
    
UNIDADE_PADRAO = "UN"

async def salvar_no_banco_em_lote(conteudo, cursor, nome_banco, janela=None):
    linhas = conteudo.split('\n')
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas")

    contadores = {"0000": 0, "0150": 0, "0200": 0, "C100": 0, "C170": 0, "salvos": 0, "erros": 0}
    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    registros_processados = set()

    dt_ini_0000 = None
    filial = None
    ind_oper = cod_part = num_doc = chv_nfe = None

    def inserir_lote(sql, lote, descricao):
        if not lote: return
        try:
            cursor.executemany(sql, lote)
            print(f"[OK] {descricao}: {len(lote)} registros inseridos.")
            contadores["salvos"] += len(lote)
        except Exception as e:
            if "Duplicate entry" in str(e):
                print(f"[WARN] Ignorando entradas duplicadas em {descricao}")
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
                partes += [filial, calcular_periodo(dt_ini_0000)]
                lote_0000.append(partes)
                contadores["0000"] += 1

            elif linha.startswith("|0150|"):
                partes += [None] * (13 - len(partes))
                municipio = partes[7]
                cod_uf = municipio[:2] if municipio else None
                uf = obter_sigla_estado(cod_uf)
                pj_pf = "PF" if partes[4] is None else "PJ"
                partes += [cod_uf, uf, pj_pf, calcular_periodo(dt_ini_0000)]
                lote_0150.append(partes)
                contadores["0150"] += 1

            elif linha.startswith("|0200|"):
                partes += [None] * (13 - len(partes))
                partes[1] = truncar(partes[1], TAMANHOS_MAXIMOS['cod_item'])
                partes[2] = truncar(partes[2], TAMANHOS_MAXIMOS['descr_item'])
                partes[5] = truncar(partes[5], TAMANHOS_MAXIMOS['unid'])
                partes.append(calcular_periodo(dt_ini_0000))
                lote_0200.append(partes)
                contadores["0200"] += 1

            elif linha.startswith("|C100|"):
                partes += [None] * (29 - len(partes))
                ind_oper, cod_part, num_doc, chv_nfe = partes[1], partes[4], partes[7], partes[9]
                registro = [calcular_periodo(dt_ini_0000)] + partes + [filial]
                lote_c100.append(registro)
                contadores["C100"] += 1

            elif linha.startswith("|C170|"):
                partes += [None] * (38 - len(partes))
                if len(partes) < 12: continue

                if not num_doc:
                    print(f"[DEBUG CRÍTICO] num_doc indefinido antes do registro C170: linha={linha}")
                    continue
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
                    None,                           # 40 id_c100
                    filial,                         # 41 filial
                    ind_oper,                       # 42 ind_oper
                    cod_part,                       # 43 cod_part
                    num_doc,                        # 44 num_doc
                    chv_nfe                         # 45 chv_nfe
                ]

                if len(dados) != 45:
                    print(f"[ERRO] Registro com tamanho inválido: {len(dados)} (esperado: 45)")
                    continue

                if not validar_estrutura_c170(dados):
                    print(f"[WARN] C170 inválido: {dados}")
                    continue

                lote_c170.append(dados)
                registros_processados.add(registro_id)
                contadores["C170"] += 1

        inserir_lote("""
            INSERT INTO `0000` (reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
            ind_perfil, ind_ativ, filial, periodo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0000, "|0000|")

        inserir_lote("""
            INSERT INTO `0150` (reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
            cod_uf, uf, pj_pf, periodo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0150, "|0150|")

        inserir_lote("""
            INSERT INTO `0200` (reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
            ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0200, "|0200|")

        inserir_lote("""
            INSERT INTO c100 (periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
            dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt, vl_seg, vl_out_da,
            vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis, vl_cofins, vl_pis_st, vl_cofins_st, filial)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_c100, "|C100|")

        print(f"[DEBUG] Tentando salvar {len(lote_c170)} registros C170")
        tamanho_lote = 100
        for i in range(0, len(lote_c170), tamanho_lote):
            lote_atual = lote_c170[i:i+tamanho_lote]

            lote_ajustado = []
            for registro in lote_atual:
                if len(registro) > 45:
                    registro_ajustado = registro[:45]
                elif len(registro) < 45:
                    registro_ajustado = registro + [None] * (45 - len(registro))
                else:
                    registro_ajustado = registro

                registro_ajustado[6] = truncar(corrigir_unidade(registro_ajustado[6]), TAMANHOS_MAXIMOS['unid'])
                registro_ajustado[9] = corrigir_ind_mov(registro_ajustado[9])

                if registro_ajustado[40] in ['', None]:
                    registro_ajustado[40] = None
                else:
                    try:
                        registro_ajustado[40] = int(registro_ajustado[40])
                    except ValueError:
                        registro_ajustado[40] = None

                lote_ajustado.append(registro_ajustado)

            try:
                print(f"[DEBUG] Tentando salvar lote C170 {i}-{i+len(lote_ajustado)}")
                cursor.executemany("""
                    INSERT INTO c170 (
                        periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                        ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                        aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                        cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                        vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                        vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
                    )VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                """, lote_ajustado)
                print(f"[OK] Lote C170 {i}-{i+len(lote_ajustado)}: {len(lote_ajustado)} registros inseridos")
                contadores["salvos"] += len(lote_ajustado)
            except Exception as e:
                contadores["erros"] += len(lote_ajustado)
                print(f"[ERRO] Lote C170 {i}-{i+len(lote_ajustado)}: {e}")

        print("[DEBUG] Iniciando verificação de produtos no cadastro_tributacao")
        try:
            codigos_inseridos = set()
            for reg in lote_c170:
                cod_item = reg[3]
                produto = reg[4]
                ncm = next((r[7] for r in lote_0200 if r[1] == cod_item), None)

                if cod_item in codigos_inseridos:
                    continue
                
                cursor.execute("SELECT 1 FROM cadastro_tributacao WHERE codigo = %s LIMIT 1", (cod_item,))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO cadastro_tributacao (codigo, produto, ncm, aliquota)
                        VALUES (%s, %s, %s, NULL)
                    """, (cod_item, produto, ncm))
                    codigos_inseridos.add(cod_item)

            print(f"[DEBUG] {len(codigos_inseridos)} produtos adicionados ao cadastro_tributacao com aliquota NULL")
        except Exception as e:
            print(f"[ERRO] Falha ao popular cadastro_tributacao: {e}")

        print(f"[FINAL] Processamento concluído: {contadores}")
        return f"Processado com sucesso. {contadores['salvos']} itens salvos, {contadores['erros']} com erro."

    except Exception as e:
        print("[FATAL] Erro durante o salvamento:", e)
        print(traceback.format_exc())
        return f"Erro geral ao salvar: {e}"

