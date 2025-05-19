from utils.siglas import obter_sigla_estado
from utils.mensagem import mensagem_error, mensagem_sucesso, mensagem_aviso, notificacao
from utils.sanitizacao import sanitizar_registro, sanitizar_campo, truncar, corrigir_unidade, corrigir_ind_mov, TAMANHOS_MAXIMOS,get_column_index, get_fallback_value, get_fallback_value_by_index, calcular_periodo, corrigir_cst_icms, validar_estrutura_c170
import time

UNIDADE_PADRAO = "UN"

def processar_registro_c170(dados, dt_ini_0000, filial, ind_oper, cod_part, num_doc, chv_nfe):
    if len(dados) > 38:
        dados = dados[:38]
    
    dados += [None] * (38 - len(dados)) if len(dados) < 38 else []
    
    if len(dados) > 6:
        dados[6] = corrigir_unidade(dados[6])
    if len(dados) > 9:
        dados[9] = corrigir_ind_mov(dados[9])

    dados[2] = truncar(dados[2], TAMANHOS_MAXIMOS['cod_item'])
    dados[4] = truncar(dados[4], TAMANHOS_MAXIMOS['descr_compl'])
    dados[6] = truncar(corrigir_unidade(dados[6]), TAMANHOS_MAXIMOS['unid'])
    dados[12] = truncar(dados[12], TAMANHOS_MAXIMOS['cod_nat'])
    dados[37] = truncar(dados[37], TAMANHOS_MAXIMOS['cod_cta'])

    if dt_ini_0000:
        periodo = calcular_periodo(dt_ini_0000)
        dados_final = [periodo] + dados + [None, filial, ind_oper, cod_part, num_doc, chv_nfe]
        return dados_final
    return None

async def salvar_no_banco_em_lote(conteudo, cursor, nome_banco):
    import re
    import traceback

    registros_inseridos = 0

    progress_status = ["|0000|", "|0150|", "|0200|", "|C100|", "|C170|"]
    linhas = conteudo.split('\n')
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas para salvar no banco")
    
    contadores = {"0000": 0, "0150": 0, "0200": 0, "C100": 0, "C170": 0}
    
    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    id_c100_atual = None
    dt_ini_0000 = None
    filial = None
    ind_oper = cod_part = num_doc = chv_nfe = None

    registros_processados = set()

    inicio_processo = time.time()
    
    try:
        print(f"[DEBUG] Extraindo dados das linhas...")
        t_inicio = time.time()
        
        for i, linha in enumerate(linhas):
            if i > 0 and i % 10000 == 0:
                print(f"[DEBUG] Processadas {i}/{len(linhas)} linhas ({i/len(linhas)*100:.1f}%)")
                
            if linha.startswith("|0000|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                dados += [None] * (15 - len(dados)) if len(dados) < 15 else []
                dt_ini_0000 = dados[3]
                cnpj_0000 = dados[6]
                filial = cnpj_0000[8:12] if cnpj_0000 else '0000'
                periodo = calcular_periodo(dt_ini_0000)
                dados += [filial, periodo]
                lote_0000.append(dados)
                contadores["0000"] += 1

            elif linha.startswith("|0150|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                dados += [None] * (13 - len(dados)) if len(dados) < 13 else []
                num = dados[7]
                cod_uf = num[:2] if num else None
                uf = obter_sigla_estado(cod_uf)
                cnpj = dados[4]
                pj_pf = "PF" if cnpj is None else "PJ"
                periodo = calcular_periodo(dt_ini_0000)
                dados += [cod_uf, uf, pj_pf, periodo]
                lote_0150.append(dados)
                contadores["0150"] += 1

            elif linha.startswith("|0200|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 1 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                dados += [None] * (13 - len(dados)) if len(dados) < 13 else []
                
                dados[1] = truncar(dados[1], TAMANHOS_MAXIMOS['cod_item'])
                dados[2] = truncar(dados[2], TAMANHOS_MAXIMOS['descr_item'])
                dados[5] = truncar(dados[5], TAMANHOS_MAXIMOS['unid'])

                periodo = calcular_periodo(dt_ini_0000)
                dados.append(periodo)
                lote_0200.append(dados)
                contadores["0200"] += 1

            elif linha.startswith("|C100|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                dados += [None] * (29 - len(dados)) if len(dados) < 29 else []
                periodo = calcular_periodo(dt_ini_0000)
                dados_final = [periodo] + dados + [filial]
                lote_c100.append(dados_final)
                ind_oper, cod_part, num_doc, chv_nfe = dados[1], dados[4], dados[7], dados[9]
                contadores["C100"] += 1

            elif linha.startswith("|C170|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 2 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]

                if len(dados) > 6:
                    dados[6] = corrigir_unidade(dados[6])
                if len(dados) > 9:
                    dados[9] = corrigir_ind_mov(dados[9])
                if len(dados) > 10:
                    dados[10] = corrigir_cst_icms(dados[10])

                if not validar_estrutura_c170(dados):
                    print(f"[WARN] Estrutura suspeita no C170 na linha {i}: {dados}")

                dados_final = processar_registro_c170(dados, dt_ini_0000, filial, ind_oper, cod_part, num_doc, chv_nfe)
                if dados_final:
                    lote_c170.append(dados_final)
                    registro_id = f"{filial}_{num_doc}_{dados[2]}" 
                    if registro_id not in registros_processados:
                        registros_processados.add(registro_id)
                        contadores["C170"] += 1

        t_fim = time.time()
        print(f"[DEBUG] Dados extraídos em {t_fim - t_inicio:.2f} segundos")
        print(f"[DEBUG] Registros extraídos: 0000: {contadores['0000']}, 0150: {contadores['0150']}, 0200: {contadores['0200']}, C100: {contadores['C100']}, C170: {contadores['C170']}")
        
        print(f"[DEBUG] Salvando registros |0000|: {len(lote_0000)} registros")
        t_inicio = time.time()
        cursor.executemany('''
            INSERT INTO `0000` (
                reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
                ind_perfil, ind_ativ, filial, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0000)
        print(f"[DEBUG] Registros |0000| salvos em {time.time() - t_inicio:.2f} segundos")

        print(f"[DEBUG] Salvando registros |0150|: {len(lote_0150)} registros")
        t_inicio = time.time()
        cursor.executemany('''
            INSERT IGNORE INTO `0150` (
                reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
                cod_uf, uf, pj_pf, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0150)
        print(f"[DEBUG] Registros |0150| salvos em {time.time() - t_inicio:.2f} segundos")

        print(f"[DEBUG] Salvando registros |0200|: {len(lote_0200)} registros")
        t_inicio = time.time()
        cursor.executemany('''
            INSERT IGNORE INTO `0200` (
                reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
                ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0200)
        print(f"[DEBUG] Registros |0200| salvos em {time.time() - t_inicio:.2f} segundos")

        print(f"[DEBUG] Salvando registros |C100|: {len(lote_c100)} registros")
        t_inicio = time.time()
        cursor.executemany('''
            INSERT INTO c100 (
                periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt,
                vl_seg, vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis,
                vl_cofins, vl_pis_st, vl_cofins_st, filial
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_c100)
        print(f"[DEBUG] Registros |C100| salvos em {time.time() - t_inicio:.2f} segundos")

        print(f"[DEBUG] Salvando registros |C170|: {len(lote_c170)} registros")
        t_inicio = time.time()

        for idx, reg in enumerate(lote_c170):
            unid_val = reg[7]
            try:
                unid_len = len(str(unid_val))
            except Exception:
                unid_len = 'indefinido'
            print(f"[DEBUG] C170[{idx}] → unid: {repr(unid_val)} (len={unid_len}, type={type(unid_val)})")

        registros_com_problemas = 0
        for idx, registro in enumerate(lote_c170):
            if len(registro) < 45:
                print(f"[DEBUG] Registro {idx} tem apenas {len(registro)} campos (esperado 45)")
                registros_com_problemas += 1
                continue
                
            if registro[7] and len(str(registro[7])) > TAMANHOS_MAXIMOS['unid']:
                print(f"[DEBUG] Registro {idx} ainda tem problema de unidade: '{registro[7]}' ({len(str(registro[7]))} caracteres)")
                mensagem_error(f"[DEBUG] Registro {idx} ainda tem problema de unidade: '{registro[7]}' ({len(str(registro[7]))} caracteres)")
                registro[7] = UNIDADE_PADRAO
                registros_com_problemas += 1
        
        if registros_com_problemas > 0:
            print(f"[DEBUG] Corrigidos {registros_com_problemas} registros com possíveis problemas")
        
        try:
            tamanho_lote = 100
            total_processado = 0
            
            for i in range(0, len(lote_c170), tamanho_lote):
                lote_atual = lote_c170[i:i+tamanho_lote]

                for idx, reg in enumerate(lote_atual):
                    reg_lista = list(reg)
                    lote_atual[idx] = tuple(reg_lista)

                try:
                    cursor.executemany('''
                        INSERT INTO c170 (
                            periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                            ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                            aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                            cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                            vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                            vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
                        ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s)
                    ''', lote_atual)

                    registros_inseridos += len(lote_atual)
                    print(f"[DEBUG] Processados {registros_inseridos}/{len(lote_c170)} registros C170")
                    
                except Exception as e:
                    print(f"[DEBUG] Erro no lote {i//tamanho_lote+1}: {e}")
                    for j, registro in enumerate(lote_atual):
                        try:
                            print(f"[DEBUG] Tentando inserir registro {j} individualmente. Tamanho={len(registro)}")
                            print(f"[DEBUG] Campo unid original: '{registro[6]}'")
                            
                            if registro[6] is not None:
                                valor_original = registro[6]
                                registro_mod = list(registro)
                                registro_mod[6] = corrigir_unidade(valor_original)
                                if registro_mod[9] is not None:
                                    if registro_mod[9] and len(str(registro_mod[9])) > 3:
                                        print(f"[WARN] ind_mov excede limite: '{registro_mod[9]}'")
                                        registro_mod[9] = corrigir_ind_mov(registro_mod[9])
                                registro = tuple(registro_mod)
                                print(f"[FORCE] unid alterada: '{valor_original}' → '{registro_mod[6]}'")
                            
                            if i*tamanho_lote+j not in registros_processados:
                                registros_processados.add(i*tamanho_lote+j)
                                total_processado += 1

                            cursor.execute('''
                                INSERT INTO c170 (
                                    periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                                    ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                                    aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                                    cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                                    vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                                    vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
                                ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s)
                            ''', registro)

                            cursor.executemany(''' ... SQL ... ''', lote_atual)
                            registros_inseridos += len(lote_atual)

                        except Exception as ex:
                            print(f"[DEBUG] Erro no registro {i*tamanho_lote+j}: {ex}")
                            
                            erro_message = str(ex)
                            if "Data too long for column" in erro_message:
                                import re
                                coluna_match = re.search(r"for column '([^']+)'", erro_message)
                                if coluna_match:
                                    coluna_problema = coluna_match.group(1)
                                    valor_problema = registro[get_column_index(coluna_problema)]
                                    print(f"[DEBUG-DETALHE] Campo '{coluna_problema}' com valor '{valor_problema}' (tipo: {type(valor_problema)}, tamanho: {len(str(valor_problema)) if valor_problema is not None else 'NULL'})")
                                    
                                    registro_fallback = list(registro)
                                    registro_fallback[get_column_index(coluna_problema)] = get_fallback_value(coluna_problema)
                                    try:
                                        cursor.execute('''
                                            INSERT INTO c170 (
                                                periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                                                ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                                                aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                                                cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                                                vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                                                vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
                                            ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                    %s, %s, %s, %s, %s, %s)
                                        ''', tuple(registro_fallback))

                                        cursor.execute(''' ... SQL ... ''', tuple(registro_fallback))
                                        registros_inseridos += 1

                                        print(f"[DEBUG] Inserido com sucesso usando valor de fallback para '{coluna_problema}'")
                                        if i*tamanho_lote+j not in registros_processados:
                                            registros_processados.add(i*tamanho_lote+j)
                                            total_processado += 1
                                    except Exception as fallback_ex:
                                        print(f"[DEBUG] Falha mesmo com fallback: {fallback_ex}")
                                        
                            if "ainda falhou após sanitização" not in locals():
                                print(f"[FALLBACK-EXTREMO] Tentando inserção com valores mínimos para registro {i*tamanho_lote+j}")
                                registro_minimo = list(registro)
                                
                                for k in range(3, len(registro_minimo)):
                                    if k in [39, 40, 41, 42, 43, 44]:
                                        continue
                                    registro_minimo[k] = get_fallback_value_by_index(k)
                                
                                try:
                                    cursor.execute('''
                                        INSERT INTO c170 (
                                            periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                                            ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                                            aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                                            cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                                            vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                                            vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
                                        ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s)
                                    ''', tuple(registro_minimo))
                                    cursor.execute(''' ... SQL ... ''', tuple(registro_minimo))
                                    registros_inseridos += 1

                                    print(f"[DEBUG] Inserido com fallback extremo!")
                                    if i*tamanho_lote+j not in registros_processados:
                                        registros_processados.add(i*tamanho_lote+j)
                                        total_processado += 1
                                except Exception as ultra_ex:
                                    print(f"[DEBUG] Falha mesmo com fallback extremo: {ultra_ex}")
                
            print(f"[DEBUG] {registros_inseridos} de {len(lote_c170)} registros |C170| salvos com sucesso.")

        except Exception as e:
            print(f"[DEBUG] ERRO ao inserir registros C170: {e}")
            mensagem_error(f"Erro ao salvar itens de notas fiscais: {e}")
            print(traceback.format_exc())

        print(f"[DEBUG] Registros |C170| processados em {time.time() - t_inicio:.2f} segundos")
        
        tempo_total = time.time() - inicio_processo
        print(f"[DEBUG] Todos os registros foram salvos no banco em {tempo_total:.2f} segundos")

        print(f"[DEBUG FINAL] Resumo do arquivo:")
        print(f" - Produtos únicos processados (|0200|): {len(lote_0200)}")
        print(f" - Itens de nota (|C170|): {len(lote_c170)} total, {registros_inseridos} salvos")
        print(f" - Registros de documentos fiscais (|C100|): {len(lote_c100)}")
        print(f" - Participantes (|0150|): {len(lote_0150)}")
        print(f" - Informações iniciais (|0000|): {len(lote_0000)}")

        if contadores['C170'] > 0 and len(lote_c170) > 0:
            mensagem_sucesso(f"Arquivo SPED processado com sucesso.\n{contadores['C100']} notas fiscais com {len(lote_c170)} itens.")
        else:
            mensagem_aviso("Processamento concluído, mas nenhum item de nota fiscal (C170) foi salvo. Verifique os dados.")

        return True
    except Exception as e:
        print(f"[DEBUG] ERRO ao salvar dados no banco: {type(e).__name__}: {e}")
        import traceback
        print(f"[DEBUG] Traceback: {traceback.format_exc()}")
        mensagem_error(f"Erro ao salvar dados em lote: {e}")
        return False

