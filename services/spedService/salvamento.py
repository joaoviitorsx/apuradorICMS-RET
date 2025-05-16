import re
import traceback
from utils.siglas import obter_sigla_estado
from utils.mensagem import mensagem_error, mensagem_sucesso, mensagem_aviso, notificacao
import time

TAMANHOS_MAXIMOS = {
    'unid': 6,
    'cod_item': 60,
    'descr_item': 255,
    'descr_compl': 255,
    'cod_nat': 10,
    'cod_cta': 255
}

UNIDADE_PADRAO = "UN"

def corrigir_unidade(valor):
    try:
        if valor is None:
            return "U"  # Apenas um caractere

        valor_str = str(valor).strip().upper()
        valor_str = re.sub(r'[\x00-\x1F\x7F\r\n]', '', valor_str)

        if not valor_str or any(c in valor_str for c in ',.'):
            return "U"  # Apenas um caractere

        if all(c.isdigit() for c in valor_str):
            return "U"  # Apenas um caractere

        # Retornar apenas o primeiro caractere para garantir compatibilidade
        return valor_str[0:1]  # Apenas um caractere
    except Exception as e:
        print(f"[ERRO] Falha ao corrigir unidade '{valor}': {e}")
        return "U"  # Apenas um caractere


def truncar_dados(dados, indice, campo, tamanho_maximo):
    if indice >= len(dados):
        return dados
    
    if dados[indice] is None:
        return dados
        
    if campo == 'unid':
        valor_original = dados[indice]
        dados[indice] = corrigir_unidade(dados[indice])
        if valor_original != dados[indice]:
            print(f"[DEBUG] Corrigido campo '{campo}': '{valor_original}' → '{dados[indice]}'")
        return dados
        
    if isinstance(dados[indice], str) and len(dados[indice]) > tamanho_maximo:
        valor_original = dados[indice]
        dados[indice] = dados[indice][:tamanho_maximo]
        print(f"[DEBUG] Truncado campo '{campo}': '{valor_original}' → '{dados[indice]}'")
    
    return dados

def processar_registro_c170(dados, dt_ini_0000, filial, ind_oper, cod_part, num_doc, chv_nfe):
    if len(dados) > 38:
        dados = dados[:38]
    
    dados += [None] * (38 - len(dados)) if len(dados) < 38 else []
    
    if len(dados) > 6:
        dados[6] = corrigir_unidade(dados[6])

    if len(dados) > 2:
        dados = truncar_dados(dados, 2, 'cod_item', TAMANHOS_MAXIMOS['cod_item'])
    if len(dados) > 4:
        dados = truncar_dados(dados, 4, 'descr_compl', TAMANHOS_MAXIMOS['descr_compl'])
    if len(dados) > 6:
        dados = truncar_dados(dados, 6, 'unid', TAMANHOS_MAXIMOS['unid'])
    if len(dados) > 12:
        dados = truncar_dados(dados, 12, 'cod_nat', TAMANHOS_MAXIMOS['cod_nat'])
    if len(dados) > 37:
        dados = truncar_dados(dados, 37, 'cod_cta', TAMANHOS_MAXIMOS['cod_cta'])
    
    if dt_ini_0000:
        periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}'
        dados_final = [periodo] + dados + [None, filial, ind_oper, cod_part, num_doc, chv_nfe]
        return dados_final
    return None

async def salvar_no_banco_em_lote(conteudo, cursor, nome_banco):
    progress_status = ["|0000|", "|0150|", "|0200|", "|C100|", "|C170|"]
    linhas = conteudo.split('\n')
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas para salvar no banco")
    
    contadores = {"0000": 0, "0150": 0, "0200": 0, "C100": 0, "C170": 0}
    
    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    id_c100_atual = None
    dt_ini_0000 = None
    filial = None
    ind_oper = cod_part = num_doc = chv_nfe = None

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
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
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
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados += [cod_uf, uf, pj_pf, periodo]
                lote_0150.append(dados)
                contadores["0150"] += 1

            elif linha.startswith("|0200|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 1 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                dados += [None] * (13 - len(dados)) if len(dados) < 13 else []
                
                dados = truncar_dados(dados, 1, 'cod_item', TAMANHOS_MAXIMOS['cod_item'])
                dados = truncar_dados(dados, 2, 'descr_item', TAMANHOS_MAXIMOS['descr_item'])
                dados = truncar_dados(dados, 5, 'unid', TAMANHOS_MAXIMOS['unid'])
                
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados.append(periodo)
                lote_0200.append(dados)
                contadores["0200"] += 1

            elif linha.startswith("|C100|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                dados += [None] * (29 - len(dados)) if len(dados) < 29 else []
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados_final = [periodo] + dados + [filial]
                lote_c100.append(dados_final)
                ind_oper, cod_part, num_doc, chv_nfe = dados[1], dados[4], dados[7], dados[9]
                contadores["C100"] += 1

            elif linha.startswith("|C170|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 2 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                
                dados_final = processar_registro_c170(dados, dt_ini_0000, filial, ind_oper, cod_part, num_doc, chv_nfe)
                if dados_final:
                    lote_c170.append(dados_final)
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

                # Antes de executar o executemany, adicione este código
                for reg in lote_atual:
                    # Garantir que o campo 'unid' tenha apenas um caractere
                    if reg[6] is not None:
                        unid_str = str(reg[6]).strip().upper()
                        if unid_str:
                            reg_lista = list(reg)
                            reg_lista[6] = unid_str[0]  # Pegar apenas o primeiro caractere
                            reg = tuple(reg_lista)

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
                    
                    total_processado += len(lote_atual)
                    print(f"[DEBUG] Processados {total_processado}/{len(lote_c170)} registros C170 ({(total_processado/len(lote_c170)*100):.1f}%)")
                    
                except Exception as e:
                    print(f"[DEBUG] Erro no lote {i//tamanho_lote+1}: {e}")
                    for j, registro in enumerate(lote_atual):
                        try:
                            print(f"[DEBUG] Tentando inserir registro {j} individualmente. Tamanho={len(registro)}")
                            
                            # Sanitizar diretamente o campo 'unid' que está na posição 7
                            print(f"[DEBUG] Campo unid original: '{registro[6]}'")
                            
                            # Forçar o campo 'unid' para ter apenas 1 caractere
                            if registro[6] is not None:
                                unid_str = str(registro[6]).strip().upper()
                                if not unid_str or len(unid_str) > 1:
                                    registro_mod = list(registro)
                                    registro_mod[6] = unid_str[0] if unid_str else "U"
                                    registro = tuple(registro_mod)
                                    print(f"[FORCE] unid alterada: '{unid_str}' → '{registro[6]}'")
                            
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
                            total_processado += 1
                        except Exception as ex:
                            print(f"[DEBUG] Erro no registro {i*tamanho_lote+j}: {ex}")
                            if "Data too long for column 'unid'" in str(ex):
                                print(f"[DEBUG] Problema com campo 'unid': '{registro[7]}'")
                
            print(f"[DEBUG] {total_processado} de {len(lote_c170)} registros |C170| salvos com sucesso.")
            
        except Exception as e:
            print(f"[DEBUG] ERRO ao inserir registros C170: {e}")
            mensagem_error(f"Erro ao salvar itens de notas fiscais: {e}")
            print(traceback.format_exc())

        print(f"[DEBUG] Registros |C170| processados em {time.time() - t_inicio:.2f} segundos")

        tempo_total = time.time() - inicio_processo
        print(f"[DEBUG] Todos os registros foram salvos no banco em {tempo_total:.2f} segundos")

        print(f"[DEBUG FINAL] Resumo do arquivo:")
        print(f" - Produtos únicos processados (|0200|): {len(lote_0200)}")
        print(f" - Itens de nota (|C170|): {len(lote_c170)} total, {total_processado} salvos")
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