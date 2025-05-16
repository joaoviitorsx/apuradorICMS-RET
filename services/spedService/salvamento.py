import re
from utils.siglas import obter_sigla_estado
from utils.mensagem import mensagem_error
import time

async def salvar_no_banco_em_lote(conteudo, cursor, nome_banco):
    progress_status = ["|0000|", "|0150|", "|0200|", "|C100|", "|C170|"]
    linhas = conteudo.split('\n')
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas para salvar no banco")
    
    # Contadores para estatísticas
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
                dados += [None] * (38 - len(dados)) if len(dados) < 38 else []
                if dt_ini_0000:
                    periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}'
                    dados_final = [periodo] + dados + [None, filial, ind_oper, cod_part, num_doc, chv_nfe]
                    lote_c170.append(dados_final)
                    contadores["C170"] += 1
        
        t_fim = time.time()
        print(f"[DEBUG] Dados extraídos em {t_fim - t_inicio:.2f} segundos")
        print(f"[DEBUG] Registros extraídos: 0000: {contadores['0000']}, 0150: {contadores['0150']}, 0200: {contadores['0200']}, C100: {contadores['C100']}, C170: {contadores['C170']}")
        
        # Insere os registros no banco
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
        cursor.executemany('''
            INSERT INTO c170 (
                periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s)
        ''', lote_c170)
        print(f"[DEBUG] Registros |C170| salvos em {time.time() - t_inicio:.2f} segundos")

        tempo_total = time.time() - inicio_processo
        print(f"[DEBUG] Todos os registros foram salvos no banco em {tempo_total:.2f} segundos")

    except Exception as e:
        print(f"[DEBUG] ERRO ao salvar dados no banco: {type(e).__name__}: {e}")
        import traceback
        print(f"[DEBUG] Traceback: {traceback.format_exc()}")
        mensagem_error(f"Erro ao salvar dados em lote: {e}")