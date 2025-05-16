import re
from utils.siglas import obter_sigla_estado
from utils.mensagem import mensagem_error

async def salvar_no_banco_em_lote(conteudo, cursor, nome_banco):
    progress_status = ["|0000|", "|0150|", "|0200|", "|C100|", "|C170|"]
    linhas = conteudo.split('\n')

    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    id_c100_atual = None
    dt_ini_0000 = None
    filial = None
    ind_oper = cod_part = num_doc = chv_nfe = None

    try:
        for linha in linhas:
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

            elif linha.startswith("|0200|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 1 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                dados += [None] * (13 - len(dados)) if len(dados) < 13 else []
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados.append(periodo)
                lote_0200.append(dados)

            elif linha.startswith("|C100|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                dados += [None] * (29 - len(dados)) if len(dados) < 29 else []
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados_final = [periodo] + dados + [filial]
                lote_c100.append(dados_final)
                ind_oper, cod_part, num_doc, chv_nfe = dados[1], dados[4], dados[7], dados[9]

            elif linha.startswith("|C170|"):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 2 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                dados += [None] * (38 - len(dados)) if len(dados) < 38 else []
                if dt_ini_0000:
                    periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}'
                    dados_final = [periodo] + dados + [None, filial, ind_oper, cod_part, num_doc, chv_nfe]
                    lote_c170.append(dados_final)

        # Inserções em lote
        cursor.executemany('''
            INSERT INTO `0000` (
                reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
                ind_perfil, ind_ativ, filial, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0000)

        cursor.executemany('''
            INSERT IGNORE INTO `0150` (
                reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
                cod_uf, uf, pj_pf, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0150)

        cursor.executemany('''
            INSERT IGNORE INTO `0200` (
                reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
                ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_0200)

        cursor.executemany('''
            INSERT INTO c100 (
                periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt,
                vl_seg, vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis,
                vl_cofins, vl_pis_st, vl_cofins_st, filial
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', lote_c100)

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

    except Exception as e:
        mensagem_error(f"Erro ao salvar dados em lote: {e}")
