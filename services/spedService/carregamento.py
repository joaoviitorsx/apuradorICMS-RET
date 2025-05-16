import asyncio
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectar_banco, fechar_banco
from utils.processData import process_data
from utils.mensagem import mensagem_error, mensagem_aviso, mensagem_sucesso
from utils.siglas import obter_sigla_estado
import threading

def processar_sped_thread(nome_banco, progress_bar, label_arquivo):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(processar_sped(nome_banco, progress_bar, label_arquivo))
    loop.close()

def iniciar_processamento_sped(nome_banco, progress_bar, label_arquivo):
    thread = threading.Thread(target=processar_sped_thread, args=(nome_banco, progress_bar, label_arquivo))
    thread.start()

async def processar_sped(nome_banco, progress_bar, label_arquivo):
    progress_bar.setValue(0)
    conexao = conectar_banco(nome_banco)
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        return

    cursor = conexao.cursor()
    cursor.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not cursor.fetchone():
        mensagem_error("Tributação não encontrada. Envie primeiramente a tributação.")
        return
    cursor.close()

    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Sped", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.")
        return

    total = len(caminhos)
    try:
        cursor = conexao.cursor()
        for i, caminho in enumerate(caminhos):
            try:
                with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                    conteudo = arquivo.read()
                    progress_bar.setValue(2)
                    conteudo_processado = process_data(conteudo)

                    label_arquivo.setText(f'Processando arquivo {i+1}/{total}')
                    await salvar_no_banco(conteudo_processado, cursor, nome_banco, progress_bar)

            except Exception as e:
                mensagem_error(f"Erro ao processar o arquivo {caminho}: {e}")
                return

        mensagem_sucesso("Arquivos processados com sucesso.")
    except Exception as e:
        mensagem_error(f"Erro inesperado: {e}")
    finally:
        cursor.close()
        fechar_banco(conexao)


async def salvar_no_banco(conteudo, cursor, nome_banco, progress_bar):
    progress_bar.setValue(5)
    linhas = conteudo.split('\n')
    id_c100_atual = None
    dt_ini_0000 = None
    filial = None
    try:
        cursor.execute("START TRANSACTION")

        for linha in linhas:
            if linha.startswith('|0000|'):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                if len(dados) < 15:
                    dados.extend([None] * (15 - len(dados)))
                dt_ini_0000 = dados[3]
                cnpj_0000 = dados[6]
                filial = cnpj_0000[8:12] if cnpj_0000 else '0000'
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados.append(filial)
                dados.append(periodo)
                cursor.execute('''
                    INSERT INTO `0000` (
                        reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
                        ind_perfil, ind_ativ, filial, periodo
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', dados)

            elif linha.startswith('|0150|'):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                if len(dados) < 13:
                    dados.extend([None] * (13 - len(dados)))
                num = dados[7]
                cod_uf = num[:2] if num else None
                uf = obter_sigla_estado(cod_uf)
                cnpj = dados[4]
                pj_pf = "PF" if cnpj is None else "PJ"
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados.extend([cod_uf, uf, pj_pf, periodo])
                cursor.execute('''
                    INSERT IGNORE INTO `0150` (
                        reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
                        cod_uf, uf, pj_pf, periodo
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', dados)

            elif linha.startswith('|0200|'):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 1 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                if len(dados) < 13:
                    dados.extend([None] * (13 - len(dados)))
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados.append(periodo)
                cursor.execute('''
                    INSERT IGNORE INTO `0200` (
                        reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
                        ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', dados)

            elif linha.startswith('|C100|'):
                dados = linha.split('|')[1:-1]
                dados = [d.strip() if d.strip() else None for d in dados]
                if len(dados) < 29:
                    dados.extend([None] * (29 - len(dados)))
                periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                dados_final = [periodo] + dados + [filial]
                cursor.execute('''
                    INSERT INTO c100 (
                        periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                        dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt,
                        vl_seg, vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis,
                        vl_cofins, vl_pis_st, vl_cofins_st, filial
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', dados_final)
                id_c100_atual = cursor.lastrowid
                cursor.execute('SELECT ind_oper, cod_part, num_doc, chv_nfe FROM c100 WHERE id = %s', (id_c100_atual,))
                ind_oper, cod_part, num_doc, chv_nfe = cursor.fetchone()

            elif linha.startswith('|C170|'):
                dados = linha.split('|')[1:-1]
                dados = [d.strip().lstrip('0') if i == 2 and d.strip() else d.strip() if d.strip() else None for i, d in enumerate(dados)]
                if len(dados) < 38:
                    dados.extend([None] * (38 - len(dados)))
                if id_c100_atual is not None:
                    periodo = f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
                    dados_final = [periodo] + dados + [id_c100_atual, filial, ind_oper, cod_part, num_doc, chv_nfe]
                    cursor.execute('''
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
                    ''', dados_final)

        cursor.execute("COMMIT")
        progress_bar.setValue(30)
    except Exception as e:
        cursor.execute("ROLLBACK")
        mensagem_error(f"Erro ao salvar no banco: {e}")
