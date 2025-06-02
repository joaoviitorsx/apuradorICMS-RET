from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170nova(empresa_id):
    print(f"[INÍCIO] Clonagem completa da c170nova para c170_clone (empresa_id={empresa_id})")
    
    conexao = conectar_banco()
    if not conexao:
        print("[ERRO] Falha na conexão com o banco.")
        return

    cursor = conexao.cursor()

    try:
        cursor.execute("DELETE FROM c170_clone WHERE empresa_id = %s", (empresa_id,))
        conexao.commit()

        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                id, empresa_id, cod_item, periodo, reg, num_item, descr_compl, ncm, qtd, unid,
                vl_item, vl_desc, cst, cfop, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado
            )
            SELECT 
                c.id, c.empresa_id, c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl, c.cod_ncm, c.qtd, c.unid,
                c.vl_item, c.vl_desc, c.cst, c.cfop, c.id_c100, c.filial,
                c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe, '' AS aliquota, '' AS resultado
            FROM c170nova c
            WHERE c.empresa_id = %s
        """, (empresa_id,))

        conexao.commit()
        print(f"[OK] {cursor.rowcount} registros clonados para c170_clone.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha durante a clonagem da c170nova: {e}")

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Clonagem finalizada.")





#precisa adicionar aliquota e resultado na tabela c170_clone que seria a clonagem da tabela c170nova,
#fazendo um cadastro de produtos com base nos produtos na tabela 0200
#precisamos verificar os cod_item que serao dos dos produtos no 0200 e fazendo assim uma verificação se o produto ja existe no banco
#cadastro_tributacao, se nao existir vamos inseri-lo, no entanto, não vai estar presente as aliquotas
# e assim vai abrir o popup para o usuario preencher as aliquotas, como pode ter varios produtos igual, mas com chaves diferentes,
# so vamos filtrar para ser chamado o produto uma vez e assim o usuario preenche as aliquotas de todos os produtos iguais

#quando os dados sao preenchidos na c170nova ele coleta dados da c100 e 0200 para serem corretamente preenchidos,
#portanto o proximo passo seria a clonagem de c170nova para inserimos as aliquotas e resultados deste produtos que passaram pelo o filtro
# no entanto, apos fazer a clonagem e antes de passarem pela atualização das aliquotas ou pelo o codigo em atualizacoes.py
# precisamos aplicar e ve os produtos de 0200 que nao estao presente em cadastro_Tributacao e inserir eles, pois verificando no banco
# indentifiquei que na tabela c170nova temos itens que nao estao presentes e entao para finalizarmos o processamento precisamos
# alimentar a base de cadastro_tributacao com os itens que estao presentes na c170nova