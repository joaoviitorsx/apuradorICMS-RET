from db.conexao import conectar_banco, fechar_banco

async def clonar_tabela_c170(empresa_id):
    print(f"[INÍCIO] Clonagem de dados para c170_clone (empresa_id={empresa_id})")

    conexao = conectar_banco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                id, periodo, reg, num_item, cod_item, descr_compl, qtd, unid,
                vl_item, vl_desc, cfop, cst, ncm, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe
            )
            SELECT 
                c.id, c.periodo, c.reg, c.num_item, c.cod_item, c.descr_compl, c.qtd, c.unid,
                c.vl_item, c.vl_desc, c.cfop, c.cst,
                t.ncm,
                c.id_c100, c.filial, c.ind_oper,
                c.cod_part, c.num_doc, c.chv_nfe
            FROM c170nova c
            JOIN cadastro_fornecedores f 
                ON c.cod_part = f.cod_part AND f.empresa_id = c.empresa_id
            LEFT JOIN cadastro_tributacao t 
                ON c.cod_item = t.codigo AND t.empresa_id = c.empresa_id
            WHERE f.decreto = 'Não'
            AND f.uf = 'CE'
            AND f.empresa_id = %s
            AND c.cfop IN ('1101', '1401', '1102', '1403', '1910', '1116')
            AND c.empresa_id = %s
        """, (empresa_id, empresa_id))

        conexao.commit()
        print(f"[OK] Dados inseridos na tabela c170_clone: {cursor.rowcount} registros.")

        cursor.execute("""
            UPDATE c170_clone c
            JOIN cadastro_tributacao t ON c.cod_item = t.codigo AND c.empresa_id = %s
            SET c.descr_compl = t.produto
            WHERE c.descr_compl IS NULL OR c.descr_compl = ''
        """, (empresa_id,))
        conexao.commit()
        print("[OK] descr_compl atualizado com nomes dos produtos da tributação.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha na clonagem: {e}")

    finally:
        cursor.close()
        fechar_banco(conexao)
        print("[FIM] Processo de clonagem encerrado.")



#precisa adicionar aliquota e resultado na tabela c170_clone que seria a clonagem da tabela c170nova,
#fazendo um cadastro de produtos com base nos produtos na tabela 0200
#precisamos verificar os cod_item que serao dos dos produtos no 0200 e fazendo assim uma verificação se o produto ja existe no banco
#cadastro_tributacao, se nao existir vamos inseri-lo, no entanto, não vai estar presente as aliquotas
# e assim vai abrir o popup para o usuario preencher as aliquotas, como pode ter varios produtos igual, mas com chaves diferentes,
# so vamos filtrar para ser chamado o produto uma vez e assim o usuario preenche as aliquotas de todos os produtos iguais