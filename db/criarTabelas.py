from mysql.connector import Error
from db.conexao import conectar_banco, fechar_banco

def criar_tabelas_principais():
    conexao = conectar_banco()
    if not conexao:
        return

    try:
        cursor = conexao.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `0000` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                reg VARCHAR(10),
                cod_ver VARCHAR(10),
                cod_fin VARCHAR(10),
                dt_ini VARCHAR(10),
                dt_fin VARCHAR(10),
                nome VARCHAR(100),
                cnpj VARCHAR(20),
                cpf VARCHAR(20),
                uf VARCHAR(5),
                ie VARCHAR(20),
                cod_num VARCHAR(20),
                im VARCHAR(20),
                suframa VARCHAR(20),
                ind_perfil VARCHAR(10),
                ind_ativ VARCHAR(10),
                filial VARCHAR(10),
                periodo VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `0150` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                reg VARCHAR(10),
                cod_part VARCHAR(60),
                nome VARCHAR(100),
                cod_pais VARCHAR(10),
                cnpj VARCHAR(20),
                cpf VARCHAR(20),
                ie VARCHAR(20),
                cod_mun VARCHAR(20),
                suframa VARCHAR(20),
                ende VARCHAR(100),
                num VARCHAR(20),
                compl VARCHAR(20),
                bairro VARCHAR(50),
                cod_uf VARCHAR(10),
                uf VARCHAR(5),
                pj_pf VARCHAR(5),
                periodo VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `0200` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                reg VARCHAR(10),
                cod_item VARCHAR(60),
                descr_item VARCHAR(255),
                cod_barra VARCHAR(60),
                cod_ant_item VARCHAR(60),
                unid_inv VARCHAR(10),
                tipo_item VARCHAR(10),
                cod_ncm VARCHAR(20),
                ex_ipi VARCHAR(10),
                cod_gen VARCHAR(10),
                cod_list VARCHAR(10),
                aliq_icms VARCHAR(10),
                cest VARCHAR(10),
                periodo VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS c100 (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                periodo VARCHAR(10),
                reg VARCHAR(10),
                ind_oper VARCHAR(5),
                ind_emit VARCHAR(5),
                cod_part VARCHAR(60),
                cod_mod VARCHAR(10),
                cod_sit VARCHAR(10),
                ser VARCHAR(10),
                num_doc VARCHAR(20),
                chv_nfe VARCHAR(60),
                dt_doc VARCHAR(10),
                dt_e_s VARCHAR(10),
                vl_doc VARCHAR(20),
                ind_pgto VARCHAR(5),
                vl_desc VARCHAR(20),
                vl_abat_nt VARCHAR(20),
                vl_merc VARCHAR(20),
                ind_frt VARCHAR(5),
                vl_frt VARCHAR(20),
                vl_seg VARCHAR(20),
                vl_out_da VARCHAR(20),
                vl_bc_icms VARCHAR(20),
                vl_icms VARCHAR(20),
                vl_bc_icms_st VARCHAR(20),
                vl_icms_st VARCHAR(20),
                vl_ipi VARCHAR(20),
                vl_pis VARCHAR(20),
                vl_cofins VARCHAR(20),
                vl_pis_st VARCHAR(20),
                vl_cofins_st VARCHAR(20),
                filial VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS c170 (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                periodo VARCHAR(10),
                reg VARCHAR(10),
                num_item VARCHAR(10),
                cod_item VARCHAR(60),
                descr_compl VARCHAR(255),
                qtd VARCHAR(20),
                unid VARCHAR(10),
                vl_item VARCHAR(20),
                vl_desc VARCHAR(20),
                ind_mov VARCHAR(5),
                cst_icms VARCHAR(10),
                cfop VARCHAR(10),
                cod_nat VARCHAR(10),
                vl_bc_icms VARCHAR(20),
                aliq_icms VARCHAR(10),
                vl_icms VARCHAR(20),
                vl_bc_icms_st VARCHAR(20),
                aliq_st VARCHAR(10),
                vl_icms_st VARCHAR(20),
                ind_apur VARCHAR(5),
                cst_ipi VARCHAR(10),
                cod_enq VARCHAR(10),
                vl_bc_ipi VARCHAR(20),
                aliq_ipi VARCHAR(10),
                vl_ipi VARCHAR(20),
                cst_pis VARCHAR(10),
                vl_bc_pis VARCHAR(20),
                aliq_pis VARCHAR(10),
                quant_bc_pis VARCHAR(20),
                aliq_pis_reais VARCHAR(20),
                vl_pis VARCHAR(20),
                cst_cofins VARCHAR(10),
                vl_bc_cofins VARCHAR(20),
                aliq_cofins VARCHAR(10),
                quant_bc_cofins VARCHAR(20),
                aliq_cofins_reais VARCHAR(20),
                vl_cofins VARCHAR(20),
                cod_cta VARCHAR(255),
                vl_abat_nt VARCHAR(20),
                id_c100 INT,
                filial VARCHAR(10),
                ind_oper VARCHAR(5),
                cod_part VARCHAR(60),
                num_doc VARCHAR(20),
                chv_nfe VARCHAR(60),
                ncm VARCHAR(44) DEFAULT '',
                mercado VARCHAR(15) DEFAULT '',
                aliquota VARCHAR(10) DEFAULT '',
                resultado VARCHAR(20),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cadastro_tributacao (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                codigo VARCHAR(60),
                produto VARCHAR(255),
                ncm VARCHAR(20),
                aliquota VARCHAR(10),
                aliquota_antiga VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cadastro_fornecedores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                cod_part VARCHAR(60),
                nome VARCHAR(100),
                cnpj VARCHAR(20),
                uf VARCHAR(5),
                cnae VARCHAR(20),
                decreto VARCHAR(10),
                simples VARCHAR(10),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS c170_clone (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                periodo VARCHAR(10),
                reg VARCHAR(10),
                num_item VARCHAR(10),
                cod_item VARCHAR(60),
                descr_compl VARCHAR(255),
                qtd VARCHAR(20),
                unid VARCHAR(10),
                vl_item VARCHAR(20),
                vl_desc VARCHAR(20),
                cfop VARCHAR(10),
                cst varchar(3),
                ncm varchar(40),      
                id_c100 INT,
                filial VARCHAR(10),
                ind_oper VARCHAR(5),
                cod_part VARCHAR(60),
                num_doc VARCHAR(20),
                chv_nfe VARCHAR(60),
                aliquota VARCHAR(10),
                resultado VARCHAR(20),
                chavefinal VARCHAR(100),
                nome VARCHAR(255),
                cnpj VARCHAR(20),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS c170nova (
                id INT AUTO_INCREMENT PRIMARY KEY,
                empresa_id INT,
                cod_item VARCHAR(60),
                periodo VARCHAR(10),
                reg VARCHAR(10),
                num_item VARCHAR(10),
                descr_compl VARCHAR(255),
                cod_ncm VARCHAR(40),
                qtd VARCHAR(20),
                unid VARCHAR(10),
                vl_item VARCHAR(20),
                vl_desc VARCHAR(20),
                cfop VARCHAR(10),
                id_c100 VARCHAR(10),
                filial VARCHAR(10),
                ind_oper VARCHAR(5),
                cod_part VARCHAR(60),
                num_doc VARCHAR(20),
                chv_nfe VARCHAR(60),
                INDEX idx_empresa (empresa_id)
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170_cod_item_empresa ON c170 (cod_item, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tributacao_codigo_empresa ON cadastro_tributacao (codigo, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170clone_cod_item_empresa ON c170_clone (cod_item, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_0200_cod_item_empresa ON `0200` (cod_item, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170nova_cod_item_empresa ON c170nova (cod_item, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170clone_codpart_empresa_periodo ON c170_clone (cod_part, empresa_id, periodo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fornecedor_codpart_empresa ON cadastro_fornecedores (cod_part, empresa_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170clone_empresa_periodo ON c170_clone (empresa_id, periodo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tributacao_empresa_aliquota ON cadastro_tributacao (empresa_id, aliquota)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_c170clone_aliquota_empresa ON c170_clone (empresa_id, aliquota)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fornecedor_empresa_simples ON cadastro_fornecedores (empresa_id, simples)")

        cursor.execute("INSERT INTO cadastro_fornecedores(empresa_id, cod_part, nome, cnpj, uf, cnae, decreto, simples) VALUES (1, '55', null , null , 'CE', null , 'Não', null);")
        conexao.commit()
        print("[DB] Todas as tabelas criadas ou atualizadas com sucesso.")

    except Error as e:
        print(f"[ERRO] Falha ao criar tabelas: {e}")
    finally:
        fechar_banco(conexao)
