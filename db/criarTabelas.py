def criar_tabelas(cursor):
    tabelas_sql = [
        '''
        CREATE TABLE IF NOT EXISTS `0000` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reg VARCHAR(4), cod_ver VARCHAR(10), cod_fin VARCHAR(10), dt_ini VARCHAR(10), dt_fin VARCHAR(10),
            nome VARCHAR(100), cnpj VARCHAR(20), cpf VARCHAR(20), uf VARCHAR(5), ie VARCHAR(20),
            cod_num VARCHAR(20), im VARCHAR(20), suframa VARCHAR(20), ind_perfil VARCHAR(5),
            ind_ativ VARCHAR(5), filial VARCHAR(10), periodo VARCHAR(10)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS `0150` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reg VARCHAR(4), cod_part VARCHAR(20), nome VARCHAR(100), cod_pais VARCHAR(10), cnpj VARCHAR(20),
            cpf VARCHAR(20), ie VARCHAR(20), cod_mun VARCHAR(10), suframa VARCHAR(20), ende VARCHAR(100),
            num VARCHAR(10), compl VARCHAR(20), bairro VARCHAR(50), cod_uf VARCHAR(5), uf VARCHAR(5),
            pj_pf VARCHAR(5), periodo VARCHAR(10)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS `0200` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reg VARCHAR(4), cod_item VARCHAR(40), descr_item VARCHAR(255), cod_barra VARCHAR(50),
            cod_ant_item VARCHAR(50), unid_inv VARCHAR(10), tipo_item VARCHAR(5), cod_ncm VARCHAR(20),
            ex_ipi VARCHAR(10), cod_gen VARCHAR(10), cod_list VARCHAR(10), aliq_icms VARCHAR(10), periodo VARCHAR(10)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS c100 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            periodo VARCHAR(10), reg VARCHAR(4), ind_oper VARCHAR(2), ind_emit VARCHAR(2), cod_part VARCHAR(20),
            cod_mod VARCHAR(10), cod_sit VARCHAR(10), ser VARCHAR(10), num_doc VARCHAR(20), chv_nfe VARCHAR(50),
            dt_doc VARCHAR(10), dt_e_s VARCHAR(10), vl_doc VARCHAR(20), ind_pgto VARCHAR(2), vl_desc VARCHAR(20),
            vl_abat_nt VARCHAR(20), vl_merc VARCHAR(20), ind_frt VARCHAR(5), vl_frt VARCHAR(20), vl_seg VARCHAR(20),
            vl_out_da VARCHAR(20), vl_bc_icms VARCHAR(20), vl_icms VARCHAR(20), vl_bc_icms_st VARCHAR(20),
            vl_icms_st VARCHAR(20), vl_ipi VARCHAR(20), vl_pis VARCHAR(20), vl_cofins VARCHAR(20),
            vl_pis_st VARCHAR(20), vl_cofins_st VARCHAR(20), filial VARCHAR(10)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS c170 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            periodo VARCHAR(10), reg VARCHAR(4), num_item VARCHAR(10), cod_item VARCHAR(40),
            descr_compl VARCHAR(255), qtd VARCHAR(20), unid VARCHAR(10), vl_item VARCHAR(20), vl_desc VARCHAR(20),
            ind_mov VARCHAR(5), cst_icms VARCHAR(10), cfop VARCHAR(10), cod_nat VARCHAR(20),
            vl_bc_icms VARCHAR(20), aliq_icms VARCHAR(10), vl_icms VARCHAR(20), vl_bc_icms_st VARCHAR(20),
            aliq_st VARCHAR(10), vl_icms_st VARCHAR(20), ind_apur VARCHAR(5), cst_ipi VARCHAR(10), cod_enq VARCHAR(10),
            vl_bc_ipi VARCHAR(20), aliq_ipi VARCHAR(10), vl_ipi VARCHAR(20), cst_pis VARCHAR(10),
            vl_bc_pis VARCHAR(20), aliq_pis VARCHAR(10), quant_bc_pis VARCHAR(20), aliq_pis_reais VARCHAR(20),
            vl_pis VARCHAR(20), cst_cofins VARCHAR(10), vl_bc_cofins VARCHAR(20), aliq_cofins VARCHAR(10),
            quant_bc_cofins VARCHAR(20), aliq_cofins_reais VARCHAR(20), vl_cofins VARCHAR(20), cod_cta VARCHAR(30),
            vl_abat_nt VARCHAR(20), id_c100 INT, filial VARCHAR(10), ind_oper VARCHAR(2), cod_part VARCHAR(20),
            num_doc VARCHAR(20), chv_nfe VARCHAR(50)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS cadastro_tributacao (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo VARCHAR(20) UNIQUE,
            produto VARCHAR(100),
            ncm VARCHAR(20),
            aliquota VARCHAR(20),
            aliquota_antiga VARCHAR(20),
            data_inicial DATETIME DEFAULT CURRENT_TIMESTAMP
        )''',

        '''
        CREATE TABLE IF NOT EXISTS cadastro_fornecedores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cod_part VARCHAR(20),
            nome VARCHAR(100),
            cnpj VARCHAR(20),
            uf VARCHAR(5),
            cnae VARCHAR(20),
            decreto VARCHAR(20),
            simples VARCHAR(10)
        )''',

        '''
        CREATE TABLE IF NOT EXISTS c170_clone (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cod_item VARCHAR(40),
            periodo VARCHAR(10),
            reg VARCHAR(20),
            num_item VARCHAR(50),
            descr_compl VARCHAR(255),
            cod_ncm VARCHAR(40),
            qtd VARCHAR(100),
            unid VARCHAR(30),
            vl_item VARCHAR(100),
            vl_desc DECIMAL(10,2),
            cfop VARCHAR(10),
            id_c100 VARCHAR(20),
            filial VARCHAR(10),
            ind_oper VARCHAR(1),
            cod_part VARCHAR(15),
            num_doc VARCHAR(50),
            chv_nfe VARCHAR(45),
            aliquota VARCHAR(20),
            resultado DECIMAL(12,2),
            chavefinal VARCHAR(100)
        )'''
    ]

    for comando in tabelas_sql:
        cursor.execute(comando)