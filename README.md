# Apurador de ICMS

Um sistema desktop completo para **apuração do ICMS (Imposto sobre Circulação de Mercadorias e Serviços)**, desenvolvido especialmente para empresas do setor **varejista**. Esta aplicação automatiza o processamento de arquivos fiscais SPED, realiza o controle de alíquotas e gera relatórios com eficiência e precisão.

## Funcionalidades

- **Processamento de SPED**  
  Importação e análise de arquivos no formato SPED Fiscal, com leitura dos blocos 0000, 0150, 0200, C100 e C170.

- **Gerenciamento de Empresas**  
  Cadastro e seleção de múltiplas empresas, com banco de dados centralizado.

- **Controle de Tributação**  
  Gestão de alíquotas de ICMS por produto e NCM, com possibilidade de preenchimento manual ou via planilha.

- **Integração com API de Fornecedores**  
  Consulta automática de dados cadastrais de fornecedores via API pública por CNPJ.

- **Cálculos Automatizados**  
  Cálculo automático do ICMS a partir dos dados extraídos e das alíquotas definidas.

- **Exportação de Dados**  
  Geração de relatórios e planilhas Excel (.xlsx).

## Foram Utilizados

- **Linguagem:** Python  
- **Banco de Dados:** MySQL  
- **Interface Gráfica:** PySide6

## Objetivo

Simplificar e automatizar o processo de apuração de ICMS, reduzindo erros manuais e otimizando o tempo gasto com tarefas fiscais no varejo.


