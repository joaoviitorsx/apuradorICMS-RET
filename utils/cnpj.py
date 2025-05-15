import aiohttp
import asyncio
import re
from functools import wraps
from time import time

# Função para criar cache
def create_cache(ttl=3600):  # Tempo de vida padrão de 1 hora
    cache_dict = {}
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = str((args, frozenset(kwargs.items())))
            now = time()
            if key in cache_dict:
                cached_time, result = cache_dict[key]
                if now - cached_time <= ttl:
                    return result
            result = await func(*args, **kwargs)
            cache_dict[key] = (time(), result)
            return result
        return wrapper
    return decorator

# Função otimizada para remover caracteres não numéricos
def remover_caracteres_nao_numericos(valor):
    return re.sub(r'\D', '', valor)

lista_cnaes = [
    '4623108', '4623199', '4632001', '4637107', '4639701', '4639702', 
    '4646002', '4647801', '4649408', '4635499', '4637102', '4637199', 
    '4644301', '4632003', '4691500', '4693100', '3240099', '4649499', 
    '8020000', '4711301', '4711302', '4712100', '4721103', '4721104', 
    '4729699', '4761003', '4789005', '4771701', '4771702', '4771703', 
    '4772500', '4763601'
]

# Dicionário para armazenar resultados em cache
cache_resultados = {}

# Função otimizada para buscar informações do decreto e CNAE
@create_cache()
async def buscar_informacoes(cnpj):
    url = f'https://minhareceita.org/{cnpj}'
    timeout = aiohttp.ClientTimeout(total=10)  # Timeout de 10 segundos
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url) as resposta:
                if resposta.status == 200:
                    dados = await resposta.json()
                    cnae_codigo = str(dados.get('cnae_fiscal', []))
                    existe_no_lista = "Sim" if cnae_codigo in lista_cnaes else "Não"
                    uf = dados.get('uf', '')
                    pegar_simples = dados.get('opcao_pelo_simples', '')
                    simples = "Sim" if pegar_simples == True else "Não"
                    return cnae_codigo, existe_no_lista, uf, simples
                else:
                    print(f"Erro na requisição: Status {resposta.status}")
                    return None, None, None, None
        except asyncio.TimeoutError:
            print(f"Timeout: A requisição para o CNPJ {cnpj} demorou muito e foi cancelada.")
            return None, None, None, None
        except Exception as e:
            print(f"Ocorreu um erro ao buscar informações para o CNPJ {cnpj}: {e}")
            return None, None

# Função para processar uma lista de CNPJs
async def processar_cnpjs(cnpjs):
    resultados = {}
    tasks = []
    for cnpj in cnpjs:
        if cnpj in cache_resultados:
            # Se o CNPJ já estiver no cache, usa o resultado armazenado
            resultados[cnpj] = cache_resultados[cnpj]
        else:
            # Se o CNPJ não estiver no cache, cria uma tarefa para a requisição assíncrona
            tasks.append(_processar_cnpj(cnpj, resultados))
    
    await asyncio.gather(*tasks)
    return resultados

# Função auxiliar para processar cada CNPJ individualmente
async def _processar_cnpj(cnpj, resultados):
    cnae_codigo, existe_no_lista, uf, simples = await buscar_informacoes(cnpj)
    resultados[cnpj] = (cnae_codigo, existe_no_lista, uf, simples)
    cache_resultados[cnpj] = (cnae_codigo, existe_no_lista, uf, simples)

def validar_cnpj(cnpj):
    """
    Valida um CNPJ.
    
    Args:
        cnpj: String contendo o CNPJ a ser validado
        
    Returns:
        bool: True se o CNPJ for válido, False caso contrário
    """
    # Remove caracteres não numéricos
    cnpj = ''.join(filter(str.isdigit, cnpj))
    
    # Verifica se tem 14 dígitos
    if len(cnpj) != 14:
        return False
        
    # Verifica se todos os dígitos são iguais
    if cnpj == cnpj[0] * 14:
        return False
        
    # Validação do primeiro dígito verificador
    soma = 0
    peso = 5
    for i in range(12):
        soma += int(cnpj[i]) * peso
        peso = peso - 1 if peso > 2 else 9
    
    digito1 = 11 - (soma % 11)
    if digito1 > 9:
        digito1 = 0
        
    if int(cnpj[12]) != digito1:
        return False
        
    # Validação do segundo dígito verificador
    soma = 0
    peso = 6
    for i in range(13):
        soma += int(cnpj[i]) * peso
        peso = peso - 1 if peso > 2 else 9
        
    digito2 = 11 - (soma % 11)
    if digito2 > 9:
        digito2 = 0
        
    if int(cnpj[13]) != digito2:
        return False
        
    return True

def formatar_cnpj(cnpj):
    """
    Formata um CNPJ no padrão XX.XXX.XXX/XXXX-XX.
    
    Args:
        cnpj: String contendo o CNPJ a ser formatado
        
    Returns:
        str: CNPJ formatado
    """
    cnpj = ''.join(filter(str.isdigit, cnpj))
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


