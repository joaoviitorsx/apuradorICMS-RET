import aiohttp
import asyncio
import re
from functools import wraps
from time import time


def create_cache(ttl=3600):
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

cache_resultados = {}
semaforo = asyncio.Semaphore(10)


@create_cache()
async def buscar_informacoes(cnpj, tentativas=3):
    url = f'https://minhareceita.org/{cnpj}'
    timeout = aiohttp.ClientTimeout(total=10)

    async with semaforo:
        for tentativa in range(1, tentativas + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as resposta:
                        if resposta.status == 200:
                            dados = await resposta.json()
                            cnae_codigo = str(dados.get('cnae_fiscal', []))
                            existe_na_lista = "Sim" if cnae_codigo in lista_cnaes else "Não"
                            uf = dados.get('uf', '')
                            pegar_simples = dados.get('opcao_pelo_simples', '')
                            simples = "Sim" if pegar_simples is True else "Não"
                            return cnae_codigo, existe_na_lista, uf, simples
                        else:
                            print(f"[{tentativa}] Status {resposta.status} para CNPJ {cnpj}")
            except asyncio.TimeoutError:
                print(f"[{tentativa}] Timeout no CNPJ {cnpj}")
            except Exception as e:
                print(f"[{tentativa}] Erro inesperado no CNPJ {cnpj}: {e}")
        return None, None, None, None


async def _processar_cnpj(cnpj, resultados):
    cnpj_limpo = remover_caracteres_nao_numericos(cnpj)
    cnae_codigo, existe_na_lista, uf, simples = await buscar_informacoes(cnpj_limpo)

    if all([cnae_codigo, existe_na_lista, uf]):
        resultados[cnpj] = (cnae_codigo, existe_na_lista, uf, simples)
        cache_resultados[cnpj] = resultados[cnpj]
    else:
        print(f"[ERRO] Não foi possível obter dados válidos para o CNPJ {cnpj}. Ignorado.")


async def processar_cnpjs(cnpjs):
    resultados = {}
    tasks = []

    for cnpj in cnpjs:
        if cnpj in cache_resultados:
            resultado = cache_resultados[cnpj]
            if all(resultado):
                resultados[cnpj] = resultado
            else:
                print(f"[CACHE ERRO] Resultado inválido em cache para {cnpj}. Ignorando.")
        else:
            tasks.append(_processar_cnpj(cnpj, resultados))

    if tasks:
        await asyncio.gather(*tasks)

    return resultados


def validar_cnpj(cnpj):
    cnpj = ''.join(filter(str.isdigit, cnpj))

    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    soma = sum(int(cnpj[i]) * (5 - i if i < 4 else 13 - i) for i in range(12))
    digito1 = 11 - (soma % 11)
    digito1 = 0 if digito1 > 9 else digito1

    if int(cnpj[12]) != digito1:
        return False

    soma = sum(int(cnpj[i]) * (6 - i if i < 5 else 14 - i) for i in range(13))
    digito2 = 11 - (soma % 11)
    digito2 = 0 if digito2 > 9 else digito2

    return int(cnpj[13]) == digito2


def formatar_cnpj(cnpj):
    cnpj = ''.join(filter(str.isdigit, cnpj))
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"