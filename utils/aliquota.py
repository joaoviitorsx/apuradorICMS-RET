import math

def formatar_aliquota(aliquota):
    """
    Formata a alíquota em formato percentual. Se for um número, converte para float, 
    multiplica por 100 e adiciona o símbolo de porcentagem.
    """
    aliquota = aliquota.strip()
    if aliquota and aliquota[0].isdigit():
        try:
            aliquota_float = round(float(aliquota) * 100, 2)
            return f"{aliquota_float:.2f}%"
        except ValueError:
            return aliquota
    return aliquota
