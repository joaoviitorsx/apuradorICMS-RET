import math

def eh_aliquota_numerica(valor):
    try:
        valor = str(valor).strip().upper()
        if valor in ['ISENTO', 'ST', 'PAUTA', 'N/A', '']:
            return False
        float(valor.replace('%', '').replace(',', '.'))
        return True
    except:
        return False

def formatar_aliquota(valor):
    try:
        if valor is None:
            return "0.00%"

        valor = str(valor).strip().replace(',', '.').replace(' ', '').upper()

        if valor in ['ISENTO', 'ST', 'PAUTA', '', 'N/A']:
            return valor

        if '%' in valor:
            numero = float(valor.replace('%', ''))
            return f"{numero:.2f}%"
        else:
            numero = float(valor)
            if numero >= 1:
                return f"{numero:.2f}%"
            else:
                return f"{(numero * 100):.2f}%"
    except:
        return "0.00%"
