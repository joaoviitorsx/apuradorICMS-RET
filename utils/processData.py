def process_data(data: str) -> str:
    linhas = data.strip().splitlines()
    resultado = []
    id_c100_atual = None

    for i, linha in enumerate(linhas):
        linha = linha.strip()

        if not linha.startswith('|'):
            continue

        partes = linha.split('|')
        if len(partes) < 2:
            print(f"[WARN] Linha malformada ignorada na linha {i}: '{linha}'")
            continue

        tipo_registro = partes[1]

        if tipo_registro == 'C100':
            id_c100_atual = partes[2] if len(partes) > 2 else None
            resultado.append(linha)

        elif tipo_registro == 'C170':
            if id_c100_atual:
                nova_linha = '|'.join(partes[:2] + [id_c100_atual] + partes[2:])
                resultado.append(nova_linha)
            else:
                print(f"[WARN] C170 sem C100 relacionado na linha {i}")
                resultado.append(linha)

        elif tipo_registro in ('0000', '0150', '0200'):
            resultado.append(linha)

        else:
            pass

    return '\n'.join(resultado)
