def process_data(data: str) -> str:
    lines = data.strip().split('\n')
    output = []
    current_c100_id = None

    for line in lines:
        if not line.startswith('|'):
            continue

        partes = line.split('|')
        if len(partes) < 2:
            continue

        registro = partes[1]

        if registro == 'C100':
            current_c100_id = partes[2] if len(partes) > 2 else None
            output.append(line)

        elif registro == 'C170':
            if current_c100_id:
                nova_linha = '|'.join(partes[:2] + [current_c100_id] + partes[2:])
                output.append(nova_linha)
            else:
                output.append(line)

        elif registro in ('0000', '0150', '0200'):
            output.append(line)

    return '\n'.join(output)
