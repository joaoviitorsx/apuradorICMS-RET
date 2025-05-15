def process_data(data):
    lines = data.strip().split('\n')
    output = []
    current_c100_id = None

    for line in lines:
        if line.startswith('|0000|') or line.startswith('|0150|') or line.startswith('|0200|') or line.startswith('|C100|') or line.startswith('|C170|'):
            parts = line.split('|')
            if parts[0] == 'C100':
                current_c100_id = parts[2]  
            elif parts[0] == 'C170':
                if current_c100_id is not None:
                    new_line = '|'.join(parts[:2] + [current_c100_id] + parts[2:])
                    output.append(new_line)
            else:
                output.append(line)

    return '\n'.join(output)

