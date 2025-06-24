cfops = [1101, 1401, 1102, 1403, 1405, 1910, 2102, 2403, 2101, 2102, 2401, 2403, 2910, 2116]
def pegar_cfop(cfop):
    return 1 if cfop in cfops else 0