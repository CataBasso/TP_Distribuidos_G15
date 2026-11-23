#Comprime un conjunto de rangos
def to_ranges(ids) -> list:
    out = []
    for i in sorted(ids):
        if out and i == out[-1][1] + 1:
            out[-1][1] = i
        else:
            out.append([i, i])
    return out

#Inversa a to_ranges
def from_ranges(ranges) -> set:
    ids = set()
    for a, b in ranges:
        ids.update(range(int(a), int(b) + 1))
    return ids

#Chequea si no falta ningun id de 0 a total-1
def covers(ids, total) -> bool:
    return len(ids) >= total and all(i in ids for i in range(total))
