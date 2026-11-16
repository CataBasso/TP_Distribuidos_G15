
import sys, time
from common.protocol import encode, decode, batch_message

rows_n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
abstract = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
row = {"url": "http://x/y", "name": "n" * 20, "lang": "en", "date_modified": "2001-01-01 00:00:00",
       "abstract": "x" * abstract, "sections": "s" * 500, "image_url": "http://i/j.png"}
msg = batch_message((1, 1), 0, [dict(row) for _ in range(rows_n)])
def t(fn, n=30):
    s = time.perf_counter()
    for _ in range(n): fn()
    return (time.perf_counter() - s) / n * 1e3
data = encode(msg)
print(f"batch de {rows_n} filas = {len(data)/1024:.0f} KiB")
print(f"decode : {t(lambda: decode(data)):.2f} ms")
print(f"encode : {t(lambda: encode(msg)):.2f} ms")
print(f"Gateway   ~ decode + encode            = {t(lambda: encode(decode(data))):.2f} ms/batch")
print(f"YearFilter~ decode + 3 encode (q1,q2,norm) = {t(lambda: [encode(decode(data)) for _ in range(1)] and [encode(msg) for _ in range(2)]):.2f} ms/batch (aprox.)")
