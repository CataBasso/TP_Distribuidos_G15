#!/usr/bin/env python3
import csv
import hashlib
import sys
from pathlib import Path

try:
    from PIL import Image
except ImportError:      # opcional
    Image = None


def check(client_dir: Path) -> int:
    images = client_dir / "images"
    bad = 0
    files = sorted(images.glob("*")) if images.is_dir() else []
    for f in files:
        digest, ext = f.stem, f.suffix
        content = f.read_bytes()
        problems = []
        if hashlib.sha256(content).hexdigest() != digest:
            problems.append("el nombre no es el sha256 del contenido")
        if ext == ".bin":
            problems.append("no parece una imagen (magic bytes desconocidos)")
        elif Image is not None and ext != ".svg":
            try:
                with Image.open(f) as im:
                    im.load()
            except Exception as e:                      # noqa: BLE001
                problems.append(f"Pillow no la pudo decodificar: {e}")
        if problems:
            bad += 1
            print(f"[MAL] {f}: " + "; ".join(problems))
    known = {f.stem for f in files}
    q2 = client_dir / "output_query_2.csv"
    missing_file = empty = 0
    if q2.exists():
        with q2.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                sha = row["image_sha256"]
                if not sha:
                    empty += 1
                    print(f"[SIN IMAGEN] {row['name']} ({row['url']})")
                elif sha not in known:
                    missing_file += 1
                    print(f"[FALTA ARCHIVO] {sha} ({row['url']})")
    print(f"{client_dir}: {len(files)} imágenes, {bad} inválidas, {missing_file} sin archivo, "
          f"{empty} artículos sin imagen")
    return bad + missing_file


if __name__ == "__main__":
    dirs = [Path(a) for a in sys.argv[1:]] or sorted(Path("output").glob("client_*"))
    sys.exit(1 if sum(check(d) for d in dirs) else 0)
