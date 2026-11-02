import csv
import os
import pytest
from collections import Counter
from pathlib import Path


OUTPUT_ROOT = Path(os.environ.get("OUTPUT_ROOT", "output"))
EXPECTED_DIR = Path(os.environ.get("EXPECTED_DIR", "data/expected/medium"))


def client_dirs() -> list[Path]:
    dirs = sorted(p for p in OUTPUT_ROOT.glob("client_*") if p.is_dir())
    assert dirs, f"No se encontraron carpetas client_* en {OUTPUT_ROOT}"
    return dirs



def compare_csv(
    output_dir: Path,
    output_file: str,
    expected_file: str,
    check_order: bool = False,
) -> None:
    output_path = output_dir / output_file
    expected_path = EXPECTED_DIR / expected_file

    assert output_path.exists(), f"No existe el archivo de salida: {output_path}"
    assert expected_path.exists(), f"No existe el archivo esperado: {expected_path}"

    with output_path.open("r", encoding="utf-8", newline="") as file:
        output_reader = csv.DictReader(file)
        output_columns = output_reader.fieldnames
        output_rows = list(output_reader)

    with expected_path.open("r", encoding="utf-8", newline="") as file:
        expected_reader = csv.DictReader(file)
        expected_columns = expected_reader.fieldnames
        expected_rows = list(expected_reader)

    errors = []

    if output_columns != expected_columns:
        errors.append(
            f"Columnas diferentes:\n"
            f"  Esperadas: {expected_columns}\n"
            f"  Obtenidas: {output_columns}"
        )

    if check_order:
        if output_rows != expected_rows:
            errors.append("Las filas o sus valores no coinciden en el orden esperado.")
            missing = Counter(
                tuple(sorted(row.items())) for row in expected_rows
            ) - Counter(tuple(sorted(row.items())) for row in output_rows)
            extra = Counter(
                tuple(sorted(row.items())) for row in output_rows
            ) - Counter(tuple(sorted(row.items())) for row in expected_rows)
        else:
            missing = Counter()
            extra = Counter()
    else:
        missing = Counter(
            tuple(sorted(row.items())) for row in expected_rows
        ) - Counter(tuple(sorted(row.items())) for row in output_rows)

        extra = Counter(
            tuple(sorted(row.items())) for row in output_rows
        ) - Counter(tuple(sorted(row.items())) for row in expected_rows)

    if missing:
        errors.append(
            "Filas que faltan en el output:\n"
            + "\n".join(f"  {dict(row)}" for row in missing.elements())
        )

    if extra:
        errors.append(
            "Filas inesperadas en el output:\n"
            + "\n".join(f"  {dict(row)}" for row in extra.elements())
        )

    if errors:
        raise AssertionError(
            f"Error comparando {output_file} con {expected_file}:\n"
            + "\n".join(errors)
            + f"\nTotal de filas esperadas: {len(expected_rows)}"
            + f"\nTotal de filas obtenidas: {len(output_rows)}"
        )

@pytest.fixture(params=client_dirs() if OUTPUT_ROOT.exists() else [], ids=lambda p: p.name)
def client_dir(request):
    return request.param

def test_query1(client_dir):
    compare_csv(client_dir, "output_query_1.csv", "query1.csv")

def test_query2(client_dir):
    compare_csv(client_dir, "output_query_2.csv", "query2.csv")

def test_query3(client_dir):
    compare_csv(client_dir, "output_query_3.csv", "query3.csv")

def test_query4(client_dir):
    compare_csv(client_dir, "output_query_4.csv", "query4.csv", check_order=True)

def test_query5(client_dir):
    compare_csv(client_dir, "output_query_5.csv", "query5.csv")