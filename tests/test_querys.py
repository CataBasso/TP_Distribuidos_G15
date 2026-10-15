import csv
from collections import Counter
from pathlib import Path


OUTPUT_DIR = Path("output")
EXPECTED_DIR = Path("data/expected")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def compare_csv(
    output_file: str,
    expected_file: str,
    check_order: bool = False,
) -> None:
    output_path = OUTPUT_DIR / output_file
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


def test_query1():
    compare_csv("output_query_1.csv", "query1.csv")


def test_query2():
    compare_csv("output_query_2.csv", "query2.csv")


def test_query3():
    compare_csv("output_query_3.csv", "query3.csv")


def test_query4():
    compare_csv("output_query_4.csv", "query4.csv", check_order=True)


def test_query5():
    compare_csv("output_query_5.csv", "query5.csv")