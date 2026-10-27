import base64
import csv
import hashlib
from pathlib import Path

from common.protocol import RESULT_COLUMNS

CSV_HEADERS = {
    1: ("date_modified", "url"),
    2: ("name", "url", "image_sha256"),
    3: ("word",),
    4: ("word", "count"),
    5: ("url", "abstract_word_count"),
}


class ResultWriter:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.images_dir = self.output_dir / "images"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(exist_ok=True)
        self._files = {}
        self._writers = {}
        self.rows_written = {q: 0 for q in CSV_HEADERS}
        for query, header in CSV_HEADERS.items():
            f = open(self.output_dir / f"output_query_{query}.csv", "w", encoding="utf-8", newline="")
            writer = csv.writer(f)
            writer.writerow(header)
            f.flush()
            self._files[query] = f
            self._writers[query] = writer

    #Escribe un mensaje 'result' del Gateway: {query, rows}
    def handle(self, message: dict) -> None:
        query = message["query"]
        if query not in CSV_HEADERS:
            raise ValueError(f"query desconocida: {query}")
        writer = self._writers[query]
        for row in message["rows"]:
            writer.writerow(self._to_csv_row(query, row))
        self._files[query].flush()
        self.rows_written[query] += len(message["rows"])

    def _to_csv_row(self, query: int, row: dict) -> list:
        if query == 2:
            return [row["name"], row["url"], self._save_image(row.get("image"))]
        return [row[column] for column in RESULT_COLUMNS[query]]

    #Guarda los bytes de la imagen como images/<sha256> y devuelve el sha256
    #El protocolo binario transporta bytes directamente
    #Se acepta también base64 (texto) por compatibilidad
    def _save_image(self, image) -> str:
        if not image:
            return ""
        content = bytes(image) if isinstance(image, (bytes, bytearray)) else base64.b64decode(image)
        digest = hashlib.sha256(content).hexdigest()
        path = self.images_dir / digest
        if not path.exists():
            path.write_bytes(content)
        return digest

    def close(self) -> None:
        for f in self._files.values():
            f.close()
