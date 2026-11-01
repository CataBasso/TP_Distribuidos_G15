'''
Módulo para cargar y usar las palabras vacías (stop words) en ingles y francés.

Usar de la siguiente manera:
    from common.stopwords import STOP_WORDS
    if word in STOP_WORDS:
        # procesar palabra
'''
from pathlib import Path

STOPWORDS_FILE = Path(__file__).with_name("stopwords.txt")


def load_stopwords() -> set[str]:
    with STOPWORDS_FILE.open("r", encoding="utf-8") as file:
        return {line.strip() for line in file if line.strip()}


STOP_WORDS = load_stopwords()