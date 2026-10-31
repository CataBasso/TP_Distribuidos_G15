#!/usr/bin/env python3
import nltk
from nltk.corpus import stopwords
from pathlib import Path

OUTPUT_FILE = Path("src/common/stopwords/stopwords.txt")

nltk.download("stopwords", quiet=True)

stop_words = set(stopwords.words("english")) | set(stopwords.words("french"))

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", encoding="utf-8") as file:
    file.write("\n".join(sorted(stop_words)) + "\n")

print(f"Se generaron {len(stop_words)} stopwords en {OUTPUT_FILE}")