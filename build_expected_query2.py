#!/usr/bin/env python3
import csv
import hashlib
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

USER_AGENT = "SD1-TP-WikipediaAnalysis/1.0 (TP facultad; vfernandezd@fi.uba.ar)"
BATCH = 50  
PAUSE = 0.5  

SOURCE = Path("data/expected/query2_source.csv")
TARGET = Path("data/expected/query2.csv")

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT

def parse_image_url(image_url):
    """Devuelve (host_api, 'File:Nombre.ext') o None si la URL no se reconoce."""
    parts = unquote(urlparse(image_url).path).split("/")
    if len(parts) < 6 or parts[1] != "wikipedia":
        return None
    project = parts[2]
    if parts[3] == "thumb" and len(parts) >= 8:
        filename = parts[6]
    else:
        filename = parts[5]
    host = "commons.wikimedia.org" if project == "commons" else f"{project}.wikipedia.org"
    return host, "File:" + filename.replace("_", " ")

def _retry_wait(response, attempt):
    try:
        return int(response.headers.get("Retry-After", 2 ** attempt))
    except ValueError:
        return 2 ** attempt

def api_get(host, params, retries=5):
    url = f"https://{host}/w/api.php"
    for attempt in range(retries):
        r = session.get(url, params=params, timeout=30)
        if r.status_code in (429, 503):
            time.sleep(_retry_wait(r, attempt))
            continue
        r.raise_for_status()
        data = r.json()
        if data.get("error", {}).get("code") == "maxlag":
            time.sleep(_retry_wait(r, attempt))
            continue
        return data
    raise RuntimeError(f"La API de {host} no respondió tras {retries} intentos")

def fetch_imageinfo(host, titles):
    data = api_get(host, {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "prop": "imageinfo",
        "iiprop": "url|sha1|size|mime",
        "redirects": 1,
        "titles": "|".join(titles),
        "maxlag": 5,
    })
    query = data.get("query", {})
    normalized = {n["from"]: n["to"] for n in query.get("normalized", [])}
    redirects = {r["from"]: r["to"] for r in query.get("redirects", [])}
    pages = {p["title"]: p for p in query.get("pages", [])}

    result = {}
    for title in titles:
        resolved = normalized.get(title, title)
        resolved = redirects.get(resolved, resolved)
        page = pages.get(resolved)
        if page and not page.get("missing") and page.get("imageinfo"):
            result[title] = page["imageinfo"][0]
    return result

def download(url, retries=5):
    for attempt in range(retries):
        r = session.get(url, timeout=60)
        if r.status_code in (429, 503):
            time.sleep(_retry_wait(r, attempt))
            continue
        r.raise_for_status()
        return r.content
    raise RuntimeError(f"No se pudo descargar {url}")

def main():
    with SOURCE.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    wanted = {} 
    for row in rows:
        image_url = (row.get("image") or "").strip()
        if not image_url:
            continue
        parsed = parse_image_url(image_url)
        if parsed is None:
            print(f"[WARN] URL de imagen no reconocida: {image_url}", file=sys.stderr)
            continue
        wanted[image_url] = parsed

    by_host = defaultdict(set)
    for host, title in wanted.values():
        by_host[host].add(title)

    infos = {} 
    for host, titles in by_host.items():
        titles = sorted(titles)
        for i in range(0, len(titles), BATCH):
            batch = titles[i:i + BATCH]
            for title, info in fetch_imageinfo(host, batch).items():
                infos[(host, title)] = info
            time.sleep(PAUSE)

    sha_by_image_url = {}
    failures = []
    for image_url, key in wanted.items():
        info = infos.get(key)
        if info is None:
            failures.append((image_url, "la API no devolvió imageinfo"))
            continue
        try:
            content = download(info["url"])
        except Exception as e:  
            failures.append((image_url, f"descarga fallida: {e}"))
            continue
        if hashlib.sha1(content).hexdigest() != info["sha1"] or len(content) != info["size"]:
            failures.append((image_url, "los bytes no coinciden con sha1/size de la API"))
            continue
        sha256 = hashlib.sha256(content).hexdigest()
        sha_by_image_url[image_url] = sha256
        time.sleep(PAUSE)

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    with TARGET.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "url", "image_sha256"])
        for row in rows:
            image_url = (row.get("image") or "").strip()
            writer.writerow([row["name"], row["url"], sha_by_image_url.get(image_url, "")])

    print(f"{len(rows)} filas escritas en {TARGET}")
    print(f"{len(sha_by_image_url)} imágenes descargadas y verificadas contra la API")
    if failures:
        print(f"{len(failures)} imágenes con problemas (quedaron con hash vacío):", file=sys.stderr)
        for image_url, reason in failures:
            print(f"  - {image_url}: {reason}", file=sys.stderr)

if __name__ == "__main__":
    main()