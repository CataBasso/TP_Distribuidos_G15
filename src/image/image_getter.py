import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse
from urllib.request import Request, urlopen


from common.batch_stage import BatchStage
from common.ft.ids import msg_id
from common.protocol.messages import result_message, eof_message, with_id
from common.topology import IMG_CTRL, RESULTS_EX, results_key

USER_AGENT = os.environ.get("USER_AGENT", "SD1-TP-WikipediaAnalysis/1.0 (TP facultad; vfernandezd@fi.uba.ar)")
STAGE = "img_getter"
QUERY = 2
TITLES_PER_REQUEST = 50    
RETRIES = 5
PAUSE = float(os.environ.get("REQUEST_PAUSE", "0.5"))
ROUNDS = int(os.environ.get("IMAGE_ROUNDS", "3"))              # rondas de reintento por batch
ROUND_SLEEP = float(os.environ.get("IMAGE_RETRY_SLEEP", "3"))  # espera base entre rondas (s)
CACHE_DIR = os.environ.get("IMAGE_CACHE_DIR")                  # sin definir = sin cache


def parse_image_url(image_url):
    parts = unquote(urlparse(image_url).path).split("/")
    if len(parts) < 6 or parts[1] != "wikipedia":
        return None
    project = parts[2]
    filename = parts[6] if parts[3] == "thumb" and len(parts) >= 8 else parts[5]
    host = "commons.wikimedia.org" if project == "commons" else f"{project}.wikipedia.org"
    return host, "File:" + filename.replace("_", " ")


class WikimediaClient:
    def __init__(self, user_agent=USER_AGENT, retries=RETRIES, timeout=30, pause=PAUSE):
        self.user_agent, self.retries, self.timeout, self.pause = user_agent, retries, timeout, pause

    def _get(self, url):
        for attempt in range(self.retries):
            try:
                request = Request(url, headers={"User-Agent": self.user_agent})
                with urlopen(request, timeout=self.timeout) as response:
                    data = response.read()
                time.sleep(self.pause)
                return data
            except HTTPError as e:
                if e.code not in (429, 503):
                    raise
                wait = e.headers.get("Retry-After", "")
                wait = int(wait) if wait.isdigit() else 2 ** attempt
                logging.warning("Wikimedia respondió %s para %s: espero %ds (intento %d/%d)",
                                e.code, url, wait, attempt + 1, self.retries)
                time.sleep(wait)
            except URLError:
                time.sleep(2 ** attempt)
        raise RuntimeError(f"no se pudo obtener {url} tras {self.retries} intentos")

    def original_urls(self, host, titles):
        params = {"action": "query", "format": "json", "formatversion": 2, "prop": "imageinfo", "iiprop": "url", "redirects": 1, "titles": "|".join(titles), "maxlag": 5}
        data = json.loads(self._get(f"https://{host}/w/api.php?{urlencode(params)}"))
        query = data.get("query", {})
        normalized = {n["from"]: n["to"] for n in query.get("normalized", [])}
        redirects = {r["from"]: r["to"] for r in query.get("redirects", [])}
        pages = {p["title"]: p for p in query.get("pages", [])}
        found = {}
        for title in titles:
            resolved = redirects.get(normalized.get(title, title), normalized.get(title, title))
            page = pages.get(resolved)
            if page and not page.get("missing") and page.get("imageinfo"):
                found[title] = page["imageinfo"][0]["url"]
        return found

    def download(self, url):
        return self._get(url)


class ImageCache:

    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)

    def _path(self, image_url):
        return os.path.join(self.dir, hashlib.sha1(image_url.encode()).hexdigest())

    def get(self, image_url):
        try:
            with open(self._path(image_url), "rb") as f:
                return f.read()
        except OSError:
            return None

    def put(self, image_url, content):
        path = self._path(image_url)
        tmp = f"{path}.{os.getpid()}.tmp"
        try:
            with open(tmp, "wb") as f:
                f.write(content)
            os.replace(tmp, path)
        except OSError as e:
            logging.warning("no se pudo escribir la cache de imágenes: %s", e)


def fetch_images(rows, wiki, cache=None, rounds=ROUNDS, sleep=ROUND_SLEEP):
    wanted = {}
    for row in rows:
        image_url = (row.get("image_url") or "").strip()
        if image_url and image_url not in wanted:
            parsed = parse_image_url(image_url)
            if parsed is None:
                logging.warning("URL de imagen no reconocida: %s", image_url)
            else:
                wanted[image_url] = parsed

    images = {}
    if cache is not None:
        for image_url in wanted:
            cached = cache.get(image_url)
            if cached is not None:
                images[image_url] = cached
    pending = {u: k for u, k in wanted.items() if u not in images}
    originals = {}                       # (host, título) -> URL original
    absent = set()                       # (host, título) que la API dice que no existen

    for attempt in range(rounds):
        if not pending:
            break
        if attempt:
            logging.warning("reintento %d/%d de %d imágenes con falla transitoria",
                            attempt + 1, rounds, len(pending))
            time.sleep(sleep * attempt)
        failed = set()

        titles_by_host = defaultdict(set)
        for image_url, (host, title) in pending.items():
            if (host, title) not in originals and (host, title) not in absent:
                titles_by_host[host].add(title)
        for host, titles in titles_by_host.items():
            titles = sorted(titles)
            for i in range(0, len(titles), TITLES_PER_REQUEST):
                chunk = titles[i:i + TITLES_PER_REQUEST]
                try:
                    found = wiki.original_urls(host, chunk)
                except Exception as e:
                    logging.warning("la API de %s falló (%s: %s)", host, type(e).__name__, e)
                    continue                       # esas imágenes quedan pendientes
                originals.update({(host, t): url for t, url in found.items()})
                absent.update((host, t) for t in chunk if t not in found)

        for image_url, key in list(pending.items()):
            if key in absent:
                del pending[image_url]             # ausencia definitiva: sin imagen
            elif key not in originals:
                failed.add(image_url)              # la API no contestó
            else:
                try:
                    images[image_url] = content = wiki.download(originals[key])
                except Exception as e:
                    logging.warning("no se pudo descargar %s (%s: %s)", image_url, type(e).__name__, e)
                    failed.add(image_url)
                else:
                    del pending[image_url]
                    if cache is not None:
                        cache.put(image_url, content)
        pending = {u: pending[u] for u in failed}

    if pending:
        logging.error("%d imágenes NO se pudieron obtener tras %d rondas (siguen sin imagen): %s",
                      len(pending), rounds, sorted(pending)[:5])

    return [{"name": row["name"], "url": row["url"], "image": images.get((row.get("image_url") or "").strip())}
            for row in rows]


class ImageGetter(BatchStage):
    name = STAGE
    ctrl_exchange = IMG_CTRL

    def __init__(self, bus, replica_id, input_queue="img_in", state_dir=None, wiki=None):
        self.input_queue = input_queue
        self.wiki = wiki or WikimediaClient()
        self.cache = ImageCache(CACHE_DIR) if CACHE_DIR else None
        super().__init__(bus, replica_id, state_dir)

    def process_batch(self, client_id, message):
        out = result_message(client_id, QUERY, fetch_images(message["rows"], self.wiki, self.cache), origin=self.replica_id)
        self.publish(RESULTS_EX, with_id(out, msg_id(client_id, STAGE, message["batch_id"])), key=results_key(client_id))

    def publish_eof(self, client_id, total):
        eof = eof_message(client_id, origin=self.replica_id)
        eof["query"] = QUERY
        self.publish(RESULTS_EX, with_id(eof, msg_id(client_id, STAGE + "-eof")), key=results_key(client_id))


def main():
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage, state_dir
    rid = env_int("ID")
    run_stage(ImageGetter(RabbitBus(broker_host()), rid, env_str("INPUT_QUEUE","img_in"),state_dir(f"image_getter_{rid}")))


if __name__ == "__main__":
    main()
