import json
import logging
import os
import signal
import time
from collections import defaultdict
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse
from urllib.request import Request, urlopen

from common.middleware.middleware_rabbitmq import (
    RabbitConnection, MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ,
)
from common.protocol import internal, messages

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ.get("INPUT_QUEUE", "img_in")
USER_AGENT = os.environ.get("USER_AGENT", "SD1-TP-WikipediaAnalysis/1.0 (TP facultad)")
RESULTS_EXCHANGE = "results_ex"
IMG_CONTROL_EXCHANGE = "img_ctrl"
ABORT_EXCHANGE = "abort_ex"
STAGE = "img"
QUERY = 2
TITLES_PER_REQUEST = 50    
RETRIES = 5
PAUSE = float(os.environ.get("REQUEST_PAUSE", "0.5")) 


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


def fetch_images(rows, wiki):
    wanted = {}                                  
    for row in rows:
        image_url = (row.get("image_url") or "").strip()
        if image_url and image_url not in wanted:
            parsed = parse_image_url(image_url)
            if parsed is None:
                logging.warning("URL de imagen no reconocida: %s", image_url)
            else:
                wanted[image_url] = parsed

    titles_by_host = defaultdict(set)
    for host, title in wanted.values():
        titles_by_host[host].add(title)
    originals = {}                             
    for host, titles in titles_by_host.items():
        titles = sorted(titles)
        for i in range(0, len(titles), TITLES_PER_REQUEST):
            try:
                found = wiki.original_urls(host, titles[i:i + TITLES_PER_REQUEST])
            except Exception as e:
                logging.warning("la API de %s falló (%s: %s)", host, type(e).__name__, e)
                continue
            originals.update({(host, t): url for t, url in found.items()})

    images = {}                                 
    for image_url, key in wanted.items():
        if key in originals:
            try:
                images[image_url] = wiki.download(originals[key])
            except Exception as e:
                logging.warning("no se pudo descargar %s (%s: %s)", image_url, type(e).__name__, e)

    return [{"name": row["name"], "url": row["url"], "image": images.get((row.get("image_url") or "").strip())}
            for row in rows]


class ImageGetter:
    def __init__(self, wiki=None):
        self.id = ID
        self.wiki = wiki or WikimediaClient()
        self.rc = RabbitConnection(MOM_HOST)
        self.input = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE, connection=self.rc)
        self.results = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, RESULTS_EXCHANGE, [], exchange_type="direct", connection=self.rc)
        self.control = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, IMG_CONTROL_EXCHANGE, [], exchange_type="fanout", connection=self.rc)
        self.abort = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, ABORT_EXCHANGE, [], exchange_type="fanout", connection=self.rc)

        self.count = {}         
        self.eof_total = {}    
        self.reported = {}      
        self.holds_eof = set()  
        self.done = set()
        self.aborted = set()

    def _send(self, message, client_id):
        self.results.send(internal.serialize(message), routing_key=str(client_id[0]))

    def on_data(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)
        if client_id not in self.aborted:
            if message["type"] == "batch":
                self._on_batch(client_id, message)
            elif message["type"] in ("eof", "stage_eof"):
                self._on_eof(client_id, message)
            else:
                logging.warning("tipo inesperado en img_in: %s", message["type"])
        ack()

    def _on_batch(self, client_id, message):
        rows = fetch_images(message["rows"], self.wiki)
        self._send(messages.result_message(client_id, QUERY, rows, origin=self.id), client_id)
        self.count[client_id] = self.count.get(client_id, 0) + 1
        if client_id in self.eof_total:
            self._broadcast(client_id)

    def _on_eof(self, client_id, message):
        if "total" not in message:
            raise ValueError("el EOF de img_in debe traer el total de batches")
        self.holds_eof.add(client_id)
        self._learn_total(client_id, message["total"])

    def _broadcast(self, client_id):
        self.control.send(internal.serialize(messages.control_message(
            client_id, STAGE, self.id, self.count.get(client_id, 0),
            total=self.eof_total.get(client_id))))

    def _learn_total(self, client_id, total):
        if client_id not in self.eof_total:
            self.eof_total[client_id] = total
            self._broadcast(client_id)

    def on_control(self, body, ack, nack):
        message = internal.deserialize(body)
        client_id = messages.client_id_of(message)
        if client_id not in self.done and client_id not in self.aborted:
            if message.get("total") is not None:
                self._learn_total(client_id, message["total"])
            reported = self.reported.setdefault(client_id, {})
            replica = message["replica"]
            reported[replica] = max(reported.get(replica, 0), message["count"])
            total = self.eof_total.get(client_id)
            if total is not None and sum(reported.values()) >= total:
                self._flush(client_id)
        ack()

    def _flush(self, client_id):
        total = self.eof_total.pop(client_id)
        self.count.pop(client_id, None)
        self.reported.pop(client_id, None)
        self.done.add(client_id)
        if client_id in self.holds_eof: 
            self.holds_eof.discard(client_id)
            self._send(messages.eof_message(
                client_id, origin=self.id, query=QUERY, total=total), client_id)

    def on_abort(self, body, ack, nack):
        client_id = messages.client_id_of(internal.deserialize(body))
        self.aborted.add(client_id)
        for state in (self.count, self.eof_total, self.reported):
            state.pop(client_id, None)
        self.holds_eof.discard(client_id)
        ack()

    def setup_consumers(self):
        self.control.consume(self.on_control)
        self.abort.consume(self.on_abort)
        self.input.consume(self.on_data)

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self.rc.stop())
        self.setup_consumers()
        try:
            self.rc.run()
        finally:
            self.close()

    def close(self):
        for name, endpoint in [("input", self.input), ("results", self.results),
                               ("control", self.control), ("abort", self.abort)]:
            try:
                endpoint.close()
            except Exception as e:
                logging.error(f"Error al cerrar '{name}': {e}")
        try:
            self.rc.close()
        except Exception as e:
            logging.error(f"Error al cerrar la conexión: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    ImageGetter().start()
    return 0


if __name__ == "__main__":
    main()
