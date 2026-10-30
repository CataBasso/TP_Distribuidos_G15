import os

os.environ.setdefault("ID", "1")
os.environ.setdefault("MOM_HOST", "localhost")

from image.image_getter import fetch_images, parse_image_url  # noqa: E402


class FakeWiki:
    def __init__(self, known, fail_download=()):
        self.known, self.fail_download = known, set(fail_download)
        self.api_calls = []

    def original_urls(self, host, titles):
        self.api_calls.append((host, list(titles)))
        return {t: self.known[(host, t)] for t in titles if (host, t) in self.known}

    def download(self, url):
        if url in self.fail_download:
            raise RuntimeError("boom")
        return b"bytes:" + url.encode()


def test_parse_commons_and_thumbnail_urls():
    plain = "https://upload.wikimedia.org/wikipedia/commons/3/3b/Satake_Yoshinobu.jpg"
    thumb = "https://upload.wikimedia.org/wikipedia/fr/thumb/1/14/Foo_%28x%29.jpg/200px-Foo_%28x%29.jpg"
    assert parse_image_url(plain) == ("commons.wikimedia.org", "File:Satake Yoshinobu.jpg")
    assert parse_image_url(thumb) == ("fr.wikipedia.org", "File:Foo (x).jpg")
    assert parse_image_url("https://example.com/x.jpg") is None


def test_every_row_is_reported_even_without_a_downloadable_image():
    u = "https://upload.wikimedia.org/wikipedia/commons/3/3b/A.jpg"
    bad = "https://upload.wikimedia.org/wikipedia/commons/1/14/B.jpg"
    wiki = FakeWiki({("commons.wikimedia.org", "File:A.jpg"): "http://orig/A",
                     ("commons.wikimedia.org", "File:B.jpg"): "http://orig/B"},
                    fail_download=["http://orig/B"])
    rows = [{"name": "a", "url": "ua", "image_url": u},
            {"name": "b", "url": "ub", "image_url": bad},
            {"name": "c", "url": "uc", "image_url": None},
            {"name": "d", "url": "ud", "image_url": "https://example.com/x.jpg"}]
    out = fetch_images(rows, wiki)
    assert [r["name"] for r in out] == ["a", "b", "c", "d"]
    assert out[0]["image"] == b"bytes:http://orig/A"
    assert [r["image"] for r in out[1:]] == [None, None, None]


def test_titles_of_a_host_go_in_one_api_call():
    base = "https://upload.wikimedia.org/wikipedia/commons/3/3b/"
    rows = [{"name": n, "url": n, "image_url": f"{base}{n}.jpg"} for n in "abc"]
    wiki = FakeWiki({})
    fetch_images(rows, wiki)
    assert len(wiki.api_calls) == 1 and wiki.api_calls[0][1] == ["File:a.jpg", "File:b.jpg", "File:c.jpg"]
