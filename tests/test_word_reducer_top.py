import collections, random, heapq


def old(words, n):
    c = collections.Counter({w: en + fr for w, (en, fr) in sorted(words.items())})
    return dict(c.most_common(n))


def new(words, n):
    best = heapq.nsmallest(n, ((-(en + fr), w) for w, (en, fr) in words.items()))
    return {w: -neg for neg, w in best}


def test_top_selection_identical_including_ties():
    rng = random.Random(3)
    for _ in range(200):
        words = {f"w{rng.randint(0, 300)}": [rng.randint(0, 4), rng.randint(0, 4)] for _ in range(150)}
        n = rng.choice([1, 5, 20, 400])
        a, b = old(words, n), new(words, n)
        assert list(a.items()) == list(b.items())
