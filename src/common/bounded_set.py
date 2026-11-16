from collections import OrderedDict

DEFAULT_MAX = 10000


class BoundedSet:

    def __init__(self, maxlen=DEFAULT_MAX):
        self._d = OrderedDict()
        self.maxlen = maxlen

    def add(self, item):
        self._d[item] = None
        self._d.move_to_end(item)
        while len(self._d) > self.maxlen:
            self._d.popitem(last=False)

    def discard(self, item):
        self._d.pop(item, None)

    def __contains__(self, item):
        return item in self._d

    def __len__(self):
        return len(self._d)

    def __iter__(self):
        return iter(self._d)
