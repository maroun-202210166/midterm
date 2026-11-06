import bisect
import hashlib


def _h(s: str) -> int:
    return int(hashlib.sha256(s.encode()).hexdigest(), 16)


class Ring:
    """Consistent hashing ring with virtual nodes."""

    def __init__(self, nodes=(), vnodes=100):
        self.vnodes = vnodes
        self._map = {}   # ring position -> node id
        self._keys = []  # sorted positions
        for n in nodes:
            self.add_node(n)

    def add_node(self, node):
        for i in range(self.vnodes):
            self._map[_h(f"{node}#{i}")] = node
        self._keys = sorted(self._map)

    def remove_node(self, node):
        self._map = {k: v for k, v in self._map.items() if v != node}
        self._keys = sorted(self._map)

    def nodes(self):
        return set(self._map.values())

    def get_nodes(self, chunk_hash: str, count: int = 1):
        """First `count` distinct nodes clockwise from the chunk's position."""
        if not self._keys:
            return []
        count = min(count, len(self.nodes()))
        i = bisect.bisect(self._keys, int(chunk_hash, 16))
        result = []
        while len(result) < count:
            node = self._map[self._keys[i % len(self._keys)]]
            if node not in result:
                result.append(node)
            i += 1
        return result