import random

# Fixed random table for the rolling (gear) hash. Seeded so it is identical on every run.
_rng = random.Random(42)
GEAR = [_rng.getrandbits(64) for _ in range(256)]
MASK64 = (1 << 64) - 1


def fixed_chunks(data: bytes, size: int = 4 * 1024 * 1024):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def cdc_chunks(data: bytes, min_size=16 * 1024, avg_size=64 * 1024, max_size=256 * 1024):
    """Content-defined chunking: cut where the rolling hash matches a pattern,
    so boundaries depend on content, not on position."""
    mask = avg_size - 1  # avg_size must be a power of 2
    n, start = len(data), 0
    while start < n:
        end = min(start + max_size, n)
        cut, fp = end, 0
        for i in range(start, end):
            fp = ((fp << 1) + GEAR[data[i]]) & MASK64
            if i - start + 1 >= min_size and (fp & mask) == 0:
                cut = i + 1
                break
        yield data[start:cut]
        start = cut


def split(data: bytes, mode: str, fixed_size: int):
    return cdc_chunks(data) if mode == "cdc" else fixed_chunks(data, fixed_size)