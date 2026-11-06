import zfec


def encode(data, k, m):
    """Cut data into k data shards + m parity shards (k+m shards in total)."""
    size = -(-len(data) // k)                    # shard size, rounded up
    padded = data.ljust(size * k, b"\0")         # pad with zeros
    parts = [padded[i * size:(i + 1) * size] for i in range(k)]
    return zfec.Encoder(k, k + m).encode(parts)


def decode(shards, k, m, original_size):
    """shards: dict {shard_index: bytes} with at least k entries."""
    idx = list(shards)[:k]
    parts = zfec.Decoder(k, k + m).decode([shards[i] for i in idx], idx)
    return b"".join(parts)[:original_size]

