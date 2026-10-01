import hashlib
import random

import pytest
from osprey.worker.lib.storage.bloom_filter import BloomFilter


def _random_keys(seed: int, count: int) -> list[int]:
    rng = random.Random(seed)
    return [rng.getrandbits(63) for _ in range(count)]


def test_added_keys_are_always_present() -> None:
    bloom = BloomFilter()
    keys = _random_keys(seed=1, count=1000)
    for key in keys:
        bloom.add(key)

    assert all(key in bloom for key in keys)


def test_false_positive_rate_at_1000_keys_is_under_one_in_ten_thousand() -> None:
    bloom = BloomFilter()
    keys = set(_random_keys(seed=2, count=1000))
    for key in keys:
        bloom.add(key)

    probes = [key for key in _random_keys(seed=3, count=100_000) if key not in keys]
    false_positives = sum(1 for key in probes if key in bloom)

    assert false_positives / len(probes) < 0.0001


def test_round_trip_through_bytes() -> None:
    bloom = BloomFilter()
    keys = _random_keys(seed=4, count=200)
    for key in keys:
        bloom.add(key)

    data = bloom.to_bytes()
    restored = BloomFilter.from_bytes(data, num_hashes=7)

    assert len(data) == 4096
    assert restored.to_bytes() == data
    assert all(key in restored for key in keys)


def test_hash_is_stable_across_filters_and_matches_the_documented_layout() -> None:
    key = 123456789012345678
    first = BloomFilter()
    second = BloomFilter()
    first.add(key)
    second.add(key)

    # Other readers rebuild positions from this recipe, so pin it here.
    digest = hashlib.blake2b(key.to_bytes(8, 'big'), digest_size=32).digest()
    positions = {int.from_bytes(digest[i * 4 : i * 4 + 4], 'big') % 32768 for i in range(7)}
    data = first.to_bytes()
    set_bits = {i for i in range(32768) if data[i // 8] & (1 << (i % 8))}

    assert data == second.to_bytes()
    assert set_bits == positions


def test_num_bits_must_be_a_multiple_of_8() -> None:
    with pytest.raises(ValueError):
        BloomFilter(num_bits=1001)
