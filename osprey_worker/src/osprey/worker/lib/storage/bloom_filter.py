from __future__ import annotations

import hashlib


class BloomFilter:
    """A fixed-size Bloom filter over 64-bit integer keys.

    The defaults (32768 bits, 7 hashes) keep the false-positive rate under 1e-5 at 1000 keys.

    Positions come from `blake2b`, never Python `hash()`, so every process and every reader
    computes the same bits for a key. Bit `p` lives in byte `p // 8` at mask `1 << (p % 8)`.
    """

    def __init__(self, num_bits: int = 32768, num_hashes: int = 7, bits: bytes | None = None) -> None:
        if num_bits <= 0 or num_bits % 8 != 0:
            raise ValueError(f'num_bits must be a positive multiple of 8, got {num_bits}')
        # A 32-byte digest yields eight 32-bit words.
        if not 1 <= num_hashes <= 8:
            raise ValueError(f'num_hashes must be between 1 and 8, got {num_hashes}')
        if bits is not None and len(bits) * 8 != num_bits:
            raise ValueError(f'bits holds {len(bits) * 8} bits, expected {num_bits}')
        self._num_bits = num_bits
        self._num_hashes = num_hashes
        self._bits = bytearray(bits) if bits is not None else bytearray(num_bits // 8)

    def _positions(self, key: int) -> list[int]:
        digest = hashlib.blake2b(key.to_bytes(8, 'big'), digest_size=32).digest()
        return [int.from_bytes(digest[i * 4 : i * 4 + 4], 'big') % self._num_bits for i in range(self._num_hashes)]

    def add(self, key: int) -> None:
        for position in self._positions(key):
            self._bits[position >> 3] |= 1 << (position & 7)

    def __contains__(self, key: int) -> bool:
        return all(self._bits[position >> 3] & (1 << (position & 7)) for position in self._positions(key))

    def to_bytes(self) -> bytes:
        return bytes(self._bits)

    @classmethod
    def from_bytes(cls, data: bytes, num_hashes: int) -> BloomFilter:
        return cls(num_bits=len(data) * 8, num_hashes=num_hashes, bits=data)
