from hashlib import md5, sha1, sha256, sha512

from ._prelude import ArgumentsBase, ExecutionContext, UDFBase
from .categories import UdfCategories

SHA256_INT_BYTES = 8
SHA256_INT_NON_NEGATIVE_MASK = (1 << 63) - 1


class Arguments(ArgumentsBase):
    input: str


# Use usedforsecurity=False in 3.9+, md5 is insecure but we're not using this in a security context.
class HashMd5(UDFBase[Arguments, str]):
    """Returns the MD5 hash of a string."""

    category = UdfCategories.HASH

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return md5(arguments.input.encode()).hexdigest()


class HashSha1(UDFBase[Arguments, str]):
    """Returns the SHA-1 hash of a string."""

    category = UdfCategories.HASH

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return sha1(arguments.input.encode()).hexdigest()


class HashSha256(UDFBase[Arguments, str]):
    """Returns the SHA-256 hash of a string."""

    category = UdfCategories.HASH

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return sha256(arguments.input.encode()).hexdigest()


class HashSha256Int(UDFBase[Arguments, int]):
    """Returns a non-negative 63-bit integer derived from a string's SHA-256 digest.

    The UDF encodes the first eight SHA-256 digest bytes in big-endian order,
    then clears the sign bit. The result is in the range ``0..2^63-1``.
    """

    category = UdfCategories.HASH

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> int:
        digest = sha256(arguments.input.encode('utf-8')).digest()
        return int.from_bytes(digest[:SHA256_INT_BYTES], byteorder='big') & SHA256_INT_NON_NEGATIVE_MASK


class HashSha512(UDFBase[Arguments, str]):
    """Returns the SHA-512 hash of a string."""

    category = UdfCategories.HASH

    def execute(self, execution_context: ExecutionContext, arguments: Arguments) -> str:
        return sha512(arguments.input.encode()).hexdigest()
