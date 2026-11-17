"""The identity of the public suffix list that the `tld` package splits domains with.

`DomainTld`, `DomainChopper` and `EmailDomain` answer from this list, so two workers holding different versions of it
can split one domain differently. Stored execution results carry the list's id so that such differences can be told
apart from real ones.
"""

import functools
import hashlib
from os.path import isabs
from typing import Optional

from osprey.worker.lib.osprey_shared.logging import get_logger
from tld.helpers import project_dir
from tld.utils import MozillaTLDSourceParser

logger = get_logger()

# osprey-rs names its list the same way (`PublicSuffixList::id`), so the same file has the same id on both workers.
ID_HEX_DIGITS = 16


def list_path() -> str:
    """The file `tld.get_tld` loads with its default parser, resolved as `tld` resolves it."""
    local_path = MozillaTLDSourceParser.local_path
    return local_path if isabs(local_path) else project_dir(local_path)


def list_id(path: str) -> Optional[str]:
    """The first 16 hex digits of the SHA-256 of the file's bytes, or None when it can't be read."""
    try:
        with open(path, 'rb') as f:
            digest = hashlib.sha256(f.read()).hexdigest()
    except OSError:
        logger.warning(f'Could not read the public suffix list at {path}; results will not name it', exc_info=True)
        return None
    return digest[:ID_HEX_DIGITS]


@functools.lru_cache(maxsize=None)
def public_suffix_list_id() -> Optional[str]:
    """The id of the list this process splits domains with, computed once."""
    try:
        path = list_path()
    except Exception:
        logger.warning('Could not locate the public suffix list; results will not name it', exc_info=True)
        return None
    return list_id(path)
