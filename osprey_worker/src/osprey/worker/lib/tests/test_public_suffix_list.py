import hashlib
from pathlib import Path
from typing import Iterator

import pytest
import tld
from osprey.worker.lib import public_suffix_list
from osprey.worker.lib.public_suffix_list import list_id, list_path, public_suffix_list_id
from tld import get_tld
from tld.conf import get_setting, set_setting
from tld.utils import reset_tld_names

_SYNTHETIC_LIST = (
    b'// ===BEGIN ICANN DOMAINS===\n'
    b'osprey-test-tld\n'
    b'// ===END ICANN DOMAINS===\n'
    b'// ===BEGIN PRIVATE DOMAINS===\n'
    b'// ===END PRIVATE DOMAINS===\n'
)


@pytest.fixture
def fresh_id() -> Iterator[None]:
    public_suffix_list_id.cache_clear()
    yield
    public_suffix_list_id.cache_clear()


def test_list_id_is_the_first_16_hex_digits_of_the_files_sha256(tmp_path: Path) -> None:
    path = tmp_path / 'list.dat'
    path.write_bytes(_SYNTHETIC_LIST)

    assert list_id(str(path)) == hashlib.sha256(_SYNTHETIC_LIST).hexdigest()[:16]


def test_list_id_is_a_fixed_function_of_the_bytes(tmp_path: Path) -> None:
    path = tmp_path / 'list.dat'
    path.write_bytes(b'abc')

    # SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    assert list_id(str(path)) == 'ba7816bf8f01cfea'


def test_list_id_hashes_raw_bytes_without_newline_translation(tmp_path: Path) -> None:
    crlf = _SYNTHETIC_LIST.replace(b'\n', b'\r\n')
    path = tmp_path / 'list.dat'
    path.write_bytes(crlf)

    assert list_id(str(path)) == hashlib.sha256(crlf).hexdigest()[:16]
    assert list_id(str(path)) != hashlib.sha256(_SYNTHETIC_LIST).hexdigest()[:16]


def test_an_unreadable_list_has_no_id(tmp_path: Path) -> None:
    assert list_id(str(tmp_path / 'missing.dat')) is None
    assert list_id(str(tmp_path)) is None
    assert list_id(str(tmp_path / 'embedded\0null.dat')) is None


def test_list_path_is_the_file_tld_splits_domains_with(tmp_path: Path) -> None:
    res = tmp_path / 'res'
    res.mkdir()
    (res / 'effective_tld_names.dat.txt').write_bytes(_SYNTHETIC_LIST)
    parent = get_setting('NAMES_LOCAL_PATH_PARENT')
    try:
        set_setting('NAMES_LOCAL_PATH_PARENT', str(tmp_path))
        reset_tld_names()
        assert get_tld('a.b.osprey-test-tld', fix_protocol=True) == 'osprey-test-tld'
        assert list_path() == str(res / 'effective_tld_names.dat.txt')
    finally:
        set_setting('NAMES_LOCAL_PATH_PARENT', parent)
        reset_tld_names()


def test_the_process_id_is_the_installed_lists_id(fresh_id: None) -> None:
    installed = Path(tld.__file__).parent / 'res' / 'effective_tld_names.dat.txt'
    expected = hashlib.sha256(installed.read_bytes()).hexdigest()[:16]

    assert public_suffix_list_id() == expected


def test_the_process_id_is_computed_once(fresh_id: None, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def counting_list_id(path: str) -> str:
        calls.append(path)
        return 'synthetic0000000'

    monkeypatch.setattr(public_suffix_list, 'list_id', counting_list_id)

    assert public_suffix_list_id() == 'synthetic0000000'
    assert public_suffix_list_id() == 'synthetic0000000'
    assert len(calls) == 1


def test_a_list_that_cannot_be_located_has_no_id(fresh_id: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken_list_path() -> str:
        raise RuntimeError('synthetic failure')

    monkeypatch.setattr(public_suffix_list, 'list_path', broken_list_path)

    assert public_suffix_list_id() is None
