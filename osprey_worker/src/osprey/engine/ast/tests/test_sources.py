"""Tests for carrying typed-action-contract schemas on the Sources payload.

The schema JSON rides on the same etcd ``Sources`` payload as the rules: a
``schemas/...json`` key space that survives ``from_path`` / ``to_dict`` /
``from_dict`` and so reaches both engines with no deployer/publisher/provider
change. These tests pin that round-trip and the hash behavior.
"""

import json

from osprey.engine.ast.grammar import Source
from osprey.engine.ast.sources import SOURCE_ENTRY_POINT_PATH, Sources

_MAIN_SML = '# main entry point\n'
_GUILD_SCHEMA = {
    'action': 'guild_joined',
    'version': 1,
    'provides': {'user': {'id': 'int'}},
    'absent': ['target_user'],
}
_USER_TYPE = {'id': 'int', 'username': 'str'}


def _write_tree(root) -> None:
    """Lay down main.sml + config.yaml + a couple schema files under ``root``."""
    (root / 'main.sml').write_text(_MAIN_SML)
    (root / 'config.yaml').write_text('sample_rate: 100\n')
    schemas_dir = root / 'schemas'
    (schemas_dir / 'types').mkdir(parents=True)
    (schemas_dir / 'guild_joined.json').write_text(json.dumps(_GUILD_SCHEMA))
    (schemas_dir / 'types' / 'user.json').write_text(json.dumps(_USER_TYPE))


class TestSourcesSchemas:
    def test_from_path_collects_schemas(self, tmp_path) -> None:
        _write_tree(tmp_path)
        sources = Sources.from_path(tmp_path)

        schemas = sources.schemas()
        assert 'schemas/guild_joined.json' in schemas
        assert 'schemas/types/user.json' in schemas
        assert json.loads(schemas['schemas/guild_joined.json']) == _GUILD_SCHEMA
        # Schema files must NOT leak into the .sml source collection.
        assert 'schemas/guild_joined.json' not in sources.paths()
        # get_schema accessor
        assert sources.get_schema('schemas/types/user.json') == json.dumps(_USER_TYPE)
        assert sources.get_schema('schemas/missing.json') is None

    def test_to_dict_from_dict_round_trips_schemas(self, tmp_path) -> None:
        _write_tree(tmp_path)
        sources = Sources.from_path(tmp_path)

        as_dict = sources.to_dict()
        # Schema keys present alongside main.sml / config.yaml
        assert 'schemas/guild_joined.json' in as_dict
        assert 'schemas/types/user.json' in as_dict
        assert SOURCE_ENTRY_POINT_PATH in as_dict

        rebuilt = Sources.from_dict(as_dict)
        assert rebuilt.schemas() == sources.schemas()
        # .sml sources preserved too
        assert rebuilt.paths() == sources.paths()

    def test_schema_keys_coexist_with_main_sml_and_config(self, tmp_path) -> None:
        _write_tree(tmp_path)
        sources = Sources.from_path(tmp_path)
        as_dict = sources.to_dict()
        # main.sml + config.yaml + 2 schema keys
        assert as_dict[SOURCE_ENTRY_POINT_PATH] == _MAIN_SML
        assert 'config.yaml' in as_dict
        assert sorted(k for k in as_dict if k.startswith('schemas/')) == [
            'schemas/guild_joined.json',
            'schemas/types/user.json',
        ]

    def test_back_compat_payload_without_schemas(self) -> None:
        """A legacy payload of only {'main.sml': ...} builds with no schemas and no raise."""
        sources = Sources.from_dict({SOURCE_ENTRY_POINT_PATH: _MAIN_SML})
        assert sources.schemas() == {}

    def test_payload_with_schema_key_does_not_hit_sml_assert(self) -> None:
        """A payload WITH a schemas/x.json key must build fine — schema keys never
        flow through ``add_source`` (which asserts ``.sml``)."""
        sources = Sources.from_dict(
            {
                SOURCE_ENTRY_POINT_PATH: _MAIN_SML,
                'schemas/guild_joined.json': json.dumps(_GUILD_SCHEMA),
            }
        )
        assert sources.schemas() == {'schemas/guild_joined.json': json.dumps(_GUILD_SCHEMA)}

    def test_to_dict_byte_identical_when_no_schemas(self) -> None:
        """Back-compat: to_dict is byte-identical with vs without the schemas map when
        ``_schemas`` is empty (schema keys only ever ADD to the dict via result.update).

        Use a payload with config.yaml so to_dict exercises its full body (a config-less
        Sources hits an unrelated pre-existing path in SourcesConfig.source).
        """
        base_payload = {SOURCE_ENTRY_POINT_PATH: _MAIN_SML, 'config.yaml': 'sample_rate: 100\n'}
        no_schemas = Sources.from_dict(dict(base_payload))
        empty_schemas = Sources.from_dict(dict(base_payload))
        # Force an explicit empty schemas map on the second one to prove parity.
        empty_schemas._schemas = {}
        assert no_schemas.to_dict() == empty_schemas.to_dict()
        assert no_schemas.to_dict()[SOURCE_ENTRY_POINT_PATH] == _MAIN_SML
        assert all(not k.startswith('schemas/') for k in no_schemas.to_dict())

    def test_hash_changes_when_schema_content_changes(self) -> None:
        """A schema-only edit (identical .sml) must invalidate the hash so the worker
        reloads and picks up new specialized graphs."""
        base = Sources.from_dict(
            {
                SOURCE_ENTRY_POINT_PATH: _MAIN_SML,
                'schemas/guild_joined.json': json.dumps(_GUILD_SCHEMA),
            }
        )
        changed_schema = dict(_GUILD_SCHEMA)
        changed_schema['absent'] = ['target_user', 'captcha_response']
        changed = Sources.from_dict(
            {
                SOURCE_ENTRY_POINT_PATH: _MAIN_SML,
                'schemas/guild_joined.json': json.dumps(changed_schema),
            }
        )
        assert base.hash() != changed.hash()

    def test_hash_stable_for_identical_schemas(self) -> None:
        payload = {
            SOURCE_ENTRY_POINT_PATH: _MAIN_SML,
            'schemas/guild_joined.json': json.dumps(_GUILD_SCHEMA),
        }
        assert Sources.from_dict(payload).hash() == Sources.from_dict(dict(payload)).hash()

    def test_hash_unchanged_when_no_schemas_vs_baseline(self) -> None:
        """Back-compat: adding the schemas map but leaving it empty must not change the
        hash of a schema-less Sources."""
        no_schemas = Sources({SOURCE_ENTRY_POINT_PATH: Source(path=SOURCE_ENTRY_POINT_PATH, contents=_MAIN_SML)})
        explicit_empty = Sources(
            {SOURCE_ENTRY_POINT_PATH: Source(path=SOURCE_ENTRY_POINT_PATH, contents=_MAIN_SML)},
            schemas={},
        )
        assert no_schemas.hash() == explicit_empty.hash()
