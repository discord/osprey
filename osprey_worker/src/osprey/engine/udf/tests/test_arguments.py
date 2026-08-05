from typing import Optional, Union

import pytest
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Expression, Source
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr

StrConstExpr = ConstExpr[str]  # This being inside the below function is causing mypy to crash


def _parse_call(contents: str) -> Call:
    source = Source(path='<test>', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Call)))


def _record_argument_dict_calls(monkeypatch: pytest.MonkeyPatch) -> list[Call]:
    calls: list[Call] = []
    original_argument_dict = Call.argument_dict

    def recording_argument_dict(call: Call) -> dict[str, Expression]:
        calls.append(call)
        return original_argument_dict(call)

    monkeypatch.setattr(Call, 'argument_dict', recording_argument_dict)
    return calls


def test_arguments_items() -> None:
    class Arguments(ArgumentsBase):
        foo: str
        bar: StrConstExpr

    items = Arguments.items()

    assert list(items) == ['foo', 'bar']
    assert items['foo'] is str
    assert items['bar'] is StrConstExpr


def test_arguments_items_cache_is_scoped_to_each_subclass() -> None:
    class FirstArguments(ArgumentsBase):
        first: str

    class SecondArguments(ArgumentsBase):
        second: int

    first_items = FirstArguments.items()
    SecondArguments.items()

    assert FirstArguments.items() is first_items


def test_resolved_arguments_reuse_unresolved_argument_ast(monkeypatch: pytest.MonkeyPatch) -> None:
    class Arguments(ArgumentsBase):
        foo: int

    argument_dict_calls = _record_argument_dict_calls(monkeypatch)
    call = _parse_call('Foo = Bar(foo=1)\n')

    unresolved = Arguments(call_node=call, arguments={'foo': 1})
    resolved = unresolved.update_with_resolved({'foo': 2})

    assert argument_dict_calls == [call]
    assert resolved._arguments_ast is unresolved._arguments_ast


def test_resolved_arguments_support_legacy_custom_init(monkeypatch: pytest.MonkeyPatch) -> None:
    class Arguments(ArgumentsBase):
        foo: int

        def __init__(self, call_node: Call, arguments: dict[str, object], resolved: bool = False):
            super().__init__(call_node=call_node, arguments=arguments, resolved=resolved)

    argument_dict_calls = _record_argument_dict_calls(monkeypatch)
    call = _parse_call('Foo = Bar(foo=1)\n')
    unresolved = Arguments(call_node=call, arguments={'foo': 1})

    resolved = unresolved.update_with_resolved({'foo': 2})

    assert resolved.foo == 2
    assert argument_dict_calls == [call]
    assert resolved._arguments_ast is unresolved._arguments_ast


def test_resolved_arguments_support_legacy_custom_new(monkeypatch: pytest.MonkeyPatch) -> None:
    class Arguments(ArgumentsBase):
        foo: int

        def __new__(cls, call_node: Call, arguments: dict[str, object], resolved: bool = False) -> 'Arguments':
            return super().__new__(cls)

    argument_dict_calls = _record_argument_dict_calls(monkeypatch)
    call = _parse_call('Foo = Bar(foo=1)\n')
    unresolved = Arguments(call_node=call, arguments={'foo': 1})

    resolved = unresolved.update_with_resolved({'foo': 2})

    assert resolved.foo == 2
    assert argument_dict_calls == [call]
    assert resolved._arguments_ast is unresolved._arguments_ast


def test_resolved_arguments_support_legacy_custom_metaclass(monkeypatch: pytest.MonkeyPatch) -> None:
    class LegacyMeta(type):
        def __call__(cls, call_node: Call, arguments: dict[str, object], resolved: bool = False) -> 'Arguments':
            return super().__call__(call_node=call_node, arguments=arguments, resolved=resolved)

    class Arguments(ArgumentsBase, metaclass=LegacyMeta):
        foo: int

    argument_dict_calls = _record_argument_dict_calls(monkeypatch)
    call = _parse_call('Foo = Bar(foo=1)\n')
    unresolved = Arguments(call_node=call, arguments={'foo': 1})

    resolved = unresolved.update_with_resolved({'foo': 2})

    assert resolved.foo == 2
    assert argument_dict_calls == [call]
    assert resolved._arguments_ast is unresolved._arguments_ast


def test_resolved_arguments_propagate_type_error_from_custom_init() -> None:
    class Arguments(ArgumentsBase):
        foo: int

        def __init__(self, call_node: Call, arguments: dict[str, object], resolved: bool = False):
            if resolved:
                raise TypeError('raised inside custom constructor')
            super().__init__(call_node=call_node, arguments=arguments, resolved=resolved)

    call = _parse_call('Foo = Bar(foo=1)\n')
    unresolved = Arguments(call_node=call, arguments={'foo': 1})

    with pytest.raises(TypeError, match='raised inside custom constructor'):
        unresolved.update_with_resolved({'foo': 2})


def test_arguments_can_be_none() -> None:
    class Arguments(ArgumentsBase):
        optional: Optional[str]
        union: Union[str, int, None]
        none: None
        obj: object
        string: str
        integer: int

    assert Arguments.kwarg_can_be_none('optional')
    assert Arguments.kwarg_can_be_none('union')
    assert Arguments.kwarg_can_be_none('none')
    assert Arguments.kwarg_can_be_none('obj')
    assert not Arguments.kwarg_can_be_none('string')
    assert not Arguments.kwarg_can_be_none('integer')


def test_arguments_can_be_none_is_cached() -> None:
    class Arguments(ArgumentsBase):
        optional: Optional[str]
        string: str

    hits_before = Arguments.kwarg_can_be_none.cache_info().hits

    assert Arguments.kwarg_can_be_none('optional') is True
    assert Arguments.kwarg_can_be_none('string') is False

    # Repeat the same calls; both should now be served from the cache.
    assert Arguments.kwarg_can_be_none('optional') is True
    assert Arguments.kwarg_can_be_none('string') is False

    assert Arguments.kwarg_can_be_none.cache_info().hits == hits_before + 2
