from typing import Optional, Union

import pytest
from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Expression, Source
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr

StrConstExpr = ConstExpr[str]  # This being inside the below function is causing mypy to crash


def _parse_call(contents: str) -> Call:
    source = Source(path='<test>', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Call)))


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

    argument_dict_calls = 0
    original_argument_dict = Call.argument_dict

    def counting_argument_dict(call: Call) -> dict[str, Expression]:
        nonlocal argument_dict_calls
        argument_dict_calls += 1
        return original_argument_dict(call)

    monkeypatch.setattr(Call, 'argument_dict', counting_argument_dict)
    call = _parse_call('Foo = Bar(foo=1)\n')

    unresolved = Arguments(call_node=call, arguments={'foo': 1})
    unresolved.update_with_resolved({'foo': 2})

    assert argument_dict_calls == 1


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
