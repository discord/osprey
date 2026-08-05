"""Tests for grammar.py node behaviors that aren't covered by validator-level tests."""

from osprey.engine.ast.ast_utils import filter_nodes
from osprey.engine.ast.grammar import Call, Source


def _parse_call(contents: str) -> Call:
    source = Source(path='<test>', contents=contents)
    return next(iter(filter_nodes(source.ast_root, Call)))


class TestCallArgumentDict:
    def test_mutating_result_does_not_affect_later_calls(self) -> None:
        call = _parse_call('Foo = Bar(a=1, b=2)\n')

        argument_dict = call.argument_dict()
        argument_dict.clear()

        assert set(call.argument_dict()) == {'a', 'b'}

    def test_content_is_correct(self) -> None:
        call = _parse_call('Foo = Bar(a=1, b=2)\n')

        argument_dict = call.argument_dict()

        assert set(argument_dict) == {'a', 'b'}
        assert argument_dict['a'].value == 1  # type: ignore[attr-defined]
        assert argument_dict['b'].value == 2  # type: ignore[attr-defined]
