from typing import ClassVar


class JsonExtractUDFMixin:
    """Marker for UDFs whose Arguments include a path: ConstExpr[str]
    that reads a value from action data. New json-extraction UDFs
    inherit this mixin (or set `extracts_json_path: ClassVar[bool] = True`)
    and are picked up by CollectJsonDataPaths with no collector edit."""

    extracts_json_path: ClassVar[bool] = True
