from __future__ import annotations

import ast
import re
from typing import Any


_FILTER_EXPR_CACHE: dict[str, Any] = {}

_FILTER_EXPR_ALLOWED_AST_NODES = {
    ast.Expression,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.Call,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.Attribute,
    ast.Subscript,
    ast.List,
    ast.Tuple,
    ast.Dict,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.USub,
    ast.UAdd,
    ast.Eq,
    ast.NotEq,
    ast.Gt,
    ast.GtE,
    ast.Lt,
    ast.LtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
}


def _int_or_default(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _camel_to_snake(name: str) -> str:
    if not name:
        return ""
    if "_" in name:
        return name
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _snake_to_camel(name: str) -> str:
    parts = [part for part in name.split("_") if part]
    if not parts:
        return name
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class _ExprObject:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)

        if name in self._payload:
            return _expr_wrap(self._payload[name])

        snake = _camel_to_snake(name)
        if snake in self._payload:
            return _expr_wrap(self._payload[snake])

        camel = _snake_to_camel(name)
        if camel in self._payload:
            return _expr_wrap(self._payload[camel])

        return None

    def __getitem__(self, key: str) -> Any:
        return self.__getattr__(str(key))


class _ExprFileRecord:
    def __init__(self, file_payload: dict[str, Any]) -> None:
        self._file_payload = file_payload

    def _get(self, key: str) -> Any:
        if key in self._file_payload:
            return self._file_payload[key]

        snake = _camel_to_snake(key)
        if snake in self._file_payload:
            return self._file_payload[snake]

        camel = _snake_to_camel(key)
        if camel in self._file_payload:
            return self._file_payload[camel]

        return None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)

        value = self._get(name)
        if value is None:
            raise AttributeError(name)
        return _ExprCallableValue(value)


class _ExprCallableValue:
    def __init__(self, value: Any) -> None:
        self._value = value

    def __call__(self) -> Any:
        return self._value

    def __repr__(self) -> str:
        return repr(self._value)

    def __str__(self) -> str:
        return str(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)

    def __int__(self) -> int:
        return _int_or_default(self._value, 0)

    def __float__(self) -> float:
        try:
            return float(self._value)
        except (TypeError, ValueError):
            return 0.0

    def _raw(self) -> Any:
        return self._value

    def __eq__(self, other: Any) -> bool:
        return self._value == other

    def __lt__(self, other: Any) -> bool:
        try:
            return self._value < other
        except Exception:
            return False

    def __le__(self, other: Any) -> bool:
        try:
            return self._value <= other
        except Exception:
            return False

    def __gt__(self, other: Any) -> bool:
        try:
            return self._value > other
        except Exception:
            return False

    def __ge__(self, other: Any) -> bool:
        try:
            return self._value >= other
        except Exception:
            return False


class _ExprStrNS:
    @staticmethod
    def contains(text: Any, fragment: Any) -> bool:
        return str(fragment or "") in str(text or "")

    @staticmethod
    def startWith(text: Any, prefix: Any) -> bool:
        return str(text or "").startswith(str(prefix or ""))

    @staticmethod
    def endWith(text: Any, suffix: Any) -> bool:
        return str(text or "").endswith(str(suffix or ""))

    @staticmethod
    def isBlank(text: Any) -> bool:
        return str(text or "").strip() == ""

    @staticmethod
    def toLowerCase(text: Any) -> str:
        return str(text or "").lower()


class _ExprReNS:
    @staticmethod
    def isMatch(pattern: Any, text: Any) -> bool:
        try:
            return re.search(str(pattern or ""), str(text or "")) is not None
        except re.error:
            return False


class _ExprNumNS:
    @staticmethod
    def between(value: Any, start: Any, end: Any) -> bool:
        try:
            current = float(value)
            left = float(start)
            right = float(end)
            return left <= current <= right
        except (TypeError, ValueError):
            return False


class _ExprCollNS:
    @staticmethod
    def isEmpty(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, _ExprObject):
            return len(value._payload) == 0
        try:
            return len(value) == 0  # type: ignore[arg-type]
        except Exception:
            return False

    @staticmethod
    def isNotEmpty(value: Any) -> bool:
        return not _ExprCollNS.isEmpty(value)


class _ExprArrayNS:
    @staticmethod
    def contains(values: Any, target: Any) -> bool:
        if isinstance(values, list):
            return target in values
        return False


class _ExprObjNS:
    @staticmethod
    def equals(a: Any, b: Any) -> bool:
        return a == b


def _expr_wrap(value: Any) -> Any:
    if isinstance(value, dict):
        return _ExprObject(value)
    if isinstance(value, list):
        return [_expr_wrap(item) for item in value]
    return value


def _message_expr_scope(message: dict[str, Any]) -> dict[str, Any]:
    scope: dict[str, Any] = {}

    for key, value in message.items():
        if key.startswith("@"):
            continue
        scope[key] = _expr_wrap(value)
        scope[_snake_to_camel(key)] = _expr_wrap(value)

    sender = message.get("sender_id")
    if isinstance(sender, dict):
        scope["senderId"] = _expr_wrap(sender)
        sender_user_id = _int_or_default(sender.get("user_id"), 0)
        if sender_user_id > 0:
            scope["senderUserId"] = sender_user_id

    return scope


def _normalize_filter_expr(expr: str) -> str:
    normalized = expr
    normalized = normalized.replace("&&", " and ")
    normalized = normalized.replace("||", " or ")
    normalized = re.sub(r"(?<![=!<>])!(?!=)", " not ", normalized)
    normalized = re.sub(r"\btrue\b", "True", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bfalse\b", "False", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bnull\b", "None", normalized, flags=re.IGNORECASE)
    normalized = re.sub(
        r"\b(str|re|num|coll|array|obj):([A-Za-z_][A-Za-z0-9_]*)",
        r"\1.\2",
        normalized,
    )
    return normalized


def _compile_filter_expr(expr: str) -> Any:
    normalized = _normalize_filter_expr(expr)
    cached = _FILTER_EXPR_CACHE.get(normalized)
    if cached is not None:
        return cached

    parsed = ast.parse(normalized, mode="eval")
    for node in ast.walk(parsed):
        if type(node) not in _FILTER_EXPR_ALLOWED_AST_NODES:
            raise ValueError(f"Unsupported expression element: {type(node).__name__}")
        if isinstance(node, ast.Attribute) and str(node.attr).startswith("__"):
            raise ValueError("Dunder attributes are not allowed")
        if isinstance(node, ast.Name) and str(node.id).startswith("__"):
            raise ValueError("Dunder names are not allowed")

    code = compile(parsed, "<filterExpr>", "eval")
    _FILTER_EXPR_CACHE[normalized] = code
    return code


def evaluate_filter_expr(
    filter_expr: str,
    *,
    file_payload: dict[str, Any],
    message: dict[str, Any],
) -> bool:
    expr_text = filter_expr.strip()
    if not expr_text:
        return True

    try:
        code = _compile_filter_expr(expr_text)
    except Exception:
        return False

    scope = _message_expr_scope(message)
    eval_globals = {
        "__builtins__": {},
        "str": _ExprStrNS,
        "re": _ExprReNS,
        "num": _ExprNumNS,
        "coll": _ExprCollNS,
        "array": _ExprArrayNS,
        "obj": _ExprObjNS,
    }
    eval_locals = dict(scope)
    eval_locals["f"] = _ExprFileRecord(file_payload)

    try:
        result = eval(code, eval_globals, eval_locals)
    except Exception:
        return False

    return bool(result)
