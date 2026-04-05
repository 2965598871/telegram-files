from __future__ import annotations

import re
from typing import Any


def phone_auth_settings() -> dict[str, Any]:
    return {
        "@type": "phoneNumberAuthenticationSettings",
        "allow_flash_call": False,
        "allow_missed_call": False,
        "is_current_phone_number": False,
        "allow_sms_retriever_api": False,
        "authentication_tokens": [],
    }


def build_tdlib_method_payload(
    method: str,
    params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if method == "SetAuthenticationPhoneNumber":
        phone_number = str(params.get("phoneNumber") or "").strip()
        if not phone_number:
            raise ValueError("phoneNumber is required")
        return (
            {
                "@type": "setAuthenticationPhoneNumber",
                "phone_number": phone_number,
                "settings": phone_auth_settings(),
            },
            {"phoneNumber": phone_number},
        )

    if method == "CheckAuthenticationCode":
        code_value = str(params.get("code") or "").strip()
        if not code_value:
            raise ValueError("code is required")
        return (
            {
                "@type": "checkAuthenticationCode",
                "code": code_value,
            },
            {},
        )

    if method == "CheckAuthenticationPassword":
        password_value = str(params.get("password") or "").strip()
        if not password_value:
            raise ValueError("password is required")
        return (
            {
                "@type": "checkAuthenticationPassword",
                "password": password_value,
            },
            {},
        )

    if method == "RequestQrCodeAuthentication":
        return (
            {
                "@type": "requestQrCodeAuthentication",
                "other_user_ids": [],
            },
            {},
        )

    raise ValueError(f"Unsupported auth method: {method}")


def _camel_to_snake(name: str) -> str:
    if not name:
        return ""
    if "_" in name:
        return name
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _tdlib_type_name(method: str) -> str:
    normalized = method.strip()
    if not normalized:
        return ""
    if normalized.startswith("@"):
        normalized = normalized[1:]
    return normalized[0].lower() + normalized[1:]


def _normalize_tdlib_payload(value: Any) -> Any:
    if isinstance(value, dict):
        payload: dict[str, Any] = {}
        for key, raw in value.items():
            normalized_key = str(key)
            if normalized_key == "@type":
                raw_type = str(raw or "").strip()
                payload[normalized_key] = (
                    _tdlib_type_name(raw_type) if raw_type else raw_type
                )
            else:
                payload[_camel_to_snake(normalized_key)] = _normalize_tdlib_payload(raw)
        return payload

    if isinstance(value, list):
        return [_normalize_tdlib_payload(item) for item in value]

    return value


def build_tdlib_generic_request(
    method: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {"@type": _tdlib_type_name(method)}
    for key, value in params.items():
        normalized_key = str(key)
        if normalized_key == "@type":
            continue
        payload[_camel_to_snake(normalized_key)] = _normalize_tdlib_payload(value)
    return payload
