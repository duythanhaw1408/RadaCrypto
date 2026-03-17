from __future__ import annotations

import os
import requests

TELEGRAM_TIMEOUT_SECONDS = 15


def _build_credentials(token: str | None, chat_id: str | None) -> tuple[str, str]:
    resolved_token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    resolved_chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not resolved_token or not resolved_chat_id:
        raise ValueError("Thiếu cấu hình Telegram (TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID)")
    return resolved_token, resolved_chat_id


def send_text(
    message: str,
    token: str | None = None,
    chat_id: str | None = None,
    timeout_seconds: int = TELEGRAM_TIMEOUT_SECONDS,
) -> None:
    resolved_token, resolved_chat_id = _build_credentials(token=token, chat_id=chat_id)
    url = f"https://api.telegram.org/bot{resolved_token}/sendMessage"
    response = requests.post(
        url,
        json={"chat_id": resolved_chat_id, "text": message},
        timeout=timeout_seconds,
    )
    response.raise_for_status()


def try_send_text(
    message: str,
    token: str | None = None,
    chat_id: str | None = None,
    timeout_seconds: int = TELEGRAM_TIMEOUT_SECONDS,
) -> tuple[bool, str | None]:
    try:
        send_text(
            message=message,
            token=token,
            chat_id=chat_id,
            timeout_seconds=timeout_seconds,
        )
    except (requests.RequestException, ValueError) as exc:
        return False, str(exc)
    return True, None
