# -*- coding: utf-8 -*-
import hashlib
import os
import os.path
import re
import threading

import requests

from api.config import GlobalConst as gc
from api.logger import set_log_account

_context = threading.local()


def set_cookie_account(username: str | None):
    account = str(username or "").strip()
    _context.username = account
    set_log_account(account)


def get_cookie_account() -> str:
    return getattr(_context, "username", "")


def _safe_account_name(username: str) -> str:
    clean = re.sub(r"[^0-9A-Za-z_.-]+", "_", username).strip("._")
    if clean:
        return clean[:64]
    return hashlib.sha1(username.encode("utf-8")).hexdigest()


def get_cookies_path(username: str | None = None) -> str:
    account = str(username or get_cookie_account() or "").strip()
    if not account:
        raise RuntimeError("缺少账号上下文，无法读取或保存账号级cookie")
    cookies_dir = gc.COOKIES_DIR
    os.makedirs(cookies_dir, exist_ok=True)
    return os.path.join(cookies_dir, f"{_safe_account_name(account)}.txt")


def save_cookies(session: requests.Session):
    buffer=""
    with open(get_cookies_path(), "w") as f:
        for k, v in session.cookies.items():
            buffer += f"{k}={v};"
        buffer = buffer.removesuffix(";")
        f.write(buffer)


def use_cookies() -> dict:
    cookies_path = get_cookies_path()
    if not os.path.exists(cookies_path):
        return {}

    cookies={}
    with open(cookies_path, "r") as f:
        buffer = f.read().strip()
        for item in buffer.split(";"):
            if not item.strip() or "=" not in item:
                continue
            k, v = item.strip().split("=", 1)
            cookies[k] = v

    return cookies
