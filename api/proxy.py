# -*- coding: utf-8 -*-
import configparser
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import requests


@dataclass(frozen=True)
class ProxyEntry:
    host: str
    port: str
    username: str
    password: str
    expire_date: str = ""
    region: str = ""
    scheme: str = "socks5h"

    @property
    def url(self) -> str:
        user = quote(self.username, safe="")
        password = quote(self.password, safe="")
        return f"{self.scheme}://{user}:{password}@{self.host}:{self.port}"

    @property
    def proxies(self) -> dict[str, str]:
        return {"http": self.url, "https": self.url}

    @property
    def label(self) -> str:
        suffix = f" {self.region}" if self.region else ""
        return f"{self.host}:{self.port}{suffix}"

    def is_expired(self) -> bool:
        if not self.expire_date:
            return False
        try:
            return datetime.strptime(self.expire_date, "%Y-%m-%d").date() < date.today()
        except ValueError:
            return False


_current_proxy: ProxyEntry | None = None
_request_timeout = 5.0


def set_current_proxy(proxy: ProxyEntry | None) -> None:
    global _current_proxy
    _current_proxy = proxy


def get_current_proxy() -> ProxyEntry | None:
    return _current_proxy


def set_request_timeout(timeout: float) -> None:
    global _request_timeout
    _request_timeout = max(float(timeout), 1.0)


def get_request_timeout(default: float | None = None) -> float:
    if default is not None and _request_timeout <= 0:
        return default
    return _request_timeout


def apply_proxy_runtime_config(proxy_config: dict[str, Any]) -> None:
    if "request_timeout" in proxy_config:
        set_request_timeout(float(proxy_config["request_timeout"]))


def apply_current_proxy(session) -> None:
    proxy = get_current_proxy()
    session.trust_env = False
    if proxy:
        session.proxies.clear()
        session.proxies.update(proxy.proxies)


def parse_proxy_line(line: str, scheme: str) -> ProxyEntry | None:
    line = line.strip()
    if not line or line.startswith("#") or line.startswith(";"):
        return None
    parts = [item.strip() for item in line.split("|")]
    if len(parts) < 4:
        raise ValueError(f"代理格式错误: {line}")
    return ProxyEntry(
        host=parts[0],
        port=parts[1],
        username=parts[2],
        password=parts[3],
        expire_date=parts[4] if len(parts) > 4 else "",
        region=parts[5] if len(parts) > 5 else "",
        scheme=scheme,
    )


def load_proxy_entries(file_path: str, scheme: str) -> list[ProxyEntry]:
    if not os.path.exists(file_path):
        raise RuntimeError(f"代理文件不存在: {file_path}")
    entries: list[ProxyEntry] = []
    with open(file_path, "r", encoding="utf8") as file:
        for line in file:
            entry = parse_proxy_line(line, scheme)
            if entry and not entry.is_expired():
                entries.append(entry)
    return entries


def load_proxy_config(config_path: str, concurrency: int) -> dict[str, Any]:
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf8")
    if not config.has_section("proxy"):
        return {"enabled": False, "entries": [], "strict": True}

    section = dict(config.items("proxy"))
    enabled = str(section.get("enabled", "false")).strip().lower() in {"1", "true", "yes", "y", "on"}
    if not enabled:
        return {"enabled": False, "entries": [], "strict": True}

    try:
        import socks  # noqa: F401
    except ImportError as exc:
        raise RuntimeError('启用SOCKS代理需要先安装依赖: pip install PySocks') from exc

    scheme = section.get("type", section.get("scheme", "socks5h")).strip() or "socks5h"
    if scheme not in {"socks5", "socks5h", "http", "https"}:
        raise RuntimeError(f"不支持的代理类型: {scheme}")

    proxy_file = section.get("file", "proxies.txt").strip() or "proxies.txt"
    if not os.path.isabs(proxy_file):
        proxy_file = os.path.join(os.path.dirname(os.path.abspath(config_path)), proxy_file)

    entries = load_proxy_entries(proxy_file, scheme)
    strict = str(section.get("strict", "true")).strip().lower() in {"1", "true", "yes", "y", "on"}
    if not entries:
        raise RuntimeError("代理已启用，但没有可用代理")
    if strict and len(entries) < concurrency:
        raise RuntimeError(f"代理数量不足: concurrency={concurrency}, 可用代理={len(entries)}")

    return {
        "enabled": True,
        "entries": entries,
        "strict": strict,
        "mode": section.get("mode", "slot").strip() or "slot",
        "request_timeout": float(section.get("request_timeout", 15)),
        "health_check": str(section.get("health_check", "true")).strip().lower() in {"1", "true", "yes", "y", "on"},
        "health_check_timeout": float(section.get("health_check_timeout", section.get("request_timeout", 15))),
        "health_check_urls": [
            item.strip()
            for item in section.get(
                "health_check_urls",
                "https://mooc2-ans.chaoxing.com,https://mooc1.chaoxing.com",
            ).split(",")
            if item.strip()
        ],
        "network_error_backoff": int(section.get("network_error_backoff", 180)),
    }


def proxy_for_slot(proxy_config: dict[str, Any], slot: int) -> ProxyEntry | None:
    if not proxy_config.get("enabled"):
        return None
    entries: list[ProxyEntry] = proxy_config["entries"]
    index = slot - 1
    if index < len(entries):
        return entries[index]
    if proxy_config.get("strict", True):
        raise RuntimeError(f"slot={slot} 没有可用代理")
    return entries[index % len(entries)]


def check_proxy_health(proxy: ProxyEntry, urls: list[str], timeout: float) -> None:
    session = requests.Session()
    session.trust_env = False
    session.proxies.update(proxy.proxies)
    last_error: Exception | None = None
    for url in urls:
        started_at = time.time()
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code < 500:
                return
            last_error = RuntimeError(f"{url} HTTP {response.status_code}")
        except requests.RequestException as exc:
            last_error = exc
        elapsed = time.time() - started_at
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
    raise RuntimeError(f"代理健康检查失败: {proxy.label}: {last_error}")
