"""本地配置：保存云文档 token(Cookie) 等。配置是唯一需要本地提供的东西。"""
import json
import os
import threading

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(HERE, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
DB_PATH = os.path.join(DATA_DIR, "meta.sqlite")

os.makedirs(DATA_DIR, exist_ok=True)

MB = 1024 * 1024

_DEFAULTS = {
    "cookie": "",            # 浏览器抓的完整 Cookie (含 wps_sid/kso_sid/csrf)
    "group_id": 0,           # 云文档 group id
    "root_name": "WpsNetDisk",  # 云端根目录名(跨设备据此发现)
    "chunk_size_mb": 128,    # 分片大小(MB), 单文件 kdocs 上限 2G
}

_lock = threading.Lock()


def load() -> dict:
    if not os.path.exists(CONFIG_PATH):
        return dict(_DEFAULTS)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    merged = dict(_DEFAULTS)
    merged.update(data or {})
    return merged


def normalize_cookie(raw: str) -> str:
    """把 Cookie 归一化成 HTTP 头用的 'name=value; name=value' 串。

    兼容三种输入:
      1) 原始请求头串: 'a=1; b=2'
      2) 浏览器扩展导出的 JSON 数组: [{"name":..,"value":..}, ...]
      3) 简单 JSON 对象: {"name":"value", ...}
    并去除换行等非法字符。
    """
    if not raw:
        return ""
    s = raw.strip()
    if s.startswith("[") or s.startswith("{"):
        try:
            obj = json.loads(s)
        except Exception:
            obj = None
        if isinstance(obj, list):
            pairs = [f'{c.get("name")}={c.get("value")}' for c in obj
                     if isinstance(c, dict) and c.get("name")]
            return "; ".join(pairs)
        if isinstance(obj, dict):
            return "; ".join(f"{k}={v}" for k, v in obj.items())
    # 原始串: 去掉换行/回车/首尾空白
    return " ".join(s.split("\n")).replace("\r", "").strip()


def save(patch: dict) -> dict:
    with _lock:
        cur = load()
        if "cookie" in patch and patch["cookie"]:
            patch = dict(patch)
            patch["cookie"] = normalize_cookie(patch["cookie"])
        for k in _DEFAULTS:
            if k in patch and patch[k] is not None:
                cur[k] = patch[k]
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=2)
        return cur


def parse_csrf(cookie: str) -> str:
    """从 Cookie 串里解析 csrf 值, 用作 csrfmiddlewaretoken。"""
    for part in (cookie or "").split(";"):
        part = part.strip()
        if part.startswith("csrf="):
            return part[len("csrf="):]
    return ""


def chunk_size_bytes(cfg: dict) -> int:
    return int(cfg.get("chunk_size_mb", 512)) * MB


def is_configured(cfg: dict) -> bool:
    # 只需 Cookie; group_id 会在初始化时自动发现并持久化
    return bool(cfg.get("cookie"))
