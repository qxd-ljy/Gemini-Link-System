import json
import time
import hmac
import hashlib
import base64
import os
import asyncio
import uuid
import ssl
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union, Dict, Any
from dataclasses import dataclass, field
from pathlib import Path
import logging

import httpx
from fastapi import FastAPI, HTTPException, Request, Depends, WebSocket, WebSocketDisconnect  # noqa: F401
from contextlib import asynccontextmanager
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from sqlalchemy.orm import Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess
import sys
import signal

from database import init_db, get_db, Admin, APIKey, APICallLog, KeepAliveTask, KeepAliveLog, KeepAliveAccountLog, AccountCookieStatus
from auth import (
    hash_password, verify_password, create_access_token, 
    generate_api_key, hash_api_key, get_current_admin, init_admin,
    encrypt_api_key, decrypt_api_key
)


# ---------- 本地 .env 加载（便于直接 python main.py 运行） ----------
def _load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                # 去除引号（支持单引号和双引号）
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        # 本地加载失败直接忽略，保持与原环境一致
        pass


_load_env_file()


# ---------- 日志配置 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gemini")

# ---------- 时区配置 ----------
# 北京时间 UTC+8
BEIJING_TZ = timezone(timedelta(hours=8))

def get_beijing_time():
    """获取当前北京时间"""
    return datetime.now(BEIJING_TZ)

def ensure_aware(dt: datetime) -> datetime:
    """确保 datetime 是 aware（有时区信息）"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # 如果是 naive datetime，假设它是北京时间并添加时区信息
        return dt.replace(tzinfo=BEIJING_TZ)
    return dt

def ensure_naive(dt: datetime) -> datetime:
    """确保 datetime 是 naive（无时区信息），转换为北京时间后移除时区"""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        # 转换为北京时间后移除时区信息
        return dt.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return dt

# ---------- 配置 ----------
SECURE_C_SES = os.getenv("SECURE_C_SES")
HOST_C_OSES = os.getenv("HOST_C_OSES")
CSESIDX = os.getenv("CSESIDX")
CONFIG_ID = os.getenv("CONFIG_ID")
PROXY_RAW = os.getenv("PROXY") or None
# 去除代理配置中可能存在的引号
if PROXY_RAW:
    PROXY_RAW = PROXY_RAW.strip().strip('"').strip("'")
    PROXY = PROXY_RAW if PROXY_RAW else None
else:
    PROXY = None
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "600"))

# ---------- 图片生成相关常量 ----------
BASE_DIR = Path(__file__).resolve().parent
IMAGE_SAVE_DIR = BASE_DIR / "generated_images"
LIST_FILE_METADATA_URL = "https://biz-discoveryengine.googleapis.com/v1alpha/locations/global/widgetListSessionFileMetadata"

# ---------- 图片数据类 ----------
@dataclass
class ChatImage:
    """表示生成的图片"""
    file_id: Optional[str] = None
    file_name: Optional[str] = None
    base64_data: Optional[str] = None
    url: Optional[str] = None
    local_path: Optional[str] = None
    mime_type: str = "image/png"

    def save_to_file(self, directory: Optional[Path] = None) -> str:
        """保存图片到本地文件，返回文件路径"""
        if self.local_path and os.path.exists(self.local_path):
            return self.local_path

        save_dir = directory or IMAGE_SAVE_DIR
        os.makedirs(save_dir, exist_ok=True)

        ext = ".png"
        if self.mime_type:
            ext_map = {
                "image/png": ".png",
                "image/jpeg": ".jpg",
                "image/gif": ".gif",
                "image/webp": ".webp",
            }
            ext = ext_map.get(self.mime_type, ".png")

        if self.file_name:
            filename = self.file_name
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gemini_{timestamp}_{uuid.uuid4().hex[:8]}{ext}"

        filepath = os.path.join(save_dir, filename)

        if self.base64_data:
            image_data = base64.b64decode(self.base64_data)
            with open(filepath, "wb") as f:
                f.write(image_data)
            self.local_path = filepath

        return filepath

# ---------- 模型映射配置 ----------
MODEL_MAPPING: Dict[str, Optional[str]] = {
    "gemini-auto": None,
    "gemini-2.5-flash": "gemini-2.5-flash",
    "gemini-2.5-pro": "gemini-2.5-pro",
    "gemini-3-pro-preview": "gemini-3-pro-preview"
}

# ---------- 全局 Session 缓存 ----------
# key: conversation_key -> {"session_id": str, "updated_at": float, "account": str}
SESSION_CACHE: Dict[str, Dict[str, Any]] = {}

# ---------- WebSocket 管理 ----------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

# ---------- HTTP 客户端 ----------
# 记录代理配置信息
if PROXY:
    logger.info(f"🌐 代理配置: {PROXY}")
else:
    logger.info("🌐 未配置代理，使用直连")
http_client = httpx.AsyncClient(
    proxy=PROXY,
    verify=False,
    http2=False,
    timeout=httpx.Timeout(TIMEOUT_SECONDS, connect=60.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
)


# ---------- 通用工具函数 ----------
def get_common_headers(jwt: str) -> dict:
    return {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "authorization": f"Bearer {jwt}",
        "content-type": "application/json",
        "origin": "https://business.gemini.google",
        "referer": "https://business.gemini.google/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "x-server-timeout": "1800",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
    }


def urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def kq_encode(s: str) -> str:
    b = bytearray()
    for ch in s:
        v = ord(ch)
        if v > 255:
            b.append(v & 255)
            b.append(v >> 8)
        else:
            b.append(v)
    return urlsafe_b64encode(bytes(b))


def create_jwt(key_bytes: bytes, key_id: str, csesidx: str) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT", "kid": key_id}
    payload = {
        "iss": "https://business.gemini.google",
        "aud": "https://biz-discoveryengine.googleapis.com",
        "sub": f"csesidx/{csesidx}",
        "iat": now,
        "exp": now + 300,
        "nbf": now,
    }
    header_b64 = kq_encode(json.dumps(header, separators=(",", ":")))
    payload_b64 = kq_encode(json.dumps(payload, separators=(",", ":")))
    message = f"{header_b64}.{payload_b64}"
    sig = hmac.new(key_bytes, message.encode(), hashlib.sha256).digest()
    return f"{message}.{urlsafe_b64encode(sig)}"


# ---------- JWT 与账号管理 ----------
class JWTManager:
    def __init__(self, account: "Account") -> None:
        self.account = account
        self.jwt: str = ""
        self.expires: float = 0
        self._lock = asyncio.Lock()

    async def get(self) -> str:
        async with self._lock:
            if time.time() > self.expires:
                await self._refresh()
            return self.jwt

    async def _refresh(self) -> None:
        cookie = f"__Secure-C_SES={self.account.secure_c_ses}"
        if self.account.host_c_oses:
            cookie += f"; __Host-C_OSES={self.account.host_c_oses}"

        logger.debug(f"🔑 正在刷新 JWT... 账号={self.account.name}")
        r = await http_client.get(
            "https://business.gemini.google/auth/getoxsrf",
            params={"csesidx": self.account.csesidx},
            headers={
                "cookie": cookie,
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/140.0.0.0 Safari/537.36"
                ),
                "referer": "https://business.gemini.google/",
            },
        )
        if r.status_code != 200:
            logger.error(
                f"getoxsrf 失败 [{self.account.name}]: {r.status_code} {r.text}"
            )
            if r.status_code in (401, 403, 429):
                self.account.mark_quota_error(r.status_code, r.text)
            raise HTTPException(r.status_code, "getoxsrf failed")

        # 尝试从响应头中解析 Cookie 过期时间
        cookie_expires_at = None
        try:
            # 检查响应头中是否有 Set-Cookie
            logger.debug(f"检查响应头中的 Set-Cookie...")
            logger.debug(f"所有响应头: {dict(r.headers)}")
            
            # httpx 使用 get_list 或 getall 获取所有同名头部
            set_cookie_headers = []
            if hasattr(r.headers, 'get_list'):
                set_cookie_headers = r.headers.get_list("set-cookie", [])
            elif hasattr(r.headers, 'getall'):
                set_cookie_headers = r.headers.getall("set-cookie", [])
            else:
                # 如果没有这些方法，尝试直接获取
                set_cookie_header = r.headers.get("set-cookie")
                if set_cookie_header:
                    set_cookie_headers = [set_cookie_header]
            
            logger.debug(f"找到 {len(set_cookie_headers)} 个 Set-Cookie 头")
            
            if set_cookie_headers:
                from http.cookies import SimpleCookie
                from database import get_beijing_time
                for set_cookie in set_cookie_headers:
                    try:
                        # 解析 Set-Cookie 头
                        cookie_obj = SimpleCookie()
                        cookie_obj.load(set_cookie)
                        for cookie_name, cookie_attrs in cookie_obj.items():
                            if cookie_name in ("__Secure-C_SES", "__Host-C_OSES"):
                                # 检查 Expires 属性
                                if "expires" in cookie_attrs:
                                    expires_str = cookie_attrs["expires"]
                                    try:
                                        # 解析 RFC 1123 格式的日期（GMT/UTC）
                                        # 注意：需要处理时区，转换为北京时间（naive）
                                        from email.utils import parsedate_to_datetime
                                        expires_dt = parsedate_to_datetime(expires_str)
                                        # 转换为北京时间（naive）
                                        if expires_dt.tzinfo:
                                            expires_dt = expires_dt.astimezone(timezone(timedelta(hours=8)))
                                            cookie_expires_at = expires_dt.replace(tzinfo=None)
                                        else:
                                            cookie_expires_at = expires_dt
                                        logger.debug(f"从 Set-Cookie 解析到过期时间: {cookie_expires_at}")
                                        break
                                    except (ValueError, TypeError) as e:
                                        logger.debug(f"解析 Expires 失败: {e}")
                                # 检查 Max-Age 属性
                                elif "max-age" in cookie_attrs:
                                    try:
                                        max_age = int(cookie_attrs["max-age"])
                                        # 使用北京时间
                                        cookie_expires_at = get_beijing_time() + timedelta(seconds=max_age)
                                        logger.debug(f"从 Max-Age 计算过期时间: {cookie_expires_at}")
                                        break
                                    except (ValueError, TypeError) as e:
                                        logger.debug(f"解析 Max-Age 失败: {e}")
                    except Exception as e:
                        logger.debug(f"解析 Set-Cookie 失败: {e}")
            else:
                logger.debug(f"响应头中没有 Set-Cookie，Google API 可能不会在每次请求时返回 Cookie 过期信息")
        except Exception as e:
            logger.debug(f"获取 Set-Cookie 响应头失败: {e}")
        
        # 如果无法从响应头获取，保存到 account 对象供后续使用
        if cookie_expires_at:
            self.account._cookie_expires_at = cookie_expires_at
            logger.info(f"✅ 成功获取 Cookie 过期时间 [{self.account.name}]: {cookie_expires_at}")
        else:
            logger.debug(f"⚠️ 无法从响应头获取 Cookie 过期时间 [{self.account.name}]")

        txt = r.text[4:] if r.text.startswith(")]}'") else r.text
        data = json.loads(txt)

        key_bytes = base64.urlsafe_b64decode(data["xsrfToken"] + "==")
        self.jwt = create_jwt(key_bytes, data["keyId"], self.account.csesidx)
        self.expires = time.time() + 270
        logger.info(f"JWT 刷新成功 [{self.account.name}]")


class Account:
    def __init__(
        self,
        name: str,
        secure_c_ses: str,
        csesidx: str,
        config_id: str,
        host_c_oses: Optional[str] = None,
    ) -> None:
        self.name = name
        self.secure_c_ses = secure_c_ses
        self.host_c_oses = host_c_oses
        self.csesidx = csesidx
        self.config_id = config_id
        self.jwt_mgr = JWTManager(self)
        self._cookie_expires_at = None  # Cookie 过期时间（如果可获取）
        self.disabled_until: float = 0.0

    def is_available(self) -> bool:
        return time.time() >= self.disabled_until

    def mark_quota_error(self, status_code: int, detail: str = "") -> None:
        cooldown_seconds = 300  # 暂停 5 分钟
        self.disabled_until = max(self.disabled_until, time.time() + cooldown_seconds)
        logger.warning(f"账号[{self.name}] 暂时标记为不可用 (status={status_code})")
        if detail:
            logger.debug(f"账号[{self.name}] 错误详情: {detail[:200]}")


class AccountPool:
    def __init__(self, accounts: List[Account]) -> None:
        if not accounts:
            raise RuntimeError("No Gemini business accounts configured")
        self.accounts = accounts
        self._rr_index = 0

    def _next_round_robin(self) -> Account:
        n = len(self.accounts)
        for _ in range(n):
            acc = self.accounts[self._rr_index % n]
            self._rr_index = (self._rr_index + 1) % n
            if acc.is_available():
                return acc
        # 如果都被标记为不可用，仍然返回一个账号避免完全瘫痪
        return self.accounts[0]

    def get_for_conversation(self, conv_key: str) -> Account:
        cached = SESSION_CACHE.get(conv_key)
        if cached:
            acc_name = cached.get("account")
            for acc in self.accounts:
                if acc.name == acc_name and acc.is_available():
                    return acc
        # 没有缓存或账号不可用，走轮询
        return self._next_round_robin()

    def get_alternative(self, exclude_name: str) -> Optional[Account]:
        for acc in self.accounts:
            if acc.name != exclude_name and acc.is_available():
                return acc
        return None


def load_accounts_from_env() -> List[Account]:
    accounts: List[Account] = []

    # 支持 ACCOUNT1_*, ACCOUNT2_*... 多账号配置
    account_indices = set()
    for key in os.environ.keys():
        if key.startswith("ACCOUNT") and key.endswith("_SECURE_C_SES"):
            idx_str = key[len("ACCOUNT") : -len("_SECURE_C_SES")]
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            account_indices.add(idx)

    for idx in sorted(account_indices):
        prefix = f"ACCOUNT{idx}_"
        secure = os.getenv(prefix + "SECURE_C_SES")
        csesidx = os.getenv(prefix + "CSESIDX")
        config_id = os.getenv(prefix + "CONFIG_ID")
        host = os.getenv(prefix + "HOST_C_OSES")
        if not (secure and csesidx and config_id):
            logger.warning(f"账号索引 {idx} 配置不完整，已跳过")
            continue
        # 去除可能存在的引号（双重保险）
        secure = secure.strip().strip('"').strip("'") if secure else None
        csesidx = csesidx.strip().strip('"').strip("'") if csesidx else None
        config_id = config_id.strip().strip('"').strip("'") if config_id else None
        # 去除 config_id 中可能存在的 ?csesidx 后缀
        if config_id and '?csesidx' in config_id:
            config_id = config_id.split('?csesidx')[0]
        host = host.strip().strip('"').strip("'") if host else None
        name = os.getenv(prefix + "NAME") or f"account-{idx}"
        if name:
            name = name.strip().strip('"').strip("'")
        accounts.append(
            Account(
                name=name,
                secure_c_ses=secure,
                csesidx=csesidx,
                config_id=config_id,
                host_c_oses=host,
            )
        )

    # 兼容旧的单账号环境变量
    if not accounts and SECURE_C_SES and CSESIDX and CONFIG_ID:
        # 去除可能存在的引号
        secure_c_ses = SECURE_C_SES.strip().strip('"').strip("'") if SECURE_C_SES else None
        csesidx = CSESIDX.strip().strip('"').strip("'") if CSESIDX else None
        config_id = CONFIG_ID.strip().strip('"').strip("'") if CONFIG_ID else None
        # 去除 config_id 中可能存在的 ?csesidx 后缀
        if config_id and '?csesidx' in config_id:
            config_id = config_id.split('?csesidx')[0]
        host_c_oses = HOST_C_OSES.strip().strip('"').strip("'") if HOST_C_OSES else None
        accounts.append(
            Account(
                name="default",
                secure_c_ses=secure_c_ses,
                csesidx=csesidx,
                config_id=config_id,
                host_c_oses=host_c_oses,
            )
        )

    return accounts


ACCOUNTS: List[Account] = load_accounts_from_env()
ACCOUNT_POOL: Optional["AccountPool"]
if ACCOUNTS:
    ACCOUNT_POOL = AccountPool(ACCOUNTS)
else:
    ACCOUNT_POOL = None


def reload_accounts_from_env_file() -> None:
    """从 .env 文件重新加载账号配置（动态重载，无需重启）"""
    global ACCOUNTS, ACCOUNT_POOL, PROXY
    
    # 重新读取 .env 文件并更新环境变量
    lines = read_env_file()
    for line_data in lines:
        line = line_data["raw"].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        # 去掉首尾的引号（支持单引号和双引号）
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        # 去除 config_id 中可能存在的 ?csesidx 后缀
        if key.endswith("_CONFIG_ID") or key == "CONFIG_ID":
            if '?csesidx' in value:
                value = value.split('?csesidx')[0]
        # 更新环境变量
        os.environ[key] = value
        # 如果更新了 PROXY，同步更新全局变量
        if key == "PROXY":
            PROXY_RAW = value.strip().strip('"').strip("'") if value else None
            PROXY = PROXY_RAW if PROXY_RAW else None
            if PROXY:
                logger.info(f"🔄 代理配置已更新: {PROXY}")
            else:
                logger.info("🔄 代理配置已清除")
    
    # 重新加载账号
    ACCOUNTS = load_accounts_from_env()
    
    # 重新创建账号池
    if ACCOUNTS:
        ACCOUNT_POOL = AccountPool(ACCOUNTS)
        logger.info(f"🔄 账号配置已重新加载，共 {len(ACCOUNTS)} 个账号")
    else:
        ACCOUNT_POOL = None
        logger.warning("⚠️ 重新加载后没有可用账号")


# ---------- Session & File 管理 ----------
async def create_google_session(account: Account) -> str:
    jwt = await account.jwt_mgr.get()
    headers = get_common_headers(jwt)
    body = {
        "configId": account.config_id,
        "additionalParams": {"token": "-"},
        "createSessionRequest": {"session": {"name": "", "displayName": ""}},
    }

    logger.info(f"🌐 申请 Session... 账号={account.name}, configId={account.config_id}")
    r = await http_client.post(
        "https://biz-discoveryengine.googleapis.com/v1alpha/locations/global/widgetCreateSession",
        headers=headers,
        json=body,
    )
    if r.status_code != 200:
        logger.error(
            f"createSession 失败 [{account.name}]: {r.status_code}, configId={account.config_id}, 响应={r.text}"
        )
        if r.status_code in (401, 403, 429):
            account.mark_quota_error(r.status_code, r.text)
        raise HTTPException(r.status_code, "createSession failed")
    sess_name = r.json()["session"]["name"]
    return sess_name


async def upload_context_file(
    account: Account, session_name: str, mime_type: str, base64_content: str
) -> str:
    """上传文件到指定 Session，返回 fileId"""
    jwt = await account.jwt_mgr.get()
    headers = get_common_headers(jwt)

    ext = mime_type.split("/")[-1] if "/" in mime_type else "bin"
    file_name = f"upload_{int(time.time())}_{uuid.uuid4().hex[:6]}.{ext}"

    body = {
        "configId": account.config_id,
        "additionalParams": {"token": "-"},
        "addContextFileRequest": {
            "name": session_name,
            "fileName": file_name,
            "mimeType": mime_type,
            "fileContents": base64_content,
        },
    }

    logger.info(f"📤 上传图片 [{mime_type}] 到 Session，账号={account.name}")
    r = await http_client.post(
        "https://biz-discoveryengine.googleapis.com/v1alpha/locations/global/widgetAddContextFile",
        headers=headers,
        json=body,
    )

    if r.status_code != 200:
        logger.error(
            f"上传文件失败 [{account.name}]: {r.status_code} {r.text}"
        )
        if r.status_code in (401, 403, 429):
            account.mark_quota_error(r.status_code, r.text)
        raise HTTPException(r.status_code, f"Upload failed: {r.text}")

    data = r.json()
    file_id = data.get("addContextFileResponse", {}).get("fileId")
    logger.info(f"图片上传成功, ID: {file_id}, 账号={account.name}")
    return file_id


# ---------- 消息处理逻辑 ----------
def get_conversation_key(messages: List[dict]) -> str:
    if not messages:
        return "empty"
    first_msg = messages[0].copy()
    if isinstance(first_msg.get("content"), list):
        text_part = "".join(
            [x.get("text", "") for x in first_msg["content"] if x.get("type") == "text"]
        )
        first_msg["content"] = text_part

    key_str = json.dumps(first_msg, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(key_str.encode("utf-8")).hexdigest()


def parse_last_message(messages: List["Message"]):
    """解析最后一条消息，分离文本和图片"""
    if not messages:
        return "", []

    last_msg = messages[-1]
    content = last_msg.content

    text_content = ""
    images = []  # List of {"mime": str, "data": str_base64}

    if isinstance(content, str):
        text_content = content
    elif isinstance(content, list):
        for part in content:
            if part.get("type") == "text":
                text_content += part.get("text", "")
            elif part.get("type") == "image_url":
                url = part.get("image_url", {}).get("url", "")
                match = re.match(r"data:(image/[^;]+);base64,(.+)", url)
                if match:
                    images.append(
                        {"mime": match.group(1), "data": match.group(2)}
                    )
                else:
                    logger.warning(
                        f"暂不支持非 Base64 图片链接: {url[:30]}..."
                    )

    return text_content, images


def build_full_context_text(messages: List["Message"]) -> str:
    """仅拼接历史文本，图片只处理当次请求的"""
    prompt = ""
    for msg in messages:
        role = "User" if msg.role in ["user", "system"] else "Assistant"
        content_str = ""
        if isinstance(msg.content, str):
            content_str = msg.content
        elif isinstance(msg.content, list):
            for part in msg.content:
                if part.get("type") == "text":
                    content_str += part.get("text", "")
                elif part.get("type") == "image_url":
                    content_str += "[图片]"

        prompt += f"{role}: {content_str}\n\n"
    return prompt


# ---------- 图片生成处理方法 ----------
async def get_session_file_metadata(account: Account, session_name: str) -> dict:
    """获取 session 中的文件元数据，包括下载链接"""
    jwt = await account.jwt_mgr.get()
    headers = get_common_headers(jwt)
    body = {
        "configId": account.config_id,
        "additionalParams": {"token": "-"},
        "listSessionFileMetadataRequest": {
            "name": session_name,
            "filter": "file_origin_type = AI_GENERATED"
        }
    }

    async with httpx.AsyncClient(proxy=PROXY, verify=False, timeout=30) as cli:
        resp = await cli.post(LIST_FILE_METADATA_URL, headers=headers, json=body)

        if resp.status_code == 401:
            # JWT 过期，刷新后重试
            jwt = await account.jwt_mgr.get()
            headers = get_common_headers(jwt)
            resp = await cli.post(LIST_FILE_METADATA_URL, headers=headers, json=body)

        if resp.status_code != 200:
            logger.warning(f"获取文件元数据失败 [{account.name}]: {resp.status_code}")
            return {}

        data = resp.json()
        result = {}
        file_metadata_list = data.get("listSessionFileMetadataResponse", {}).get("fileMetadata", [])
        for fm in file_metadata_list:
            fid = fm.get("fileId")
            if fid:
                result[fid] = fm

        return result


def build_image_download_url(session_name: str, file_id: str) -> str:
    """构造正确的图片下载 URL"""
    return f"https://biz-discoveryengine.googleapis.com/v1alpha/{session_name}:downloadFile?fileId={file_id}&alt=media"


async def download_image_with_jwt(account: Account, session_name: str, file_id: str) -> bytes:
    """使用 JWT 认证下载图片"""
    url = build_image_download_url(session_name, file_id)
    jwt = await account.jwt_mgr.get()
    headers = get_common_headers(jwt)

    async with httpx.AsyncClient(proxy=PROXY, verify=False, timeout=120) as cli:
        resp = await cli.get(url, headers=headers, follow_redirects=True)

        if resp.status_code == 401:
            # JWT 过期，刷新后重试
            jwt = await account.jwt_mgr.get()
            headers = get_common_headers(jwt)
            resp = await cli.get(url, headers=headers, follow_redirects=True)

        resp.raise_for_status()
        content = resp.content

        # 检测是否为 base64 编码的内容
        try:
            text_content = content.decode("utf-8", errors="ignore").strip()
            if text_content.startswith("iVBORw0KGgo") or text_content.startswith("/9j/"):
                # 是 base64 编码，需要解码
                return base64.b64decode(text_content)
        except Exception:
            pass

        return content


async def save_generated_image(account: Account, session_name: str, file_id: str, file_name: Optional[str], mime_type: str, chat_id: str, image_index: int = 1) -> ChatImage:
    """下载并保存生成的图片，按 chat_id 命名"""
    img = ChatImage(
        file_id=file_id,
        file_name=file_name,
        mime_type=mime_type,
    )

    try:
        image_data = await download_image_with_jwt(account, session_name, file_id)
        os.makedirs(IMAGE_SAVE_DIR, exist_ok=True)

        ext = ".png"
        ext_map = {"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp"}
        ext = ext_map.get(mime_type, ".png")

        # 按 {chat_id}_{序号}.png 命名
        filename = f"{chat_id}_{image_index}{ext}"
        filepath = IMAGE_SAVE_DIR / filename

        # 如果文件已存在，添加时间戳避免覆盖
        if filepath.exists():
            timestamp = datetime.now().strftime("%H%M%S")
            filename = f"{chat_id}_{image_index}_{timestamp}{ext}"
            filepath = IMAGE_SAVE_DIR / filename

        with open(filepath, "wb") as f:
            f.write(image_data)

        img.local_path = str(filepath)
        img.file_name = filename
        img.base64_data = base64.b64encode(image_data).decode("utf-8")
        logger.info(f"图片已保存 [{account.name}]: {filepath}")
    except Exception as e:
        logger.error(f"下载图片失败 [{account.name}]: {e}")

    return img


def parse_images_from_response(data_list: list) -> tuple[list, Optional[str]]:
    """
    从 API 响应中解析图片文件引用
    返回: (file_ids_list, current_session)
    file_ids_list: [{"fileId": str, "mimeType": str}, ...]
    """
    file_ids = []
    current_session = None

    for data in data_list:
        sar = data.get("streamAssistResponse")
        if not sar:
            continue

        # 获取 session 信息
        session_info = sar.get("sessionInfo", {})
        if session_info.get("session"):
            current_session = session_info["session"]

        answer = sar.get("answer") or {}
        replies = answer.get("replies") or []

        for reply in replies:
            gc = reply.get("groundedContent", {})
            content = gc.get("content", {})

            # 检查 file 字段（图片生成的关键）
            file_info = content.get("file")
            if file_info and file_info.get("fileId"):
                file_ids.append({
                    "fileId": file_info["fileId"],
                    "mimeType": file_info.get("mimeType", "image/png")
                })

    return file_ids, current_session


# ---------- 应用生命周期管理 ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    init_db()
    db = next(get_db())
    try:
        init_admin(db)
        
        # 清理遗留的"running"状态（可能是上次异常退出导致的）
        running_logs = db.query(KeepAliveLog).filter(
            KeepAliveLog.status == "running"
        ).all()
        for log in running_logs:
            log.status = "error"
            log.finished_at = get_beijing_time()
            log.message = "服务重启，进程已终止"
            
            # 更新所有运行中的账号日志
            running_account_logs = db.query(KeepAliveAccountLog).filter(
                KeepAliveAccountLog.task_log_id == log.id,
                KeepAliveAccountLog.status == "running"
            ).all()
            for acc_log in running_account_logs:
                acc_log.status = "error"
                acc_log.finished_at = get_beijing_time()
                acc_log.message = "服务重启，进程已终止"
        
        # 更新任务状态
        task = db.query(KeepAliveTask).first()
        if task and task.last_status == "running":
            task.last_status = "error"
            task.last_message = "服务重启，进程已终止"
        
        db.commit()
    finally:
        db.close()
    
    # 启动定时任务调度器
    scheduler.start()
    setup_keep_alive_scheduler()
    
    yield  # 应用运行期间
    
    # 关闭时执行
    try:
        scheduler.shutdown()
        await http_client.aclose()
    except Exception:  # noqa: BLE001
        pass


# ---------- OpenAI 兼容接口 ----------
app = FastAPI(title="Gemini-Business OpenAI Gateway", lifespan=lifespan)

# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")

security_bearer = HTTPBearer()


class Message(BaseModel):
    role: str
    content: Union[str, List[Dict[str, Any]]]


class ChatRequest(BaseModel):
    model: str = "gemini-auto"
    messages: List[Message]
    stream: bool = True
    temperature: Optional[float] = 0.7
    top_p: Optional[float] = 1.0


def create_chunk(
    id: str, created: int, model: str, delta: dict, finish_reason: Optional[str]
) -> str:
    chunk = {
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }
        ],
    }
    return json.dumps(chunk, ensure_ascii=False)


@app.get("/v1/models")
async def list_models():
    data = []
    now = int(time.time())
    for m in MODEL_MAPPING.keys():
        data.append(
            {
                "id": m,
                "object": "model",
                "created": now,
                "owned_by": "google",
                "permission": [],
            }
        )
    return {"object": "list", "data": data}


@app.get("/health")
async def health():
    return {"status": "ok", "time": get_beijing_time().isoformat()}


@app.get("/")
async def root():
    """重定向到登录页面"""
    return RedirectResponse(url="/static/index.html")


@app.websocket("/ws/admin/events")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ---------- 管理员认证接口 ----------
class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


class ChangeUsernameRequest(BaseModel):
    new_username: str
    password: str  # 需要验证密码才能修改用户名


@app.post("/admin/login", response_model=LoginResponse)
async def admin_login(req: LoginRequest, db: Session = Depends(get_db)):
    """管理员登录"""
    admin = db.query(Admin).filter(Admin.username == req.username).first()
    
    if not admin or not verify_password(req.password, admin.hashed_password):
        raise HTTPException(
            status_code=401,
            detail="用户名或密码错误"
        )
    
    access_token = create_access_token({"sub": admin.username})
    return LoginResponse(access_token=access_token)


@app.put("/admin/change-password")
async def change_password(
    req: ChangePasswordRequest,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """修改管理员密码"""
    # 验证旧密码
    if not verify_password(req.old_password, admin.hashed_password):
        raise HTTPException(
            status_code=400,
            detail="旧密码错误"
        )
    
    # 验证新密码长度
    if len(req.new_password) < 6:
        raise HTTPException(
            status_code=400,
            detail="新密码长度至少为 6 位"
        )
    
    # 更新密码
    admin.hashed_password = hash_password(req.new_password)
    db.commit()
    
    logger.info(f"✅ 管理员 {admin.username} 修改了密码")
    
    return {"message": "密码修改成功"}


@app.put("/admin/change-username")
async def change_username(
    req: ChangeUsernameRequest,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """修改管理员用户名"""
    # 验证密码
    if not verify_password(req.password, admin.hashed_password):
        raise HTTPException(
            status_code=400,
            detail="密码错误"
        )
    
    # 验证新用户名
    if not req.new_username or len(req.new_username.strip()) == 0:
        raise HTTPException(
            status_code=400,
            detail="用户名不能为空"
        )
    
    if len(req.new_username) > 50:
        raise HTTPException(
            status_code=400,
            detail="用户名长度不能超过 50 个字符"
        )
    
    # 检查新用户名是否已存在
    existing_admin = db.query(Admin).filter(Admin.username == req.new_username.strip()).first()
    if existing_admin and existing_admin.id != admin.id:
        raise HTTPException(
            status_code=400,
            detail="用户名已存在"
        )
    
    old_username = admin.username
    # 更新用户名
    admin.username = req.new_username.strip()
    db.commit()
    
    logger.info(f"✅ 管理员 {old_username} 将用户名修改为 {admin.username}")
    
    return {"message": "用户名修改成功", "new_username": admin.username}


# ---------- API 密钥管理接口 ----------
class GenerateKeysRequest(BaseModel):
    count: int = 1
    expires_days: int = 30
    name_prefix: str = "API Key"


class APIKeyResponse(BaseModel):
    id: int
    key: Optional[str] = None  # 仅在生成时返回明文
    name: str
    created_at: datetime
    expires_at: datetime
    is_active: bool
    usage_count: int
    last_used_at: Optional[datetime]


@app.post("/admin/api-keys", response_model=List[APIKeyResponse])
async def generate_api_keys(
    req: GenerateKeysRequest,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """批量生成 API 密钥"""
    if req.count < 1 or req.count > 100:
        raise HTTPException(status_code=400, detail="数量必须在 1-100 之间")
    
    if req.expires_days < 1 or req.expires_days > 3650:
        raise HTTPException(status_code=400, detail="有效期必须在 1-3650 天之间")
    
    keys = []
    # 存储时使用 naive datetime（数据库兼容）
    expires_at = ensure_naive(get_beijing_time() + timedelta(days=req.expires_days))
    
    for i in range(req.count):
        # 生成 UUID 格式密钥
        plain_key = generate_api_key()
        key_hash = hash_api_key(plain_key)
        encrypted = encrypt_api_key(plain_key)  # 加密存储
        
        name = f"{req.name_prefix} #{i+1}" if req.count > 1 else req.name_prefix
        
        api_key = APIKey(
            key_hash=key_hash,
            encrypted_key=encrypted,  # 存储加密后的密钥
            name=name,
            expires_at=expires_at,
            is_active=True
        )
        db.add(api_key)
        db.commit()
        db.refresh(api_key)
        
        keys.append(APIKeyResponse(
            id=api_key.id,
            key=plain_key,  # 仅在生成时返回明文
            name=api_key.name,
            created_at=api_key.created_at,
            expires_at=api_key.expires_at,
            is_active=api_key.is_active,
            usage_count=api_key.usage_count,
            last_used_at=api_key.last_used_at
        ))
    
    return keys


@app.get("/admin/api-keys", response_model=List[APIKeyResponse])
async def list_api_keys(
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """列出所有 API 密钥（仅显示活跃的）"""
    # 按创建时间升序排列（最老的在前）
    keys = db.query(APIKey).filter(APIKey.is_active == True).order_by(APIKey.created_at.asc()).all()
    return [
        APIKeyResponse(
            id=k.id,
            name=k.name,
            created_at=k.created_at,
            expires_at=k.expires_at,
            is_active=k.is_active,
            usage_count=k.usage_count,
            last_used_at=k.last_used_at
        )
        for k in keys
    ]


@app.delete("/admin/api-keys/{key_id}")
async def revoke_api_key(
    key_id: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """撤销 API 密钥"""
    api_key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not api_key:
        raise HTTPException(status_code=404, detail="密钥不存在")
    
    api_key.is_active = False
    db.commit()
    return {"message": "密钥已撤销"}


@app.get("/admin/api-keys/{key_id}/view")
async def view_api_key(
    key_id: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """查看 API 密钥明文"""
    api_key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not api_key:
        raise HTTPException(status_code=404, detail="密钥不存在")
    
    try:
        decrypted_key = decrypt_api_key(api_key.encrypted_key)
        return {"key": decrypted_key}
    except Exception as e:
        logger.error(f"Failed to decrypt key: {e}")
        raise HTTPException(status_code=500, detail="解密失败")


@app.get("/admin/stats")
async def get_stats(
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取统计信息"""
    from sqlalchemy import func
    
    total_keys = db.query(APIKey).count()
    # 获取当前北京时间（naive 格式用于数据库比较）
    now_naive = get_beijing_time().replace(tzinfo=None)
    active_keys = db.query(APIKey).filter(
        APIKey.is_active == True,
        APIKey.expires_at > now_naive
    ).count()
    total_usage = db.query(func.sum(APIKey.usage_count)).filter(
        APIKey.is_active == True
    ).scalar() or 0
    
    return {
        "total_keys": total_keys,
        "active_keys": active_keys,
        "total_usage": total_usage
    }


@app.get("/admin/api-keys/{key_id}/logs")
async def get_api_key_logs(
    key_id: int,
    page: int = 1,
    page_size: int = 50,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取指定 API 密钥的调用日志"""
    # 验证密钥是否存在
    api_key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not api_key:
        raise HTTPException(status_code=404, detail="密钥不存在")
    
    # 查询日志
    offset = (page - 1) * page_size
    logs = db.query(APICallLog).filter(
        APICallLog.api_key_id == key_id
    ).order_by(
        APICallLog.timestamp.desc()
    ).offset(offset).limit(page_size).all()
    
    # 获取总数
    total = db.query(APICallLog).filter(APICallLog.api_key_id == key_id).count()
    
    return {
        "key_id": key_id,
        "key_name": api_key.name,
        "total": total,
        "page": page,
        "page_size": page_size,
        "logs": [
            {
                "id": log.id,
                "timestamp": log.timestamp.isoformat(),
                "model": log.model,
                "status": log.status,
                "error_message": log.error_message,
                "ip_address": log.ip_address,
                "endpoint": log.endpoint,
                "response_time": log.response_time
            }
            for log in logs
        ]
    }


@app.get("/admin/api-keys/{key_id}/stats")
async def get_api_key_stats(
    key_id: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取指定 API 密钥的统计信息"""
    from sqlalchemy import func
    
    # 验证密钥是否存在
    api_key = db.query(APIKey).filter(APIKey.id == key_id).first()
    if not api_key:
        raise HTTPException(status_code=404, detail="密钥不存在")
    
    # 总调用次数
    total_calls = db.query(APICallLog).filter(APICallLog.api_key_id == key_id).count()
    
    # 成功/失败统计
    success_calls = db.query(APICallLog).filter(
        APICallLog.api_key_id == key_id,
        APICallLog.status == "success"
    ).count()
    
    error_calls = db.query(APICallLog).filter(
        APICallLog.api_key_id == key_id,
        APICallLog.status == "error"
    ).count()
    
    # 按模型统计
    model_stats = db.query(
        APICallLog.model,
        func.count(APICallLog.id).label("count")
    ).filter(
        APICallLog.api_key_id == key_id
    ).group_by(APICallLog.model).all()
    
    # 平均响应时间
    avg_response_time = db.query(
        func.avg(APICallLog.response_time)
    ).filter(
        APICallLog.api_key_id == key_id,
        APICallLog.response_time.isnot(None)
    ).scalar() or 0
    
    # 最近 7 天的调用趋势（使用 naive datetime 用于数据库查询）
    seven_days_ago = ensure_naive(get_beijing_time() - timedelta(days=7))
    
    daily_stats = db.query(
        func.date(APICallLog.timestamp).label("date"),
        func.count(APICallLog.id).label("count")
    ).filter(
        APICallLog.api_key_id == key_id,
        APICallLog.timestamp >= seven_days_ago
    ).group_by(
        func.date(APICallLog.timestamp)
    ).order_by(
        func.date(APICallLog.timestamp)
    ).all()
    
    return {
        "key_id": key_id,
        "key_name": api_key.name,
        "total_calls": total_calls,
        "success_calls": success_calls,
        "error_calls": error_calls,
        "success_rate": round(success_calls / total_calls * 100, 2) if total_calls > 0 else 0,
        "avg_response_time": round(avg_response_time, 2),
        "model_stats": [
            {"model": m[0], "count": m[1]}
            for m in model_stats
        ],
        "daily_stats": [
            {"date": str(d[0]), "count": d[1]}
            for d in daily_stats
        ]
    }


# ---------- 账号管理接口 ----------
def extract_email_from_name(name: str) -> Optional[str]:
    """从账号名称中提取邮箱地址"""
    if not name:
        return None
    
    # 邮箱正则表达式
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, name)
    if match:
        return match.group(0).lower()  # 转换为小写以便比较
    return None


def read_env_file() -> List[Dict[str, str]]:
    """读取 .env 文件，返回所有行（包括注释和空行）"""
    env_path = ".env"
    if not os.path.exists(env_path):
        return []
    
    lines = []
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                lines.append({"raw": line.rstrip("\n\r"), "type": "line"})
    except Exception as e:
        logger.error(f"读取 .env 文件失败: {e}")
        raise HTTPException(status_code=500, detail=f"读取 .env 文件失败: {e}")
    
    return lines


def write_env_file(lines: List[Dict[str, str]]) -> None:
    """写入 .env 文件"""
    env_path = ".env"
    try:
        with open(env_path, "w", encoding="utf-8") as f:
            for line_data in lines:
                f.write(line_data["raw"] + "\n")
    except Exception as e:
        logger.error(f"写入 .env 文件失败: {e}")
        raise HTTPException(status_code=500, detail=f"写入 .env 文件失败: {e}")


def reindex_accounts_in_file(lines: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """重新分配账号索引，使索引从1开始连续"""
    # 解析现有账号
    accounts = parse_accounts_from_env_lines(lines)
    
    # 过滤掉旧格式账号（index=0），只保留有效的账号
    valid_accounts = [acc for acc in accounts if acc["index"] > 0]
    
    # 如果没有有效账号，直接返回
    if not valid_accounts:
        return lines
    
    # 按索引排序
    valid_accounts.sort(key=lambda x: x["index"])
    
    # 创建索引映射：旧索引 -> 新索引（从1开始连续编号）
    index_mapping = {}
    for new_idx, acc in enumerate(valid_accounts, start=1):
        old_idx = acc["index"]
        index_mapping[old_idx] = new_idx
    
    # 重新构建文件内容
    new_lines = []
    i = 0
    
    while i < len(lines):
        line_data = lines[i]
        line = line_data["raw"]
        
        # 检查是否是账号相关的行
        if "ACCOUNT" in line and "_" in line:
            # 尝试匹配 ACCOUNT{数字}_ 格式
            match = re.match(r'ACCOUNT(\d+)_', line)
            if match:
                old_idx = int(match.group(1))
                if old_idx in index_mapping:
                    new_idx = index_mapping[old_idx]
                    # 替换索引
                    new_line = line.replace(f"ACCOUNT{old_idx}_", f"ACCOUNT{new_idx}_")
                    new_lines.append({"raw": new_line, "type": "line"})
                    i += 1
                    continue
        
        # 检查是否是账号注释行
        if line.strip().startswith("#") and "Account" in line:
            # 尝试匹配 # Account {数字}: 格式
            match = re.match(r'#\s*Account\s+(\d+):', line, re.IGNORECASE)
            if match:
                old_idx = int(match.group(1))
                if old_idx in index_mapping:
                    new_idx = index_mapping[old_idx]
                    # 获取账号名称
                    name_match = re.search(r':\s*([^@]+)', line)
                    name = name_match.group(1).strip() if name_match else f"account-{new_idx}"
                    # 替换注释
                    new_line = f"# Account {new_idx}: {name}@{new_idx}"
                    new_lines.append({"raw": new_line, "type": "comment"})
                    i += 1
                    continue
        
        # 其他行保持不变
        new_lines.append(line_data)
        i += 1
    
    return new_lines


def parse_accounts_from_env_lines(lines: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """从 .env 文件行中解析账号配置"""
    accounts = []
    account_vars = {}  # {index: {vars}}
    
    # 解析所有账号相关的环境变量
    for line_data in lines:
        line = line_data["raw"].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # 去除 config_id 中可能存在的 ?csesidx 后缀
        if key.endswith("_CONFIG_ID") and '?csesidx' in value:
            value = value.split('?csesidx')[0]
        
        # 检查是否是账号配置
        if key.startswith("ACCOUNT") and key.endswith("_SECURE_C_SES"):
            idx_str = key[len("ACCOUNT") : -len("_SECURE_C_SES")]
            try:
                idx = int(idx_str)
                if idx not in account_vars:
                    account_vars[idx] = {}
                account_vars[idx]["SECURE_C_SES"] = value
                account_vars[idx]["index"] = idx
            except ValueError:
                continue
        elif key.startswith("ACCOUNT") and "_" in key:
            parts = key.split("_", 1)
            if len(parts) == 2 and parts[0].startswith("ACCOUNT"):
                idx_str = parts[0][len("ACCOUNT"):]
                try:
                    idx = int(idx_str)
                    var_name = parts[1]
                    if idx not in account_vars:
                        account_vars[idx] = {}
                    account_vars[idx][var_name] = value
                    account_vars[idx]["index"] = idx
                except ValueError:
                    continue
    
    # 检查旧的单账号配置
    old_account = {}
    for line_data in lines:
        line = line_data["raw"].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # 去除 config_id 中可能存在的 ?csesidx 后缀
        if key == "CONFIG_ID" and '?csesidx' in value:
            value = value.split('?csesidx')[0]
        if key in ["SECURE_C_SES", "CSESIDX", "CONFIG_ID", "HOST_C_OSES"]:
            old_account[key] = value
    
    # 处理多账号配置
    for idx in sorted(account_vars.keys()):
        vars_dict = account_vars[idx]
        if vars_dict.get("SECURE_C_SES") and vars_dict.get("CSESIDX") and vars_dict.get("CONFIG_ID"):
            accounts.append({
                "index": idx,
                "name": vars_dict.get("NAME") or f"account-{idx}",
                "secure_c_ses": vars_dict.get("SECURE_C_SES"),
                "csesidx": vars_dict.get("CSESIDX"),
                "config_id": vars_dict.get("CONFIG_ID"),
                "host_c_oses": vars_dict.get("HOST_C_OSES", ""),
            })
    
    # 处理旧的单账号配置
    if not accounts and old_account.get("SECURE_C_SES") and old_account.get("CSESIDX") and old_account.get("CONFIG_ID"):
        accounts.append({
            "index": 0,  # 0 表示旧格式
            "name": "default",
            "secure_c_ses": old_account.get("SECURE_C_SES"),
            "csesidx": old_account.get("CSESIDX"),
            "config_id": old_account.get("CONFIG_ID"),
            "host_c_oses": old_account.get("HOST_C_OSES", ""),
        })
    
    return accounts


class AccountRequest(BaseModel):
    name: str
    secure_c_ses: str
    csesidx: str
    config_id: str
    host_c_oses: Optional[str] = ""


class AccountResponse(BaseModel):
    index: int
    name: str
    secure_c_ses: str
    csesidx: str
    config_id: str
    host_c_oses: str
    status: Optional[str] = None  # 测试状态
    cookie_status: Optional[str] = None  # Cookie 状态: valid, expired, unknown
    last_check_at: Optional[datetime] = None  # 最后检查时间
    expires_at: Optional[datetime] = None  # 预估到期时间（如果可获取）
    error_message: Optional[str] = None  # 错误信息（如果过期）


@app.get("/admin/accounts", response_model=List[AccountResponse])
async def list_accounts(
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """列出所有账号配置"""
    from database import AccountCookieStatus
    
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 获取当前运行时的账号状态
    account_status = {}
    if ACCOUNT_POOL:
        for acc in ACCOUNT_POOL.accounts:
            account_status[acc.name] = "available" if acc.is_available() else "unavailable"
    
    # 从数据库获取 Cookie 状态
    cookie_status_map = {}
    cookie_statuses = db.query(AccountCookieStatus).all()
    logger.debug(f"从数据库读取到 {len(cookie_statuses)} 条 Cookie 状态记录")
    for cs in cookie_statuses:
        cookie_status_map[cs.account_name] = {
            "cookie_status": cs.cookie_status,
            "last_check_at": cs.last_check_at,
            "expires_at": cs.expires_at,
            "error_message": cs.error_message
        }
        if cs.last_check_at:
            logger.debug(f"账号 {cs.account_name} 的最后检查时间: {cs.last_check_at}")
    
    result = []
    for acc in accounts:
        status = account_status.get(acc["name"], "unknown")
        cookie_info = cookie_status_map.get(acc["name"], {})
        
        result.append(AccountResponse(
            index=acc["index"],
            name=acc["name"],
            secure_c_ses=acc["secure_c_ses"],
            csesidx=acc["csesidx"],
            config_id=acc["config_id"],
            host_c_oses=acc.get("host_c_oses", ""),
            status=status,
            cookie_status=cookie_info.get("cookie_status"),
            last_check_at=cookie_info.get("last_check_at"),
            expires_at=cookie_info.get("expires_at"),
            error_message=cookie_info.get("error_message")
        ))
    
    return result


@app.post("/admin/accounts", response_model=AccountResponse)
async def create_account(
    req: AccountRequest,
    admin: Admin = Depends(get_current_admin)
):
    """创建新账号"""
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 邮箱去重检查
    new_email = extract_email_from_name(req.name)
    if new_email:
        for acc in accounts:
            existing_email = extract_email_from_name(acc["name"])
            if existing_email and existing_email == new_email:
                raise HTTPException(
                    status_code=400,
                    detail=f"邮箱 {new_email} 已存在于账号 {acc['name']} 中"
                )
    
    # 找到下一个可用的索引
    existing_indices = {acc["index"] for acc in accounts if acc["index"] > 0}
    next_index = 1
    while next_index in existing_indices:
        next_index += 1
    
    # 添加新账号配置到文件末尾
    new_lines = [
        {"raw": f"# Account {next_index}: {req.name}@{next_index}", "type": "comment"},
        {"raw": f'ACCOUNT{next_index}_NAME="{req.name}"', "type": "line"},
        {"raw": f'ACCOUNT{next_index}_SECURE_C_SES="{req.secure_c_ses}"', "type": "line"},
        {"raw": f'ACCOUNT{next_index}_CSESIDX="{req.csesidx}"', "type": "line"},
        {"raw": f'ACCOUNT{next_index}_CONFIG_ID="{req.config_id}"', "type": "line"},
    ]
    if req.host_c_oses:
        new_lines.append({"raw": f'ACCOUNT{next_index}_HOST_C_OSES="{req.host_c_oses}"', "type": "line"})
    new_lines.append({"raw": "", "type": "line"})  # 空行分隔
    
    lines.extend(new_lines)
    write_env_file(lines)
    
    # 动态重新加载账号配置
    reload_accounts_from_env_file()
    
    logger.info(f"✅ 管理员 {admin.username} 创建了新账号: {req.name} (索引: {next_index})")
    
    return AccountResponse(
        index=next_index,
        name=req.name,
        secure_c_ses=req.secure_c_ses,
        csesidx=req.csesidx,
        config_id=req.config_id,
        host_c_oses=req.host_c_oses or "",
        status="unknown"
    )


class BulkAccountRequest(BaseModel):
    """批量添加账号请求"""
    text: str  # 原始文本，从中提取账号信息


def extract_accounts_from_text(text: str) -> List[Dict[str, str]]:
    """
    从文本中模糊匹配提取账号信息
    查找 NAME、SECURE_C_SES、CSESIDX、CONFIG_ID、HOST_C_OSES 字段
    """
    accounts = []
    lines = text.split('\n')
    
    # 使用正则表达式匹配各种可能的格式
    # 支持格式：KEY=value, KEY="value", KEY='value', KEY: value 等
    # 匹配带引号的值：["']([^"']+)["'] 或 不带引号的值：([^\s=:]+)
    def extract_value(line: str, key_pattern: str) -> Optional[str]:
        # 先尝试匹配带引号的格式
        quoted_pattern = re.compile(
            rf'{key_pattern}\s*[=:]\s*["\']([^"\']+)["\']',
            re.IGNORECASE
        )
        match = quoted_pattern.search(line)
        if match:
            return match.group(1).strip()
        
        # 再尝试匹配不带引号的格式
        unquoted_pattern = re.compile(
            rf'{key_pattern}\s*[=:]\s*([^\s=:]+)',
            re.IGNORECASE
        )
        match = unquoted_pattern.search(line)
        if match:
            return match.group(1).strip()
        
        return None
    
    current_account = {}
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            # 遇到空行或注释，如果当前账号有必需字段，保存它
            if current_account.get('secure_c_ses') and current_account.get('csesidx') and current_account.get('config_id'):
                accounts.append(current_account.copy())
                current_account = {}
            continue
        
        # 尝试匹配各个字段
        # NAME
        value = extract_value(line, r'(?:NAME|name)')
        if value:
            current_account['name'] = value
            continue
        
        # SECURE_C_SES
        value = extract_value(line, r'(?:SECURE_C_SES|secure_c_ses|SECURE_CSES|secure_cses)')
        if value:
            current_account['secure_c_ses'] = value
            continue
        
        # CSESIDX
        value = extract_value(line, r'(?:CSESIDX|csesidx|CSES_IDX|cses_idx)')
        if value:
            current_account['csesidx'] = value
            continue
        
        # CONFIG_ID
        value = extract_value(line, r'(?:CONFIG_ID|config_id|CONFIGID|configid)')
        if value:
            current_account['config_id'] = value
            continue
        
        # HOST_C_OSES
        value = extract_value(line, r'(?:HOST_C_OSES|host_c_oses|HOST_COSES|host_coses)')
        if value:
            current_account['host_c_oses'] = value
            continue
        
        # 也支持 ACCOUNT1_NAME="value" 这种格式
        account_match = re.match(
            r'ACCOUNT\d+_(NAME|SECURE_C_SES|CSESIDX|CONFIG_ID|HOST_C_OSES)\s*=\s*["\']?([^"\']+)["\']?',
            line,
            re.IGNORECASE
        )
        if account_match:
            key = account_match.group(1).lower()
            value = account_match.group(2).strip()
            if key == 'name':
                current_account['name'] = value
            elif key == 'secure_c_ses':
                current_account['secure_c_ses'] = value
            elif key == 'csesidx':
                current_account['csesidx'] = value
            elif key == 'config_id':
                current_account['config_id'] = value
            elif key == 'host_c_oses':
                current_account['host_c_oses'] = value
    
    # 保存最后一个账号
    if current_account.get('secure_c_ses') and current_account.get('csesidx') and current_account.get('config_id'):
        accounts.append(current_account)
    
    # 如果没有找到任何账号，尝试按空行或明显分隔符分割文本块
    if not accounts:
        # 尝试按多个空行或明显分隔符分割
        blocks = re.split(r'\n\s*\n+', text)
        for block in blocks:
            account = {}
            # NAME
            value = extract_value(block, r'(?:NAME|name)')
            if value:
                account['name'] = value
            # SECURE_C_SES
            value = extract_value(block, r'(?:SECURE_C_SES|secure_c_ses|SECURE_CSES|secure_cses)')
            if value:
                account['secure_c_ses'] = value
            # CSESIDX
            value = extract_value(block, r'(?:CSESIDX|csesidx|CSES_IDX|cses_idx)')
            if value:
                account['csesidx'] = value
            # CONFIG_ID
            value = extract_value(block, r'(?:CONFIG_ID|config_id|CONFIGID|configid)')
            if value:
                account['config_id'] = value
            # HOST_C_OSES
            value = extract_value(block, r'(?:HOST_C_OSES|host_c_oses|HOST_COSES|host_coses)')
            if value:
                account['host_c_oses'] = value
            
            if account.get('secure_c_ses') and account.get('csesidx') and account.get('config_id'):
                accounts.append(account)
    
    return accounts


@app.post("/admin/accounts/bulk")
async def create_accounts_bulk(
    req: BulkAccountRequest,
    admin: Admin = Depends(get_current_admin)
):
    """批量创建账号（带去重功能，从文本中模糊匹配提取）"""
    if not req.text or not req.text.strip():
        raise HTTPException(status_code=400, detail="文本内容不能为空")
    
    # 从文本中提取账号信息
    extracted_accounts = extract_accounts_from_text(req.text)
    if not extracted_accounts:
        raise HTTPException(status_code=400, detail="未能从文本中提取到有效的账号信息，请确保包含 NAME、SECURE_C_SES、CSESIDX、CONFIG_ID 字段")
    
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 获取已存在的账号标识（用于去重）
    existing_config_ids = {acc["config_id"] for acc in accounts}
    existing_names = {acc["name"] for acc in accounts}
    # 提取已存在账号的邮箱集合（用于邮箱去重）
    existing_emails = set()
    for acc in accounts:
        email = extract_email_from_name(acc["name"])
        if email:
            existing_emails.add(email)
    
    # 找到下一个可用的索引
    existing_indices = {acc["index"] for acc in accounts if acc["index"] > 0}
    next_index = 1
    while next_index in existing_indices:
        next_index += 1
    
    created_accounts = []
    skipped_accounts = []
    seen_in_batch = set()  # 用于批量添加内部的去重（CONFIG_ID）
    seen_emails_in_batch = set()  # 用于批量添加内部的邮箱去重
    
    new_lines = []
    
    for acc_data in extracted_accounts:
        # 获取字段值，如果不存在则使用空字符串
        name = acc_data.get('name', '').strip() or f'account-{next_index}'
        secure_c_ses = acc_data.get('secure_c_ses', '').strip()
        csesidx = acc_data.get('csesidx', '').strip()
        config_id = acc_data.get('config_id', '').strip()
        host_c_oses = acc_data.get('host_c_oses', '').strip()
        
        # 验证必需字段
        if not secure_c_ses or not csesidx or not config_id:
            skipped_accounts.append({
                "name": name,
                "reason": "缺少必需字段（SECURE_C_SES、CSESIDX、CONFIG_ID）"
            })
            continue
        
        # 去重检查1：检查是否与已存在的账号重复（使用 CONFIG_ID）
        if config_id in existing_config_ids:
            skipped_accounts.append({
                "name": name,
                "reason": "CONFIG_ID 已存在"
            })
            continue
        
        # 去重检查2：检查是否与已存在的账号名称重复
        if name in existing_names:
            skipped_accounts.append({
                "name": name,
                "reason": "账号名称已存在"
            })
            continue
        
        # 去重检查3：邮箱去重 - 检查是否与已存在的账号邮箱重复
        email = extract_email_from_name(name)
        if email:
            if email in existing_emails:
                skipped_accounts.append({
                    "name": name,
                    "reason": f"邮箱 {email} 已存在"
                })
                continue
        
        # 去重检查4：检查批量添加的账号之间是否重复（使用 CONFIG_ID）
        if config_id in seen_in_batch:
            skipped_accounts.append({
                "name": name,
                "reason": "批量添加中 CONFIG_ID 重复"
            })
            continue
        
        # 去重检查5：检查批量添加的账号名称是否重复
        if name in {acc["name"] for acc in created_accounts}:
            skipped_accounts.append({
                "name": name,
                "reason": "批量添加中账号名称重复"
            })
            continue
        
        # 去重检查6：批量添加中的邮箱去重
        if email and email in seen_emails_in_batch:
            skipped_accounts.append({
                "name": name,
                "reason": f"批量添加中邮箱 {email} 重复"
            })
            continue
        
        # 通过所有去重检查，添加到待创建列表
        seen_in_batch.add(config_id)
        if email:
            seen_emails_in_batch.add(email)
            existing_emails.add(email)  # 更新已存在邮箱集合，避免后续重复
        
        # 添加新账号配置
        new_lines.append({"raw": f"# Account {next_index}: {name}@{next_index}", "type": "comment"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_NAME="{name}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_SECURE_C_SES="{secure_c_ses}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_CSESIDX="{csesidx}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_CONFIG_ID="{config_id}"', "type": "line"})
        if host_c_oses:
            new_lines.append({"raw": f'ACCOUNT{next_index}_HOST_C_OSES="{host_c_oses}"', "type": "line"})
        new_lines.append({"raw": "", "type": "line"})  # 空行分隔
        
        created_accounts.append({
            "index": next_index,
            "name": name
        })
        
        # 更新已存在的集合（避免后续重复）
        existing_config_ids.add(config_id)
        existing_names.add(name)
        
        next_index += 1
        # 确保索引不冲突
        while next_index in existing_indices:
            next_index += 1
    
    if new_lines:
        lines.extend(new_lines)
        write_env_file(lines)
        # 动态重新加载账号配置
        reload_accounts_from_env_file()
        logger.info(f"✅ 管理员 {admin.username} 批量创建了 {len(created_accounts)} 个账号，跳过了 {len(skipped_accounts)} 个重复账号")
    
    return {
        "message": f"成功创建 {len(created_accounts)} 个账号",
        "created": created_accounts,
        "skipped": skipped_accounts,
        "total": len(extracted_accounts),
        "created_count": len(created_accounts),
        "skipped_count": len(skipped_accounts)
    }


@app.put("/admin/accounts/{account_index}", response_model=AccountResponse)
async def update_account(
    account_index: int,
    req: AccountRequest,
    admin: Admin = Depends(get_current_admin)
):
    """更新账号配置"""
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 检查账号是否存在
    account_exists = any(acc["index"] == account_index for acc in accounts)
    if not account_exists:
        raise HTTPException(status_code=404, detail="账号不存在")
    
    # 邮箱去重检查（排除当前账号）
    new_email = extract_email_from_name(req.name)
    if new_email:
        for acc in accounts:
            if acc["index"] == account_index:
                continue  # 跳过当前账号
            existing_email = extract_email_from_name(acc["name"])
            if existing_email and existing_email == new_email:
                raise HTTPException(
                    status_code=400,
                    detail=f"邮箱 {new_email} 已存在于账号 {acc['name']} 中"
                )
    
    # 处理旧格式账号（index=0），转换为新格式
    if account_index == 0:
        # 删除旧格式的配置行
        old_keys = ["SECURE_C_SES", "CSESIDX", "CONFIG_ID", "HOST_C_OSES"]
        new_lines = []
        for line_data in lines:
            line = line_data["raw"].strip()
            if "=" in line:
                key = line.split("=", 1)[0].strip()
                if key not in old_keys:
                    new_lines.append(line_data)
            else:
                new_lines.append(line_data)
        
        # 找到下一个可用的索引
        existing_indices = {acc["index"] for acc in accounts if acc["index"] > 0}
        next_index = 1
        while next_index in existing_indices:
            next_index += 1
        
        # 添加新格式的账号配置
        new_lines.append({"raw": f"# Account {next_index}: {req.name}@{next_index}", "type": "comment"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_NAME="{req.name}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_SECURE_C_SES="{req.secure_c_ses}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_CSESIDX="{req.csesidx}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{next_index}_CONFIG_ID="{req.config_id}"', "type": "line"})
        if req.host_c_oses:
            new_lines.append({"raw": f'ACCOUNT{next_index}_HOST_C_OSES="{req.host_c_oses}"', "type": "line"})
        new_lines.append({"raw": "", "type": "line"})
        
        write_env_file(new_lines)
        
        # 动态重新加载账号配置
        reload_accounts_from_env_file()
        
        logger.info(f"✅ 管理员 {admin.username} 更新了账号: {req.name} (从旧格式转换为索引: {next_index})")
        
        return AccountResponse(
            index=next_index,
            name=req.name,
            secure_c_ses=req.secure_c_ses,
            csesidx=req.csesidx,
            config_id=req.config_id,
            host_c_oses=req.host_c_oses or "",
            status="unknown"
        )
    
    # 更新新格式账号配置
    new_lines = []
    i = 0
    in_account_section = False
    account_start = -1
    account_end = -1
    
    while i < len(lines):
        line = lines[i]["raw"]
        
        # 检查是否进入目标账号区域
        if f"ACCOUNT{account_index}_" in line:
            if not in_account_section:
                in_account_section = True
                account_start = i
                # 检查前面是否有注释
                if i > 0 and lines[i-1]["raw"].strip().startswith("#"):
                    account_start = i - 1
        elif in_account_section:
            # 检查是否离开账号区域（遇到下一个账号或空行后的非账号行）
            if line.strip() and not line.startswith("#") and "ACCOUNT" not in line:
                # 可能是其他配置，结束当前账号区域
                account_end = i
                break
            elif line.strip() == "" and i > account_start:
                # 空行，可能是账号区域结束
                account_end = i
                break
        
        i += 1
    
    if in_account_section and account_end == -1:
        account_end = len(lines)
    
    # 重建文件内容
    if account_start >= 0:
        # 保留账号区域之前的内容
        new_lines.extend(lines[:account_start])
        
        # 添加更新后的账号配置
        account_name_line = f"# Account {account_index}: {req.name}@{account_index}"
        if account_start > 0 and lines[account_start-1]["raw"].strip().startswith("#"):
            # 如果前面有注释，替换它
            new_lines[-1] = {"raw": account_name_line, "type": "comment"}
        else:
            new_lines.append({"raw": account_name_line, "type": "comment"})
        
        new_lines.append({"raw": f'ACCOUNT{account_index}_NAME="{req.name}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_SECURE_C_SES="{req.secure_c_ses}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_CSESIDX="{req.csesidx}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_CONFIG_ID="{req.config_id}"', "type": "line"})
        if req.host_c_oses:
            new_lines.append({"raw": f'ACCOUNT{account_index}_HOST_C_OSES="{req.host_c_oses}"', "type": "line"})
        new_lines.append({"raw": "", "type": "line"})  # 空行分隔
        
        # 保留账号区域之后的内容
        new_lines.extend(lines[account_end:])
    else:
        # 如果找不到账号区域，追加到文件末尾
        new_lines = lines
        new_lines.append({"raw": f"# Account {account_index}: {req.name}@{account_index}", "type": "comment"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_NAME="{req.name}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_SECURE_C_SES="{req.secure_c_ses}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_CSESIDX="{req.csesidx}"', "type": "line"})
        new_lines.append({"raw": f'ACCOUNT{account_index}_CONFIG_ID="{req.config_id}"', "type": "line"})
        if req.host_c_oses:
            new_lines.append({"raw": f'ACCOUNT{account_index}_HOST_C_OSES="{req.host_c_oses}"', "type": "line"})
        new_lines.append({"raw": "", "type": "line"})
    
    write_env_file(new_lines)
    
    # 动态重新加载账号配置
    reload_accounts_from_env_file()
    
    logger.info(f"✅ 管理员 {admin.username} 更新了账号: {req.name} (索引: {account_index})")
    
    return AccountResponse(
        index=account_index,
        name=req.name,
        secure_c_ses=req.secure_c_ses,
        csesidx=req.csesidx,
        config_id=req.config_id,
        host_c_oses=req.host_c_oses or "",
        status="unknown"
    )


@app.delete("/admin/accounts/{account_index}")
async def delete_account(
    account_index: int,
    admin: Admin = Depends(get_current_admin)
):
    """删除账号配置"""
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 检查账号是否存在
    account_exists = any(acc["index"] == account_index for acc in accounts)
    if not account_exists:
        raise HTTPException(status_code=404, detail="账号不存在")
    
    # 处理旧格式账号（index=0）
    if account_index == 0:
        # 删除旧格式的配置行
        old_keys = ["SECURE_C_SES", "CSESIDX", "CONFIG_ID", "HOST_C_OSES"]
        new_lines = []
        for line_data in lines:
            line = line_data["raw"].strip()
            if "=" in line:
                key = line.split("=", 1)[0].strip()
                if key not in old_keys:
                    new_lines.append(line_data)
            else:
                new_lines.append(line_data)
        
        write_env_file(new_lines)
        # 动态重新加载账号配置
        reload_accounts_from_env_file()
        logger.info(f"✅ 管理员 {admin.username} 删除了旧格式账号")
        return {"message": "账号已删除"}
    
    # 删除新格式账号配置
    new_lines = []
    i = 0
    in_account_section = False
    account_start = -1
    account_end = -1
    
    while i < len(lines):
        line = lines[i]["raw"]
        
        # 检查是否进入目标账号区域
        if f"ACCOUNT{account_index}_" in line:
            if not in_account_section:
                in_account_section = True
                account_start = i
                # 检查前面是否有注释
                if i > 0 and lines[i-1]["raw"].strip().startswith("#"):
                    account_start = i - 1
        elif in_account_section:
            # 检查是否离开账号区域
            if line.strip() and not line.startswith("#") and "ACCOUNT" not in line:
                account_end = i
                break
            elif line.strip() == "" and i > account_start:
                account_end = i
                break
        
        i += 1
    
    if in_account_section and account_end == -1:
        account_end = len(lines)
    
    # 重建文件内容（跳过账号区域）
    if account_start >= 0:
        new_lines.extend(lines[:account_start])
        new_lines.extend(lines[account_end:])
    else:
        new_lines = lines
    
    # 重新分配索引
    new_lines = reindex_accounts_in_file(new_lines)
    
    write_env_file(new_lines)
    
    # 动态重新加载账号配置
    reload_accounts_from_env_file()
    
    logger.info(f"✅ 管理员 {admin.username} 删除了账号 (索引: {account_index})，已自动重新分配索引")
    
    return {"message": "账号已删除，索引已自动重新分配"}


class BulkDeleteAccountRequest(BaseModel):
    """批量删除账号请求"""
    indices: List[int]  # 要删除的账号索引列表


@app.post("/admin/accounts/bulk-delete")
async def bulk_delete_accounts(
    req: BulkDeleteAccountRequest,
    admin: Admin = Depends(get_current_admin)
):
    """批量删除账号"""
    if not req.indices:
        raise HTTPException(status_code=400, detail="请选择要删除的账号")
    
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 验证要删除的账号是否存在
    existing_indices = {acc["index"] for acc in accounts}
    invalid_indices = [idx for idx in req.indices if idx not in existing_indices]
    if invalid_indices:
        raise HTTPException(status_code=404, detail=f"以下账号不存在: {invalid_indices}")
    
    # 不允许删除默认账号（index=0）
    if 0 in req.indices:
        raise HTTPException(status_code=400, detail="不能删除默认账号")
    
    # 按索引从大到小排序，从后往前删除，避免索引变化影响
    sorted_indices = sorted(req.indices, reverse=True)
    
    deleted_count = 0
    for account_index in sorted_indices:
        # 删除账号配置
        new_lines = []
        i = 0
        in_account_section = False
        account_start = -1
        account_end = -1
        
        while i < len(lines):
            line = lines[i]["raw"]
            
            # 检查是否进入目标账号区域
            if f"ACCOUNT{account_index}_" in line:
                if not in_account_section:
                    in_account_section = True
                    account_start = i
                    # 检查前面是否有注释
                    if account_start > 0 and lines[account_start - 1]["raw"].strip().startswith("#"):
                        account_start -= 1
            elif in_account_section:
                # 检查是否离开账号区域（遇到空行或下一个账号）
                if not line.strip() or (line.strip().startswith("#") and "Account" in line and f"Account {account_index}" not in line):
                    account_end = i
                    break
                # 检查是否是下一个账号的配置
                if "ACCOUNT" in line and f"ACCOUNT{account_index}_" not in line:
                    account_end = i
                    break
            
            i += 1
        
        # 如果找到了账号区域，删除它
        if account_start >= 0:
            if account_end == -1:
                account_end = len(lines)
            new_lines = lines[:account_start] + lines[account_end:]
            lines = new_lines
            deleted_count += 1
    
    if deleted_count > 0:
        # 重新分配索引
        lines = reindex_accounts_in_file(lines)
        write_env_file(lines)
        # 动态重新加载账号配置
        reload_accounts_from_env_file()
        logger.info(f"✅ 管理员 {admin.username} 批量删除了 {deleted_count} 个账号 (索引: {req.indices})，已自动重新分配索引")
    
    return {
        "message": f"成功删除 {deleted_count} 个账号，索引已自动重新分配",
        "deleted_count": deleted_count
    }


# ---------- 保活策略管理接口 ----------
class KeepAliveTaskRequest(BaseModel):
    is_enabled: bool
    schedule_time: str  # HH:MM 格式
    api_keepalive_enabled: bool = True  # API 保活是否启用
    api_keepalive_interval: int = 30  # API 保活间隔（分钟）
    auto_check_enabled: bool = False  # 自动检查是否启用
    auto_check_interval: int = 60  # 自动检查间隔（分钟）
    auto_check_auto_fix: bool = True  # 检测到无效时自动修复


class KeepAliveTaskResponse(BaseModel):
    id: int
    is_enabled: bool
    schedule_time: str
    api_keepalive_enabled: bool
    api_keepalive_interval: int
    auto_check_enabled: bool = False
    auto_check_interval: int = 60
    auto_check_auto_fix: bool = True
    last_run_at: Optional[datetime]
    last_status: Optional[str]
    last_message: Optional[str]
    last_api_keepalive_at: Optional[datetime]
    last_auto_check_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class KeepAliveLogResponse(BaseModel):
    id: int
    task_id: int
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    message: Optional[str]
    accounts_count: Optional[int]
    success_count: Optional[int]
    fail_count: Optional[int]


@app.get("/admin/keep-alive/task", response_model=KeepAliveTaskResponse)
async def get_keep_alive_task(
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取保活任务配置"""
    global current_keep_alive_process
    
    task = db.query(KeepAliveTask).first()
    if not task:
        # 创建默认任务
        task = KeepAliveTask(
            is_enabled=True,
            schedule_time="00:00",
            api_keepalive_enabled=True,
            api_keepalive_interval=30
        )
        db.add(task)
        db.commit()
        db.refresh(task)
    
    # 检查实际进程状态，如果数据库显示"running"但进程不存在，更新状态
    async with keep_alive_process_lock:
        is_actually_running = current_keep_alive_process is not None and current_keep_alive_process.poll() is None
    
    # 如果数据库显示运行中，但实际没有进程，清理状态
    if task.last_status == "running" and not is_actually_running:
        # 检查是否有未完成的日志
        running_log = db.query(KeepAliveLog).filter(
            KeepAliveLog.status == "running"
        ).order_by(KeepAliveLog.started_at.desc()).first()
        
        if running_log:
            # 更新日志状态
            running_log.status = "error"
            running_log.finished_at = get_beijing_time()
            running_log.message = "进程异常退出"
            
            # 更新所有运行中的账号日志
            running_account_logs = db.query(KeepAliveAccountLog).filter(
                KeepAliveAccountLog.task_log_id == running_log.id,
                KeepAliveAccountLog.status == "running"
            ).all()
            for acc_log in running_account_logs:
                acc_log.status = "error"
                acc_log.finished_at = get_beijing_time()
                acc_log.message = "进程异常退出"
        
        # 更新任务状态
        task.last_status = "error" if running_log else task.last_status
        task.last_message = "进程异常退出" if running_log else task.last_message
        db.commit()
    
    return KeepAliveTaskResponse(
        id=task.id,
        is_enabled=task.is_enabled,
        schedule_time=task.schedule_time,
        api_keepalive_enabled=getattr(task, 'api_keepalive_enabled', True),
        api_keepalive_interval=getattr(task, 'api_keepalive_interval', 30),
        auto_check_enabled=getattr(task, 'auto_check_enabled', False),
        auto_check_interval=getattr(task, 'auto_check_interval', 60),
        auto_check_auto_fix=getattr(task, 'auto_check_auto_fix', True),
        last_run_at=task.last_run_at,
        last_status=task.last_status,
        last_message=task.last_message,
        last_api_keepalive_at=getattr(task, 'last_api_keepalive_at', None),
        last_auto_check_at=getattr(task, 'last_auto_check_at', None),
        created_at=task.created_at,
        updated_at=task.updated_at
    )


@app.put("/admin/keep-alive/task", response_model=KeepAliveTaskResponse)
async def update_keep_alive_task(
    req: KeepAliveTaskRequest,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """更新保活任务配置"""
    # 验证时间格式
    try:
        hour, minute = map(int, req.schedule_time.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("时间格式错误")
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="时间格式错误，应为 HH:MM (24小时制)")
    
    # 验证 API 保活间隔
    if req.api_keepalive_interval < 5 or req.api_keepalive_interval > 1440:
        raise HTTPException(status_code=400, detail="API 保活间隔必须在 5-1440 分钟之间")
    
    # 验证自动检查间隔
    if req.auto_check_interval < 5 or req.auto_check_interval > 1440:
        raise HTTPException(status_code=400, detail="自动检查间隔必须在 5-1440 分钟之间")
    
    task = db.query(KeepAliveTask).first()
    if not task:
        task = KeepAliveTask(
            is_enabled=req.is_enabled,
            schedule_time=req.schedule_time,
            api_keepalive_enabled=req.api_keepalive_enabled,
            api_keepalive_interval=req.api_keepalive_interval,
            auto_check_enabled=req.auto_check_enabled,
            auto_check_interval=req.auto_check_interval,
            auto_check_auto_fix=req.auto_check_auto_fix
        )
        db.add(task)
    else:
        task.is_enabled = req.is_enabled
        task.schedule_time = req.schedule_time
        task.api_keepalive_enabled = req.api_keepalive_enabled
        task.api_keepalive_interval = req.api_keepalive_interval
        task.auto_check_enabled = req.auto_check_enabled
        task.auto_check_interval = req.auto_check_interval
        task.auto_check_auto_fix = req.auto_check_auto_fix
        task.updated_at = get_beijing_time()
    
    db.commit()
    db.refresh(task)
    
    # 重新设置调度器
    try:
        scheduler.remove_job("keep_alive_task")
        scheduler.remove_job("api_keepalive_task")
        scheduler.remove_job("auto_check_task")
    except Exception:
        # 如果任务不存在，忽略错误
        pass
    
    if task.is_enabled:
        try:
            hour, minute = map(int, task.schedule_time.split(":"))
            scheduler.add_job(
                execute_keep_alive_task,
                trigger=CronTrigger(hour=hour, minute=minute, timezone=BEIJING_TZ),
                id="keep_alive_task",
                replace_existing=True
            )
            logger.info(f"✅ 保活任务已更新，每日 {task.schedule_time} (北京时间) 执行")
        except Exception as e:
            logger.error(f"❌ 设置保活任务调度器失败: {e}")
            raise HTTPException(status_code=500, detail=f"设置调度器失败: {str(e)}")
    else:
        logger.info("ℹ️ 保活任务已禁用")
    
    # 设置 API 保活调度器
    if task.api_keepalive_enabled:
        try:
            scheduler.add_job(
                execute_api_keepalive_task,
                trigger=CronTrigger(minute=f"*/{task.api_keepalive_interval}", timezone=BEIJING_TZ),
                id="api_keepalive_task",
                replace_existing=True
            )
            logger.info(f"✅ API 保活任务已设置，每 {task.api_keepalive_interval} 分钟执行一次")
        except Exception as e:
            logger.error(f"❌ 设置 API 保活调度器失败: {e}")
    else:
        logger.info("ℹ️ API 保活任务已禁用")
    
    # 设置自动检查调度器
    if task.auto_check_enabled:
        try:
            scheduler.add_job(
                execute_auto_check_task,
                trigger=CronTrigger(minute=f"*/{task.auto_check_interval}", timezone=BEIJING_TZ),
                id="auto_check_task",
                replace_existing=True
            )
            logger.info(f"✅ 自动检查任务已设置，每 {task.auto_check_interval} 分钟执行一次")
        except Exception as e:
            logger.error(f"❌ 设置自动检查调度器失败: {e}")
    else:
        logger.info("ℹ️ 自动检查任务已禁用")
    
    return KeepAliveTaskResponse(
        id=task.id,
        is_enabled=task.is_enabled,
        schedule_time=task.schedule_time,
        api_keepalive_enabled=task.api_keepalive_enabled,
        api_keepalive_interval=task.api_keepalive_interval,
        last_run_at=task.last_run_at,
        last_status=task.last_status,
        last_message=task.last_message,
        last_api_keepalive_at=getattr(task, 'last_api_keepalive_at', None),
        created_at=task.created_at,
        updated_at=task.updated_at
    )


@app.post("/admin/keep-alive/execute")
async def execute_keep_alive_manual(
    admin: Admin = Depends(get_current_admin)
):
    """手动执行保活任务"""
    global current_keep_alive_process
    
    async with keep_alive_process_lock:
        if current_keep_alive_process is not None:
            raise HTTPException(status_code=400, detail="保活任务正在执行中，请等待完成或先中断")
    
    logger.info(f"🔧 管理员 {admin.username} 手动触发保活任务")
    # 在后台执行，不阻塞响应
    asyncio.create_task(execute_keep_alive_task())
    return {"message": "保活任务已开始执行"}


@app.post("/admin/keep-alive/cancel")
async def cancel_keep_alive_task(
    admin: Admin = Depends(get_current_admin)
):
    """中断正在执行的保活任务"""
    global current_keep_alive_process
    
    async with keep_alive_process_lock:
        if current_keep_alive_process is None:
            raise HTTPException(status_code=400, detail="没有正在执行的保活任务")
        
        try:
            # 先发送中断信号，让子进程有机会清理浏览器
            logger.info("🛑 正在中断保活任务，等待浏览器关闭...")
            try:
                if sys.platform == 'win32':
                    # Windows 上尝试发送 SIGINT（如果支持）
                    try:
                        current_keep_alive_process.send_signal(signal.SIGINT)
                    except (AttributeError, ValueError):
                        # 如果不支持 send_signal，使用 terminate
                        current_keep_alive_process.terminate()
                else:
                    # Unix 系统上使用 SIGTERM
                    current_keep_alive_process.terminate()
            except Exception as e:
                logger.warning(f"发送中断信号失败: {e}，尝试直接终止")
                # 如果发送信号失败，直接终止
                current_keep_alive_process.terminate()
            
            try:
                # 等待5秒，让子进程有时间清理浏览器
                await asyncio.wait_for(
                    asyncio.to_thread(current_keep_alive_process.wait),
                    timeout=5
                )
                logger.info("✅ 保活任务已正常终止")
            except asyncio.TimeoutError:
                # 如果5秒后还没结束，强制杀死
                logger.warning("⚠️ 保活任务未在5秒内正常终止，强制终止进程...")
                try:
                    current_keep_alive_process.kill()
                    await asyncio.wait_for(
                        asyncio.to_thread(current_keep_alive_process.wait),
                        timeout=2
                    )
                except Exception as e:
                    logger.error(f"强制终止进程失败: {e}")
            
            # 无论子进程如何终止，都尝试关闭由 Selenium 启动的 Edge 浏览器窗口（Windows 上）
            # 注意：只关闭明确由 Selenium 启动的 Edge，不会关闭用户正在使用的 Edge
            if sys.platform == 'win32':
                try:
                    logger.info("🔍 检查并关闭残留的 Edge 浏览器窗口（仅限保活任务启动的）...")
                    closed_count = 0
                    
                    # 直接关闭 msedgedriver 进程及其所有子进程（包括 Edge 浏览器）
                    # 这是最快最可靠的方法，因为 Selenium 启动的 Edge 都是 msedgedriver 的子进程
                    try:
                        kill_driver_cmd = ['taskkill', '/F', '/IM', 'msedgedriver.exe', '/T']
                        kill_result = subprocess.run(
                            kill_driver_cmd,
                            capture_output=True,
                            text=True,
                            timeout=3,
                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                        )
                        if kill_result.returncode == 0:
                            # 统计关闭的进程数量（从输出中提取）
                            output = kill_result.stdout or ""
                            if "成功" in output or "successfully" in output.lower():
                                # 尝试从输出中提取数量
                                import re
                                match = re.search(r'(\d+)', output)
                                if match:
                                    closed_count = int(match.group(1))
                                else:
                                    closed_count = 1  # 至少关闭了 msedgedriver
                    except subprocess.TimeoutError:
                        logger.debug("关闭 msedgedriver 超时")
                    except Exception as e:
                        logger.debug(f"关闭 msedgedriver 时出错: {e}")
                    
                    if closed_count > 0:
                        time.sleep(0.5)  # 减少等待时间
                        logger.info(f"✅ 已关闭 {closed_count} 个由保活任务启动的 Edge 浏览器窗口")
                    else:
                        logger.info("ℹ️ 没有发现残留的 Edge 浏览器窗口（由保活任务启动的）")
                        
                except Exception as e:
                    logger.warning(f"⚠️ 尝试关闭 Edge 浏览器时出错: {e}")
                    logger.info("💡 如有残留的浏览器窗口，请手动关闭")
            
            # 更新日志状态
            db = next(get_db())
            try:
                # 找到最新的运行中的日志
                running_log = db.query(KeepAliveLog).filter(
                    KeepAliveLog.status == "running"
                ).order_by(KeepAliveLog.started_at.desc()).first()
                
                if running_log:
                    running_log.status = "cancelled"
                    running_log.finished_at = get_beijing_time()
                    running_log.message = "任务被管理员中断"
                    
                    # 更新所有运行中的账号日志
                    running_account_logs = db.query(KeepAliveAccountLog).filter(
                        KeepAliveAccountLog.task_log_id == running_log.id,
                        KeepAliveAccountLog.status == "running"
                    ).all()
                    for acc_log in running_account_logs:
                        acc_log.status = "cancelled"
                        acc_log.finished_at = get_beijing_time()
                        acc_log.message = "任务被中断"
                    
                    # 更新任务状态
                    task = db.query(KeepAliveTask).first()
                    if task:
                        task.last_status = "cancelled"
                        task.last_message = "任务被管理员中断"
                    
                    db.commit()
                
            finally:
                db.close()
            
            current_keep_alive_process = None
            
            # 如果保活任务已经更新了部分账号配置，重新加载账号配置
            try:
                reload_accounts_from_env_file()
                logger.info("🔄 中断保活任务后已重新加载账号配置")
            except Exception as e:
                logger.error(f"❌ 重新加载账号配置失败: {e}")
            
            logger.info(f"🛑 管理员 {admin.username} 中断了保活任务")
            return {"message": "保活任务已中断"}
            
        except Exception as e:
            logger.error(f"❌ 中断保活任务失败: {e}")
            raise HTTPException(status_code=500, detail=f"中断失败: {str(e)}")


class KeepAliveAccountLogResponse(BaseModel):
    id: int
    task_log_id: int
    account_name: str
    account_email: Optional[str]
    started_at: datetime
    finished_at: Optional[datetime]
    status: str
    message: Optional[str]


@app.get("/admin/keep-alive/logs", response_model=List[KeepAliveLogResponse])
async def get_keep_alive_logs(
    page: int = 1,
    page_size: int = 20,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取保活任务执行日志"""
    offset = (page - 1) * page_size
    logs = db.query(KeepAliveLog).order_by(
        KeepAliveLog.started_at.desc()
    ).offset(offset).limit(page_size).all()
    
    return [
        KeepAliveLogResponse(
            id=log.id,
            task_id=log.task_id,
            started_at=log.started_at,
            finished_at=log.finished_at,
            status=log.status,
            message=log.message,
            accounts_count=log.accounts_count,
            success_count=log.success_count,
            fail_count=log.fail_count
        )
        for log in logs
    ]


@app.get("/admin/keep-alive/logs/{log_id}/accounts", response_model=List[KeepAliveAccountLogResponse])
async def get_keep_alive_account_logs(
    log_id: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """获取指定任务日志的账号级别日志"""
    # 验证任务日志是否存在
    task_log = db.query(KeepAliveLog).filter(KeepAliveLog.id == log_id).first()
    if not task_log:
        raise HTTPException(status_code=404, detail="任务日志不存在")
    
    account_logs = db.query(KeepAliveAccountLog).filter(
        KeepAliveAccountLog.task_log_id == log_id
    ).order_by(KeepAliveAccountLog.started_at.asc()).all()
    
    return [
        KeepAliveAccountLogResponse(
            id=log.id,
            task_log_id=log.task_log_id,
            account_name=log.account_name,
            account_email=log.account_email,
            started_at=log.started_at,
            finished_at=log.finished_at,
            status=log.status,
            message=log.message
        )
        for log in account_logs
    ]


@app.delete("/admin/keep-alive/logs/{log_id}")
async def delete_keep_alive_log(
    log_id: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """删除指定的保活任务日志"""
    # 验证任务日志是否存在
    task_log = db.query(KeepAliveLog).filter(KeepAliveLog.id == log_id).first()
    if not task_log:
        raise HTTPException(status_code=404, detail="任务日志不存在")
    
    # 删除关联的账号级别日志
    db.query(KeepAliveAccountLog).filter(
        KeepAliveAccountLog.task_log_id == log_id
    ).delete()
    
    # 删除任务日志
    db.delete(task_log)
    db.commit()
    
    logger.info(f"🗑️ 管理员 {admin.username} 删除了保活任务日志 {log_id}")
    return {"message": "日志已删除"}


@app.post("/admin/accounts/reload")
async def reload_accounts(
    admin: Admin = Depends(get_current_admin)
):
    """手动重新加载账号配置"""
    try:
        reload_accounts_from_env_file()
        logger.info(f"🔄 管理员 {admin.username} 手动重新加载了账号配置")
        return {"message": "账号配置已重新加载"}
    except Exception as e:
        logger.error(f"❌ 重新加载账号配置失败: {e}")
        raise HTTPException(status_code=500, detail=f"重新加载失败: {str(e)}")


@app.post("/admin/keep-alive/logs/bulk-delete")
async def bulk_delete_keep_alive_logs(
    log_ids: List[int],
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """批量删除保活任务日志"""
    if not log_ids:
        raise HTTPException(status_code=400, detail="请选择要删除的日志")
    
    # 验证所有日志是否存在
    task_logs = db.query(KeepAliveLog).filter(
        KeepAliveLog.id.in_(log_ids)
    ).all()
    
    if len(task_logs) != len(log_ids):
        raise HTTPException(status_code=404, detail="部分日志不存在")
    
    # 删除关联的账号级别日志
    db.query(KeepAliveAccountLog).filter(
        KeepAliveAccountLog.task_log_id.in_(log_ids)
    ).delete(synchronize_session=False)
    
    # 删除任务日志
    for task_log in task_logs:
        db.delete(task_log)
    
    db.commit()
    
    logger.info(f"🗑️ 管理员 {admin.username} 批量删除了 {len(log_ids)} 条保活任务日志")
    return {"message": f"已删除 {len(log_ids)} 条日志"}


@app.get("/admin/keep-alive/status")
async def get_keep_alive_status(
    admin: Admin = Depends(get_current_admin)
):
    """获取保活任务当前状态（是否正在运行）"""
    global current_keep_alive_process
    
    async with keep_alive_process_lock:
        is_running = current_keep_alive_process is not None and current_keep_alive_process.poll() is None
    
    return {
        "is_running": is_running
    }


@app.post("/admin/auto-check/execute")
async def execute_auto_check_now(
    admin: Admin = Depends(get_current_admin)
):
    """立即执行自动检查任务"""
    try:
        # 在后台执行自动检查任务
        asyncio.create_task(execute_auto_check_task())
        return {"message": "自动检查任务已开始执行"}
    except Exception as e:
        logger.error(f"执行自动检查失败: {e}")
        raise HTTPException(status_code=500, detail=f"执行失败: {str(e)}")


@app.post("/admin/accounts/batch-check")
async def batch_check_accounts(
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """批量检查所有账号的 Cookie 状态（API 保活）"""
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    results = []
    check_time = get_beijing_time()
    
    for acc in accounts:
        test_acc = Account(
            name=acc["name"],
            secure_c_ses=acc["secure_c_ses"],
            csesidx=acc["csesidx"],
            config_id=acc["config_id"],
            host_c_oses=acc.get("host_c_oses", ""),
        )
        
        cookie_status = "unknown"
        error_msg = None
        expires_at = None
        
        try:
            jwt_token = await test_acc.jwt_mgr.get()
            if jwt_token:
                cookie_status = "valid"
                # 尝试获取 Cookie 过期时间（仅从响应头获取，不估算）
                if hasattr(test_acc, '_cookie_expires_at') and test_acc._cookie_expires_at:
                    expires_at = test_acc._cookie_expires_at
                # 如果无法从响应头获取，expires_at 保持为 None
                
                result_item = {
                    "index": acc["index"],
                    "name": acc["name"],
                    "cookie_status": cookie_status,
                    "last_check_at": check_time,
                    "expires_at": expires_at,
                    "error_message": None
                }
            else:
                cookie_status = "unknown"
                error_msg = "无法获取 JWT"
                result_item = {
                    "index": acc["index"],
                    "name": acc["name"],
                    "cookie_status": cookie_status,
                    "last_check_at": check_time,
                    "expires_at": None,
                    "error_message": error_msg
                }
        except HTTPException as e:
            error_msg = str(e.detail) if hasattr(e, 'detail') else str(e)
            if e.status_code == 401:
                cookie_status = "expired"
                error_msg = "Cookie 已过期"
            elif e.status_code == 403:
                cookie_status = "forbidden"
                error_msg = "Cookie 无效或被禁止"
            elif e.status_code == 429:
                cookie_status = "rate_limited"
                error_msg = "请求过于频繁"
            
            result_item = {
                "index": acc["index"],
                "name": acc["name"],
                "cookie_status": cookie_status,
                "last_check_at": check_time,
                "expires_at": None,
                "error_message": error_msg
            }
        except Exception as e:
            error_msg = str(e)
            result_item = {
                "index": acc["index"],
                "name": acc["name"],
                "cookie_status": "unknown",
                "last_check_at": check_time,
                "expires_at": None,
                "error_message": error_msg
            }
        
        results.append(result_item)
        
        # 保存到数据库
        try:
            account_status = db.query(AccountCookieStatus).filter(
                AccountCookieStatus.account_name == acc["name"]
            ).first()
            
            if account_status:
                # 更新现有记录
                account_status.cookie_status = cookie_status
                account_status.last_check_at = check_time
                # 只有在 Cookie 有效时才更新过期时间，避免覆盖有效的过期时间
                if cookie_status == "valid" and expires_at:
                    account_status.expires_at = expires_at
                account_status.error_message = error_msg
                account_status.updated_at = check_time
            else:
                # 创建新记录
                account_status = AccountCookieStatus(
                    account_name=acc["name"],
                    cookie_status=cookie_status,
                    last_check_at=check_time,
                    expires_at=expires_at if cookie_status == "valid" else None,
                    error_message=error_msg
                )
                db.add(account_status)
        except Exception as e:
            logger.error(f"保存账号 Cookie 状态失败 [{acc['name']}]: {e}")
    
    # 批量提交
    try:
        db.commit()
    except Exception as e:
        logger.error(f"批量保存 Cookie 状态失败: {e}")
        db.rollback()
    
    return {"results": results}


@app.post("/admin/accounts/{account_index}/test")
async def test_account(
    account_index: int,
    admin: Admin = Depends(get_current_admin),
    db: Session = Depends(get_db)
):
    """测试账号是否可用（API 保活检查）"""
    lines = read_env_file()
    accounts = parse_accounts_from_env_lines(lines)
    
    # 找到目标账号
    target_account = None
    for acc in accounts:
        if acc["index"] == account_index:
            target_account = acc
            break
    
    if not target_account:
        raise HTTPException(status_code=404, detail="账号不存在")
    
    # 创建临时账号对象进行测试
    test_acc = Account(
        name=target_account["name"],
        secure_c_ses=target_account["secure_c_ses"],
        csesidx=target_account["csesidx"],
        config_id=target_account["config_id"],
        host_c_oses=target_account.get("host_c_oses", ""),
    )
    
    check_time = get_beijing_time()
    cookie_status = "unknown"
    error_msg = None
    expires_at = None
    
    try:
        # 尝试获取 JWT（这会验证 Cookie 是否有效）
        jwt_token = await test_acc.jwt_mgr.get()
        if jwt_token:
            cookie_status = "valid"
            
            # 尝试获取 Cookie 过期时间（仅从响应头获取，不估算）
            if hasattr(test_acc, '_cookie_expires_at') and test_acc._cookie_expires_at:
                expires_at = test_acc._cookie_expires_at
            # 如果无法从响应头获取，expires_at 保持为 None
            
            result = {
                "status": "success",
                "message": "账号测试成功，Cookie 有效",
                "cookie_status": cookie_status,
                "last_check_at": check_time,
                "expires_at": expires_at,
                "error_message": None
            }
        else:
            cookie_status = "unknown"
            error_msg = "无法获取 JWT"
            result = {
                "status": "error",
                "message": "无法获取 JWT",
                "cookie_status": cookie_status,
                "last_check_at": check_time,
                "expires_at": None,
                "error_message": error_msg
            }
    except HTTPException as e:
        # 根据错误码判断 Cookie 状态
        error_msg = str(e.detail) if hasattr(e, 'detail') else str(e)
        
        if e.status_code == 401:
            cookie_status = "expired"
            error_msg = "Cookie 已过期，需要重新登录"
        elif e.status_code == 403:
            cookie_status = "forbidden"
            error_msg = "Cookie 无效或被禁止访问"
        elif e.status_code == 429:
            cookie_status = "rate_limited"
            error_msg = "请求过于频繁，请稍后再试"
        
        logger.error(f"账号测试失败 [{target_account['name']}]: {e.status_code} - {error_msg}")
        result = {
            "status": "error",
            "message": f"账号测试失败: {error_msg}",
            "cookie_status": cookie_status,
            "last_check_at": check_time,
            "expires_at": None,
            "error_message": error_msg
        }
    except Exception as e:
        logger.error(f"账号测试失败: {e}")
        error_msg = str(e)
        result = {
            "status": "error",
            "message": f"账号测试失败: {error_msg}",
            "cookie_status": "unknown",
            "last_check_at": check_time,
            "expires_at": None,
            "error_message": error_msg
        }
    
    # 保存到数据库
    try:
        account_status = db.query(AccountCookieStatus).filter(
            AccountCookieStatus.account_name == target_account["name"]
        ).first()
        
        if account_status:
            # 更新现有记录
            account_status.cookie_status = cookie_status
            account_status.last_check_at = check_time
            # 只有在 Cookie 有效时才更新过期时间，避免覆盖有效的过期时间
            if cookie_status == "valid" and expires_at:
                account_status.expires_at = expires_at
            account_status.error_message = error_msg
            account_status.updated_at = check_time
        else:
            # 创建新记录
            account_status = AccountCookieStatus(
                account_name=target_account["name"],
                cookie_status=cookie_status,
                last_check_at=check_time,
                expires_at=expires_at if cookie_status == "valid" else None,
                error_message=error_msg
            )
            db.add(account_status)
        
        db.commit()
    except Exception as e:
        logger.error(f"保存账号 Cookie 状态失败: {e}")
        db.rollback()
    
    return result


# ---------- API 密钥验证中间件 ----------
async def verify_api_key_middleware(request: Request, call_next):
    """验证 API 密钥"""
    from fastapi.responses import JSONResponse
    import time as time_module
    
    # 只对 /v1/ 开头的路径进行验证
    if request.url.path.startswith("/v1/"):
        auth_header = request.headers.get("Authorization")
        start_time = time_module.time()
        
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "缺少 API 密钥"}
            )
        
        api_key = auth_header.replace("Bearer ", "")
        key_hash = hash_api_key(api_key)
        
        # 获取数据库会话
        db = next(get_db())
        db_key = None
        client_ip = request.client.host if request.client else "unknown"
        
        try:
            db_key = db.query(APIKey).filter(APIKey.key_hash == key_hash).first()
            
            if not db_key:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "无效的 API 密钥"}
                )
            
            if not db_key.is_active:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "API 密钥已被撤销"}
                )
            
            # 确保 expires_at 是 aware datetime 以便比较
            expires_at = ensure_aware(db_key.expires_at)
            if expires_at < get_beijing_time():
                return JSONResponse(
                    status_code=401,
                    content={"detail": "API 密钥已过期"}
                )
            
            # 更新使用统计（存储时使用 naive datetime）
            db_key.usage_count += 1
            db_key.last_used_at = ensure_naive(get_beijing_time())
            db.commit()
            
            # 存储到请求状态，用于后续记录
            request.state.api_key_id = db_key.id
            request.state.start_time = start_time
            request.state.client_ip = client_ip
            
        except Exception as e:
            logger.error(f"API key validation error: {e}")
            return JSONResponse(
                status_code=500,
                content={"detail": "服务器内部错误"}
            )
        finally:
            db.close()
    
    # 执行实际请求
    response = await call_next(request)
    
    # 记录调用日志
    if request.url.path.startswith("/v1/") and hasattr(request.state, "api_key_id"):
        db = next(get_db())
        try:
            # 尝试从请求体获取模型信息
            model = "unknown"
            if hasattr(request.state, "model"):
                model = request.state.model
            
            response_time = int((time_module.time() - request.state.start_time) * 1000)
            
            log_entry = APICallLog(
                api_key_id=request.state.api_key_id,
                timestamp=ensure_naive(get_beijing_time()),
                model=model,
                status="success" if response.status_code < 400 else "error",
                error_message=None if response.status_code < 400 else f"HTTP {response.status_code}",
                ip_address=request.state.client_ip,
                endpoint=request.url.path,
                response_time=response_time
            )
            db.add(log_entry)
            db.commit()
            
            # 广播更新消息
            await manager.broadcast("update")
        except Exception as e:
            logger.error(f"Failed to log API call: {e}")
        finally:
            db.close()
    
    return response

app.middleware("http")(verify_api_key_middleware)


@app.post("/admin/chat/completions")
async def admin_chat(
    req: ChatRequest,
    admin: Admin = Depends(get_current_admin)
):
    """管理员专用的聊天接口，无需 API 密钥"""
    # 复用现有的聊天逻辑，但不记录到 API 调用日志
    request = Request({"type": "http", "method": "POST", "path": "/admin/chat/completions"})
    request.state.model = req.model
    
    # 1. 模型校验
    if req.model not in MODEL_MAPPING:
        raise HTTPException(
            status_code=404, detail=f"Model '{req.model}' not found."
        )

    if ACCOUNT_POOL is None:
        raise HTTPException(
            status_code=500,
            detail="No Gemini business accounts configured",
        )

    # 2. 解析请求内容
    last_text, current_images = parse_last_message(req.messages)

    # 3. 锚定对话 & 账号
    conv_key = get_conversation_key([m.dict() for m in req.messages])
    account = ACCOUNT_POOL.get_for_conversation(conv_key)
    cached = SESSION_CACHE.get(conv_key)

    if cached:
        google_session = cached["session_id"]
        text_to_send = last_text
        logger.info(
            f"♻️ 管理员对话延续旧对话[{req.model}] 账号={account.name} session={google_session[-12:]}"
        )
        cached["updated_at"] = time.time()
        is_retry_mode = False
    else:
        logger.info(f"🆕 管理员开启新对话 [{req.model}] 使用账号 {account.name}")

        google_session: Optional[str] = None
        first_error: Optional[Exception] = None
        tried_accounts = set()

        while True:
            tried_accounts.add(account.name)
            try:
                google_session = await create_google_session(account)
                break
            except HTTPException as e:
                if e.status_code in (401, 403, 429):
                    account.mark_quota_error(e.status_code, str(e.detail))
                    alt = ACCOUNT_POOL.get_alternative(account.name)
                    if not alt or alt.name in tried_accounts:
                        if first_error:
                            raise first_error
                        raise e
                    first_error = e
                    account = alt
                    continue
                raise

        SESSION_CACHE[conv_key] = {
            "session_id": google_session,
            "updated_at": time.time(),
            "account": account.name,
        }
        text_to_send = build_full_context_text(req.messages)
        is_retry_mode = True

    chat_id = f"chatcmpl-admin-{uuid.uuid4().hex[:8]}"
    created_time = int(time.time())

    # 4. 封装生成逻辑
    async def response_wrapper():
        nonlocal account

        retry_count = 0
        max_retries = 2

        current_text = text_to_send
        current_retry_mode = is_retry_mode
        current_file_ids: List[str] = []

        while retry_count <= max_retries:
            try:
                cached_session = SESSION_CACHE.get(conv_key)
                if not cached_session:
                    new_sess = await create_google_session(account)
                    SESSION_CACHE[conv_key] = {
                        "session_id": new_sess,
                        "updated_at": time.time(),
                        "account": account.name,
                    }
                    cached_session = SESSION_CACHE[conv_key]

                current_session = cached_session["session_id"]

                # A. 如果有图片且还没上传到当前 Session，先上传
                if current_images and not current_file_ids:
                    for img in current_images:
                        fid = await upload_context_file(
                            account, current_session, img["mime"], img["data"]
                        )
                        current_file_ids.append(fid)

                # B. 准备文本 (重试模式下发全文)
                if current_retry_mode:
                    current_text = build_full_context_text(req.messages)

                # C. 发起对话
                async for chunk in stream_chat_generator(
                    account,
                    current_session,
                    current_text,
                    current_file_ids,
                    req.model,
                    chat_id,
                    created_time,
                    req.stream,
                ):
                    yield chunk
                break

            except (httpx.ConnectError, httpx.ReadTimeout, ssl.SSLError,
                    asyncio.TimeoutError) as e:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"网络错误重试失败 [{account.name}]: {e}")
                    raise HTTPException(status_code=503, detail="Service Unavailable")
                logger.warning(f"网络错误，重试 {retry_count}/{max_retries} [{account.name}]")
                await asyncio.sleep(1)
                continue

            except HTTPException as e:
                if e.status_code in (401, 403, 429):
                    account.mark_quota_error(e.status_code, str(e.detail))
                    alt = ACCOUNT_POOL.get_alternative(account.name)
                    if alt and alt.name != account.name:
                        logger.info(f"切换到备用账号: {alt.name}")
                        account = alt
                        retry_count = 0
                        continue
                raise

    if req.stream:
        return StreamingResponse(
            response_wrapper(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # 非流式响应
    full_content = ""
    async for chunk_str in response_wrapper():
        if chunk_str.startswith("data: [DONE]"):
            break
        if chunk_str.startswith("data: "):
            try:
                data = json.loads(chunk_str[6:])
                delta = data["choices"][0]["delta"]
                if "content" in delta:
                    full_content += delta["content"]
            except Exception:
                pass

    response_data = {
        "id": chat_id,
        "object": "chat.completion",
        "created": created_time,
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": full_content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }

    return response_data


@app.post("/v1/chat/completions")
async def chat(req: ChatRequest, request: Request):
    # 记录模型到请求状态
    request.state.model = req.model
    
    # 1. 模型校验
    if req.model not in MODEL_MAPPING:
        raise HTTPException(
            status_code=404, detail=f"Model '{req.model}' not found."
        )

    if ACCOUNT_POOL is None:
        raise HTTPException(
            status_code=500,
            detail="No Gemini business accounts configured",
        )

    # 2. 解析请求内容
    last_text, current_images = parse_last_message(req.messages)

    # 3. 锚定对话 & 账号
    conv_key = get_conversation_key([m.dict() for m in req.messages])
    account = ACCOUNT_POOL.get_for_conversation(conv_key)
    cached = SESSION_CACHE.get(conv_key)

    if cached:
        google_session = cached["session_id"]
        text_to_send = last_text
        logger.info(
            f"♻️ 延续旧对话[{req.model}] 账号={account.name} session={google_session[-12:]}"
        )
        cached["updated_at"] = time.time()
        is_retry_mode = False
    else:
        logger.info(f"🆕 开启新对话 [{req.model}] 使用账号 {account.name}")

        google_session: Optional[str] = None
        first_error: Optional[Exception] = None
        tried_accounts = set()

        while True:
            tried_accounts.add(account.name)
            try:
                google_session = await create_google_session(account)
                break
            except HTTPException as e:
                if e.status_code in (401, 403, 429):
                    account.mark_quota_error(e.status_code, str(e.detail))
                    alt = ACCOUNT_POOL.get_alternative(account.name)
                    if not alt or alt.name in tried_accounts:
                        first_error = e
                        break
                    logger.info(
                        f"createSession 配额受限，切换账号 {account.name} -> {alt.name}"
                    )
                    account = alt
                    continue
                first_error = e
                break
            except Exception as e:
                first_error = e
                break

        if not google_session:
            if isinstance(first_error, HTTPException):
                raise first_error
            raise HTTPException(status_code=500, detail="No available account")

        text_to_send = build_full_context_text(req.messages)
        SESSION_CACHE[conv_key] = {
            "session_id": google_session,
            "updated_at": time.time(),
            "account": account.name,
        }
        is_retry_mode = True

    chat_id = f"chatcmpl-{uuid.uuid4()}"
    created_time = int(time.time())

    # 4. 封装生成逻辑（含图片上传、重试和账号切换）
    async def response_wrapper():
        nonlocal account

        retry_count = 0
        max_retries = 2

        current_text = text_to_send
        current_retry_mode = is_retry_mode
        current_file_ids: List[str] = []

        while retry_count <= max_retries:
            try:
                cached_session = SESSION_CACHE.get(conv_key)
                if not cached_session:
                    new_sess = await create_google_session(account)
                    SESSION_CACHE[conv_key] = {
                        "session_id": new_sess,
                        "updated_at": time.time(),
                        "account": account.name,
                    }
                    cached_session = SESSION_CACHE[conv_key]

                current_session = cached_session["session_id"]

                # A. 如果有图片且还没上传到当前 Session，先上传
                if current_images and not current_file_ids:
                    for img in current_images:
                        fid = await upload_context_file(
                            account, current_session, img["mime"], img["data"]
                        )
                        current_file_ids.append(fid)

                # B. 准备文本 (重试模式下发全文)
                if current_retry_mode:
                    current_text = build_full_context_text(req.messages)

                # C. 发起对话
                async for chunk in stream_chat_generator(
                    account,
                    current_session,
                    current_text,
                    current_file_ids,
                    req.model,
                    chat_id,
                    created_time,
                    req.stream,
                ):
                    yield chunk
                break

            except (httpx.ConnectError, httpx.ReadTimeout, ssl.SSLError, HTTPException) as e:
                retry_count += 1
                logger.warning(
                    f"⚠️ 请求异常 (重试 {retry_count}/{max_retries}) 账号={account.name}: {e}"
                )

                status_code = getattr(e, "status_code", None)

                # 先判定配额/权限类错误，尝试切换账号
                if isinstance(e, HTTPException) and status_code in (401, 403, 429):
                    account.mark_quota_error(status_code, str(e.detail))
                    alt = ACCOUNT_POOL.get_alternative(account.name)
                    if alt:
                        logger.info(f"🔁 切换到备用账号 {alt.name}")
                        account = alt
                        try:
                            new_sess = await create_google_session(account)
                            SESSION_CACHE[conv_key] = {
                                "session_id": new_sess,
                                "updated_at": time.time(),
                                "account": account.name,
                            }
                            current_retry_mode = True
                            current_file_ids = []
                            continue
                        except Exception as create_err:
                            logger.error(
                                f"备用账号创建 Session 失败: {create_err}"
                            )
                            if req.stream:
                                yield "data: " + json.dumps(
                                    {
                                        "error": {
                                            "message": "All accounts exhausted"
                                        }
                                    }
                                ) + "\n\n"
                                return
                            raise

                # 非配额错误或切换失败，尝试当前账号重建 session
                if retry_count <= max_retries:
                    logger.info("🔄 尝试重建 Session...")
                    try:
                        new_sess = await create_google_session(account)
                        SESSION_CACHE[conv_key] = {
                            "session_id": new_sess,
                            "updated_at": time.time(),
                            "account": account.name,
                        }
                        current_retry_mode = True
                        current_file_ids = []
                    except Exception as create_err:
                        logger.error(f"Session 重建失败: {create_err}")
                        if req.stream:
                            yield "data: " + json.dumps(
                                {
                                    "error": {
                                        "message": "Session Recovery Failed"
                                    }
                                }
                            ) + "\n\n"
                            return
                        raise
                else:
                    if req.stream:
                        yield "data: " + json.dumps(
                            {"error": {"message": f"Final Error: {e}"}}
                        ) + "\n\n"
                        return
                    raise

    if req.stream:
        return StreamingResponse(
            response_wrapper(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    full_content = ""
    async for chunk_str in response_wrapper():
        if chunk_str.startswith("data: [DONE]"):
            break
        if chunk_str.startswith("data: "):
            try:
                data = json.loads(chunk_str[6:])
                delta = data["choices"][0]["delta"]
                if "content" in delta:
                    full_content += delta["content"]
            except Exception:
                pass

    response_data = {
        "id": chat_id,
        "object": "chat.completion",
        "created": created_time,
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": full_content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }

    return response_data


async def stream_chat_generator(
    account: Account,
    session: str,
    text_content: str,
    file_ids: List[str],
    model_name: str,
    chat_id: str,
    created_time: int,
    is_stream: bool = True,
):
    jwt = await account.jwt_mgr.get()
    headers = get_common_headers(jwt)

    body: Dict[str, Any] = {
        "configId": account.config_id,
        "additionalParams": {"token": "-"},
        "streamAssistRequest": {
            "session": session,
            "query": {"parts": [{"text": text_content}]},
            "filter": "",
            "fileIds": file_ids,
            "answerGenerationMode": "NORMAL",
            "toolsSpec": {
                "webGroundingSpec": {},
                "toolRegistry": "default_tool_registry",
                "imageGenerationSpec": {},
                "videoGenerationSpec": {},
            },
            "languageCode": "zh-CN",
            "userMetadata": {"timeZone": "Asia/Shanghai"},
            "assistSkippingMode": "REQUEST_ASSIST",
        },
    }

    target_model_id = MODEL_MAPPING.get(model_name)
    if target_model_id:
        body["streamAssistRequest"]["assistGenerationConfig"] = {
            "modelId": target_model_id
        }

    if is_stream:
        chunk = create_chunk(
            chat_id, created_time, model_name, {"role": "assistant"}, None
        )
        yield f"data: {chunk}\n\n"

    r = await http_client.post(
        "https://biz-discoveryengine.googleapis.com/v1alpha/locations/global/widgetStreamAssist",
        headers=headers,
        json=body,
    )

    if r.status_code != 200:
        logger.error(
            f"widgetStreamAssist 失败 [{account.name}]: {r.status_code} {r.text}"
        )
        if r.status_code in (401, 403, 429):
            account.mark_quota_error(r.status_code, r.text)
        raise HTTPException(status_code=r.status_code, detail=f"Upstream Error {r.text}")

    try:
        data_list = r.json()
    except Exception as e:  # noqa: BLE001
        logger.error(f"JSON 解析失败 [{account.name}]: {e}")
        raise HTTPException(status_code=502, detail="Invalid JSON response")

    # ========== 第一步：收集文本内容和思考过程 ==========
    full_content = ""
    generated_images: List[ChatImage] = []
    thinking_parts = []

    for data in data_list:
        for reply in (
            data.get("streamAssistResponse", {})
            .get("answer", {})
            .get("replies", [])
        ):
            # 提取 API 返回的 thought 字段
            thought = reply.get("thought", "")
            if thought:
                thinking_parts.append(thought)
            
            # 提取正文内容
            text = (
                reply.get("groundedContent", {})
                .get("content", {})
                .get("text", "")
            )
            if text and not thought:
                full_content += text

    # ========== 第二步：从正文中提取 **标题** 格式的思考标题 ==========
    lines = full_content.splitlines()
    filtered_lines = []
    thinking_titles = []
    
    for line in lines:
        # 匹配单独成行的 **标题** 格式
        m = re.match(r'^\s*\*\*([^*]+)\*\*\s*$', line)
        if m:
            title_text = m.group(1).strip()
            # 判断是否是思考标题：英文短语，长度适中，不超过6个单词
            if (re.match(r'^[A-Za-z\s\'\-\(\)]+$', title_text) and 
                len(title_text) < 100 and 
                len(title_text.split()) <= 6):
                thinking_titles.append(title_text)
                continue  # 跳过思考标题行
        filtered_lines.append(line)
    
    if thinking_titles:
        thinking_parts.extend(thinking_titles)
    
    # 更新正文内容（已移除思考标题）
    full_content = "\n".join(filtered_lines).strip()

    # ========== 第三步：处理并发送思考过程 ==========
    if thinking_parts:
        thinking_content = "\n\n".join(thinking_parts)
        thinking_html = f"<details><summary>显示思路</summary>\n\n{thinking_content}\n\n</details>\n\n"
        full_content = thinking_html + full_content
        
        if is_stream:
            # 发送 thinking 字段（兼容支持该字段的前端）
            thinking_chunk = create_chunk(
                chat_id, created_time, model_name, {"thinking": thinking_content}, None
            )
            yield f"data: {thinking_chunk}\n\n"
        logger.info(f"📝 提取到 {len(thinking_parts)} 个思考步骤 [{account.name}]")

    # ========== 第四步：流式发送正文内容（逐字输出） ==========
    if full_content:
        if is_stream:
            # 流式输出：将内容分块发送，模拟逐字输出
            # 使用较小的分块和适当的延迟，实现平滑的流式效果
            chunk_size = 4  # 每次发送4个字符，平衡流畅度和性能
            sent_length = 0
            total_chunks = 0
            
            while sent_length < len(full_content):
                # 计算本次要发送的内容
                end_pos = min(sent_length + chunk_size, len(full_content))
                delta_content = full_content[sent_length:end_pos]
                
                if delta_content:
                    chunk = create_chunk(
                        chat_id, created_time, model_name, {"content": delta_content}, None
                    )
                    yield f"data: {chunk}\n\n"
                    sent_length = end_pos
                    total_chunks += 1
                    # 添加延迟，模拟真实流式输出（30ms，约每秒33个字符）
                    # 这个速度既不会太快也不会太慢，适合大多数客户端
                    await asyncio.sleep(0.03)
            
            logger.debug(f"📤 流式发送完成: {total_chunks} 个数据块, 总长度 {len(full_content)} 字符")
        else:
            # 非流式：一次性发送完整内容
            chunk = create_chunk(
                chat_id, created_time, model_name, {"content": full_content}, None
            )
            yield f"data: {chunk}\n\n"

    # ========== 第五步：解析并下载生成的图片 ==========
    image_file_ids, response_session = parse_images_from_response(data_list)
    
    if response_session and response_session != session:
        logger.info(f"🔄 检测到新 Session [{account.name}]: ...{response_session[-15:]}")

    if image_file_ids:
        logger.info(f"🖼️  检测到 {len(image_file_ids)} 个生成图片 [{account.name}]")
        session_for_download = response_session or session

        try:
            file_metadata = await get_session_file_metadata(account, session_for_download)
            existing_image_count = 0

            for idx, finfo in enumerate(image_file_ids):
                fid = finfo["fileId"]
                mime = finfo["mimeType"]
                meta = file_metadata.get(fid, {})
                file_name = meta.get("name")
                session_path = meta.get("session") or session_for_download

                image_index = existing_image_count + idx + 1
                img = await save_generated_image(account, session_path, fid, file_name, mime, chat_id, image_index)
                generated_images.append(img)

        except Exception as e:
            logger.error(f"❌ 处理生成图片失败 [{account.name}]: {e}", exc_info=True)

    # ========== 第六步：将图片链接添加到文本内容中 ==========
    if generated_images:
        image_markdown = "\n\n"
        for img in generated_images:
            if img.base64_data:
                image_markdown += f"![生成的图片](data:{img.mime_type};base64,{img.base64_data})\n\n"
        
        if image_markdown.strip():
            full_content += image_markdown
            if is_stream:
                chunk = create_chunk(
                    chat_id, created_time, model_name, {"content": image_markdown}, None
                )
                yield f"data: {chunk}\n\n"
            logger.info(f"✅ 已将 {len(generated_images)} 张图片添加到回复中 [{account.name}]")

    if is_stream:
        final_chunk = create_chunk(
            chat_id, created_time, model_name, {}, "stop"
        )
        yield f"data: {final_chunk}\n\n"
        yield "data: [DONE]\n\n"


# ---------- 保活任务调度器 ----------
scheduler = AsyncIOScheduler(timezone=BEIJING_TZ)

# 当前运行的保活任务进程（用于中断）
current_keep_alive_process: Optional[subprocess.Popen] = None
keep_alive_process_lock = asyncio.Lock()


async def execute_api_keepalive_task():
    """执行 API 保活任务 - 通过调用 Gemini API 保持会话活跃"""
    db = next(get_db())
    try:
        task = db.query(KeepAliveTask).first()
        if not task or not getattr(task, 'api_keepalive_enabled', True):
            logger.debug("API 保活任务已禁用，跳过执行")
            return
        
        logger.info("🔄 开始执行 API 保活任务...")
        
        if ACCOUNT_POOL is None or not ACCOUNT_POOL.accounts:
            logger.warning("⚠️ 没有可用账号，跳过 API 保活")
            return
        
        # 重新加载账号配置
        try:
            reload_accounts_from_env_file()
        except Exception as e:
            logger.warning(f"⚠️ 重新加载账号配置失败: {e}")
        
        success_count = 0
        fail_count = 0
        total_accounts = len(ACCOUNT_POOL.accounts)
        invalid_accounts = []  # 记录无效的账号，用于自动修复
        check_time = ensure_naive(get_beijing_time())  # 转换为 naive datetime 用于数据库存储
        
        # 对每个账号执行一次简单的 API 调用
        for account in ACCOUNT_POOL.accounts:
            if not account.is_available():
                logger.debug(f"⏭️ 跳过不可用账号: {account.name}")
                fail_count += 1
                continue
            
            cookie_status = "unknown"
            error_msg = None
            expires_at = None
            
            try:
                # 只验证 JWT 是否有效，通过刷新 JWT 来保持会话活跃
                # 这种方式更轻量，不会产生实际的 API 调用
                jwt = await account.jwt_mgr.get()
                if jwt:
                    cookie_status = "valid"
                    success_count += 1
                    logger.debug(f"✅ API 保活成功: {account.name} (JWT 刷新)")
                    
                    # 尝试获取 Cookie 过期时间（仅从响应头获取，不估算）
                    if hasattr(account, '_cookie_expires_at') and account._cookie_expires_at:
                        expires_at = account._cookie_expires_at
                else:
                    cookie_status = "unknown"
                    error_msg = "无法获取 JWT"
                    fail_count += 1
                    logger.warning(f"⚠️ API 保活失败 [{account.name}]: 无法获取 JWT")
                    invalid_accounts.append(account.name)
                        
            except HTTPException as e:
                fail_count += 1
                error_msg = str(e.detail) if hasattr(e, 'detail') else str(e)
                if e.status_code in (401, 403):
                    # Cookie 无效，记录到待修复列表
                    if e.status_code == 401:
                        cookie_status = "expired"
                        error_msg = "Cookie 已过期"
                    elif e.status_code == 403:
                        cookie_status = "forbidden"
                        error_msg = "Cookie 无效或被禁止"
                    account.mark_quota_error(e.status_code, str(e.detail))
                    invalid_accounts.append(account.name)
                    logger.warning(f"⚠️ Cookie 无效 [{account.name}]: {e.status_code} - 将自动修复")
                elif e.status_code == 429:
                    cookie_status = "rate_limited"
                    error_msg = "请求过于频繁"
                    account.mark_quota_error(e.status_code, str(e.detail))
                    logger.warning(f"⚠️ API 保活失败 [{account.name}]: 请求过于频繁")
                else:
                    logger.warning(f"⚠️ API 保活失败 [{account.name}]: {e.status_code}")
            except Exception as e:
                fail_count += 1
                error_msg = str(e)
                logger.warning(f"⚠️ API 保活异常 [{account.name}]: {str(e)}")
            
            # 保存检查结果到数据库
            try:
                account_status = db.query(AccountCookieStatus).filter(
                    AccountCookieStatus.account_name == account.name
                ).first()
                
                if account_status:
                    # 更新现有记录
                    account_status.cookie_status = cookie_status
                    account_status.last_check_at = check_time
                    # 只有在 Cookie 有效时才更新过期时间，避免覆盖有效的过期时间
                    if cookie_status == "valid" and expires_at:
                        account_status.expires_at = expires_at
                    account_status.error_message = error_msg
                    account_status.updated_at = check_time
                else:
                    # 创建新记录
                    account_status = AccountCookieStatus(
                        account_name=account.name,
                        cookie_status=cookie_status,
                        last_check_at=check_time,
                        expires_at=expires_at if cookie_status == "valid" else None,
                        error_message=error_msg
                    )
                    db.add(account_status)
            except Exception as e:
                logger.error(f"保存账号 Cookie 状态失败 [{account.name}]: {e}")
        
        # 批量提交所有账号状态更新
        try:
            db.commit()
            logger.debug(f"✅ 已保存 {total_accounts} 个账号的 Cookie 状态到数据库")
        except Exception as e:
            logger.error(f"批量保存 Cookie 状态失败: {e}")
            db.rollback()
        
        # 如果启用了自动修复，且检测到无效账号，则调用浏览器保活来修复
        if invalid_accounts and getattr(task, 'auto_check_auto_fix', True):
            logger.info(f"🔧 检测到 {len(invalid_accounts)} 个无效账号，开始自动修复: {', '.join(invalid_accounts)}")
            try:
                # 调用浏览器保活来修复这些账号
                # 注意：这里只修复无效的账号，有效的账号不处理
                await execute_keep_alive_task_for_accounts(invalid_accounts)
                logger.info(f"✅ 自动修复完成: {len(invalid_accounts)} 个账号")
            except Exception as e:
                logger.error(f"❌ 自动修复失败: {e}")
        
        # 更新任务状态（与 Cookie 状态一起提交）
        task.last_api_keepalive_at = get_beijing_time()
        task.last_message = f"API 保活完成: 成功 {success_count}/{total_accounts}"
        
        # 再次提交任务状态（如果之前已经提交过，这里会更新任务状态）
        try:
            db.commit()
            logger.info(f"✅ API 保活任务完成: 成功 {success_count}/{total_accounts}，已更新 Cookie 状态")
        except Exception as e:
            logger.error(f"更新任务状态失败: {e}")
            db.rollback()
        
    except Exception as e:
        logger.error(f"❌ API 保活任务执行失败: {e}")
        if task:
            task.last_message = f"API 保活失败: {str(e)}"
            db.commit()
    finally:
        db.close()


async def execute_keep_alive_task_for_accounts(account_names: List[str] = None):
    """
    执行保活任务（仅针对指定的账号）
    
    Args:
        account_names: 要处理的账号名称列表，如果为 None 则处理所有账号
    """
    global current_keep_alive_process
    
    # 如果指定了账号列表，需要修改 keep_alive_env.py 来只处理这些账号
    # 这里我们通过环境变量传递账号列表
    if account_names:
        import json
        os.environ['KEEP_ALIVE_TARGET_ACCOUNTS'] = json.dumps(account_names)
        logger.info(f"🎯 保活任务将只处理以下账号: {', '.join(account_names)}")
    else:
        os.environ.pop('KEEP_ALIVE_TARGET_ACCOUNTS', None)
    
    # 调用原有的保活任务
    await execute_keep_alive_task()


async def execute_auto_check_task():
    """执行自动检查任务 - 检查所有账号的 Cookie 状态，如果无效则自动修复"""
    db = next(get_db())
    try:
        task = db.query(KeepAliveTask).first()
        if not task or not getattr(task, 'auto_check_enabled', False):
            logger.debug("自动检查任务已禁用，跳过执行")
            return
        
        logger.info("🔍 开始执行自动检查任务...")
        
        if ACCOUNT_POOL is None or not ACCOUNT_POOL.accounts:
            logger.warning("⚠️ 没有可用账号，跳过自动检查")
            return
        
        # 重新加载账号配置
        try:
            reload_accounts_from_env_file()
        except Exception as e:
            logger.warning(f"⚠️ 重新加载账号配置失败: {e}")
        
        invalid_accounts = []  # 记录无效的账号
        valid_count = 0
        total_accounts = len(ACCOUNT_POOL.accounts)
        check_time = ensure_naive(get_beijing_time())  # 转换为 naive datetime 用于数据库存储
        
        # 检查每个账号的 Cookie 状态
        for account in ACCOUNT_POOL.accounts:
            if not account.is_available():
                logger.debug(f"⏭️ 跳过不可用账号: {account.name}")
                continue
            
            cookie_status = "unknown"
            error_msg = None
            expires_at = None
            
            try:
                # 验证 JWT 是否有效
                jwt = await account.jwt_mgr.get()
                if jwt:
                    cookie_status = "valid"
                    valid_count += 1
                    logger.debug(f"✅ Cookie 有效: {account.name}")
                    
                    # 尝试获取 Cookie 过期时间（仅从响应头获取，不估算）
                    if hasattr(account, '_cookie_expires_at') and account._cookie_expires_at:
                        expires_at = account._cookie_expires_at
                else:
                    cookie_status = "unknown"
                    error_msg = "无法获取 JWT"
                    invalid_accounts.append(account.name)
                    logger.warning(f"⚠️ Cookie 无效 [{account.name}]: 无法获取 JWT")
                        
            except HTTPException as e:
                error_msg = str(e.detail) if hasattr(e, 'detail') else str(e)
                if e.status_code in (401, 403):
                    # Cookie 无效
                    if e.status_code == 401:
                        cookie_status = "expired"
                        error_msg = "Cookie 已过期"
                    elif e.status_code == 403:
                        cookie_status = "forbidden"
                        error_msg = "Cookie 无效或被禁止"
                    invalid_accounts.append(account.name)
                    logger.warning(f"⚠️ Cookie 无效 [{account.name}]: {e.status_code}")
                elif e.status_code == 429:
                    cookie_status = "rate_limited"
                    error_msg = "请求过于频繁"
                    logger.warning(f"⚠️ 请求过于频繁 [{account.name}]")
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"⚠️ 检查异常 [{account.name}]: {str(e)}")
            
            # 保存检查结果到数据库
            try:
                account_status = db.query(AccountCookieStatus).filter(
                    AccountCookieStatus.account_name == account.name
                ).first()
                
                if account_status:
                    # 更新现有记录
                    account_status.cookie_status = cookie_status
                    account_status.last_check_at = check_time
                    # 只有在 Cookie 有效时才更新过期时间，避免覆盖有效的过期时间
                    if cookie_status == "valid" and expires_at:
                        account_status.expires_at = expires_at
                    account_status.error_message = error_msg
                    account_status.updated_at = check_time
                else:
                    # 创建新记录
                    account_status = AccountCookieStatus(
                        account_name=account.name,
                        cookie_status=cookie_status,
                        last_check_at=check_time,
                        expires_at=expires_at if cookie_status == "valid" else None,
                        error_message=error_msg
                    )
                    db.add(account_status)
            except Exception as e:
                logger.error(f"保存账号 Cookie 状态失败 [{account.name}]: {e}")
        
        # 批量提交所有账号状态更新
        try:
            db.commit()
            logger.info(f"✅ 已保存 {total_accounts} 个账号的 Cookie 状态到数据库（最后检查时间: {check_time}）")
        except Exception as e:
            logger.error(f"批量保存 Cookie 状态失败: {e}")
            db.rollback()
        
        # 如果启用了自动修复，且检测到无效账号，则调用浏览器保活来修复
        if invalid_accounts and getattr(task, 'auto_check_auto_fix', True):
            logger.info(f"🔧 检测到 {len(invalid_accounts)} 个无效账号，开始自动修复: {', '.join(invalid_accounts)}")
            try:
                # 调用浏览器保活来修复这些账号
                await execute_keep_alive_task_for_accounts(invalid_accounts)
                logger.info(f"✅ 自动修复完成: {len(invalid_accounts)} 个账号")
            except Exception as e:
                logger.error(f"❌ 自动修复失败: {e}")
        
        # 更新任务状态
        task.last_auto_check_at = get_beijing_time()
        task.last_message = f"自动检查完成: 有效 {valid_count}/{total_accounts}, 无效 {len(invalid_accounts)}"
        db.commit()
        
        logger.info(f"✅ 自动检查任务完成: 有效 {valid_count}/{total_accounts}, 无效 {len(invalid_accounts)}")
        
    except Exception as e:
        logger.error(f"❌ 自动检查任务执行失败: {e}")
        if task:
            task.last_message = f"自动检查失败: {str(e)}"
            db.commit()
    finally:
        db.close()


async def execute_keep_alive_task():
    """执行保活任务"""
    global current_keep_alive_process
    
    db = next(get_db())
    log_entry = None
    try:
        # 获取保活任务配置
        task = db.query(KeepAliveTask).first()
        if not task:
            # 如果没有任务配置，创建一个默认的
            task = KeepAliveTask(
                is_enabled=True,
                schedule_time="00:00"
            )
            db.add(task)
            db.commit()
            db.refresh(task)
        
        if not task.is_enabled:
            logger.info("保活任务已禁用，跳过执行")
            return
        
        logger.info("🔄 开始执行保活任务...")
        
        # 在开始执行前，先重新加载一次账号配置（以防.env文件已被部分更新）
        try:
            reload_accounts_from_env_file()
            logger.info("🔄 保活任务开始前已重新加载账号配置")
        except Exception as e:
            logger.warning(f"⚠️ 保活任务开始前重新加载账号配置失败: {e}")
        
        # 创建执行日志
        log_entry = KeepAliveLog(
            task_id=task.id,
            started_at=get_beijing_time(),
            status="running"
        )
        db.add(log_entry)
        db.commit()
        db.refresh(log_entry)
        
        # 更新任务状态
        task.last_run_at = get_beijing_time()
        task.last_status = "running"
        task.last_message = "执行中..."
        db.commit()
        
        try:
            # 执行 keep_alive_env.py 脚本（从 .env 文件读取账号）
            script_path = os.path.join(BASE_DIR, "keep_alive_env.py")
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"保活脚本不存在: {script_path}")
            
            # 使用 subprocess.Popen 执行脚本，以便可以中断
            async with keep_alive_process_lock:
                current_keep_alive_process = subprocess.Popen(
                    [sys.executable, "-u", script_path],  # -u 参数确保无缓冲输出
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,  # 将 stderr 合并到 stdout，确保错误也能被读取
                    text=True,
                    encoding='utf-8',  # 明确指定 UTF-8 编码，确保 Unicode 字符正确显示
                    errors='replace',  # 遇到编码错误时替换而不是报错
                    cwd=BASE_DIR,
                    bufsize=0  # 无缓冲
                )
            
            # 实时读取输出并解析账号日志
            output_lines = []
            account_logs_dict = {}  # 存储账号级别的日志 {account_name: account_log}
            account_index_to_log = {}  # 存储账号索引到日志的映射 {account_index: account_log}
            
            # 在后台读取输出
            async def read_output():
                nonlocal account_logs_dict, account_index_to_log
                try:
                    while True:
                        # 检查进程是否还在运行
                        if current_keep_alive_process.poll() is not None:
                            # 进程已结束，读取剩余输出
                            remaining = await asyncio.to_thread(
                                lambda: current_keep_alive_process.stdout.read()
                            )
                            if remaining:
                                for line in remaining.strip().split('\n'):
                                    if line.strip():
                                        output_lines.append(line.strip())
                                        logger.info(f"保活输出: {line.strip()}")
                            break
                        
                        # 读取一行输出
                        line = await asyncio.to_thread(
                            lambda: current_keep_alive_process.stdout.readline()
                        )
                        if not line:
                            await asyncio.sleep(0.1)  # 短暂等待
                            continue
                        
                        line = line.strip()
                        if line:
                            output_lines.append(line)
                            logger.info(f"保活输出: {line}")
                            
                            # 解析账号级别的日志：格式如 "[1/3] 开始更新账号: user@example.com (user@example.com) - 2024-01-01 12:00:00"
                            # 或 "[1/3] 更新成功账号: user@example.com (user@example.com) - 2024-01-01 12:00:00 (耗时: 1分30秒)"
                            account_match = re.search(r'\[(\d+)/(\d+)\]\s*(开始更新|更新成功|更新失败).*?账号:\s*([^(]+)(?:\(([^)]+)\))?', line)
                            if account_match:
                                account_index = account_match.group(1)
                                total_accounts = account_match.group(2)
                                action = account_match.group(3)
                                account_name = account_match.group(4).strip()
                                account_email = account_match.group(5).strip() if account_match.group(5) else extract_email_from_name(account_name)
                                
                                # 如果账号日志已存在，更新状态；否则创建新日志
                                if account_name in account_logs_dict:
                                    account_log = account_logs_dict[account_name]
                                    if "成功" in action:
                                        account_log.status = "success"
                                    elif "失败" in action:
                                        account_log.status = "error"
                                    account_log.finished_at = get_beijing_time()
                                    # 累积所有日志行到 message 中
                                    if account_log.message:
                                        account_log.message = account_log.message + "\n" + line
                                    else:
                                        account_log.message = line
                                else:
                                    # 创建账号日志
                                    account_log = KeepAliveAccountLog(
                                        task_log_id=log_entry.id,
                                        account_name=account_name,
                                        account_email=account_email,
                                        started_at=get_beijing_time(),
                                        status="running" if "开始" in action else ("success" if "成功" in action else "error"),
                                        message=line
                                    )
                                    account_logs_dict[account_name] = account_log
                                    account_index_to_log[int(account_index)] = account_log  # 保存索引映射
                                    db.add(account_log)
                                
                                db.commit()
                            else:
                                # 如果不是账号级别的日志，尝试关联到对应的账号日志
                                # 查找包含账号索引的行，如 "[1/21] 需要验证码" 或 "[1/21] ✅ 找到验证码"
                                index_match = re.search(r'\[(\d+)/(\d+)\]', line)
                                if index_match:
                                    account_index = int(index_match.group(1))
                                    # 通过账号索引找到对应的账号日志
                                    if account_index in account_index_to_log:
                                        account_log = account_index_to_log[account_index]
                                        # 累积日志到 message 中
                                        if account_log.message:
                                            account_log.message = account_log.message + "\n" + line
                                        else:
                                            account_log.message = line
                                        db.commit()
                                    elif account_logs_dict:
                                        # 如果索引映射中没有，使用最后创建的账号日志（向后兼容）
                                        last_account_log = max(account_logs_dict.values(), key=lambda x: x.started_at)
                                        if last_account_log:
                                            if last_account_log.message:
                                                last_account_log.message = last_account_log.message + "\n" + line
                                            else:
                                                last_account_log.message = line
                                            db.commit()
                except Exception as e:
                    logger.error(f"读取保活输出异常: {e}")
            
            # 启动后台读取任务
            read_task = asyncio.create_task(read_output())
            
            # 等待进程完成或中断
            try:
                returncode = await asyncio.wait_for(
                    asyncio.to_thread(current_keep_alive_process.wait),
                    timeout=3600  # 1小时超时
                )
            except asyncio.TimeoutError:
                # 超时，终止进程
                current_keep_alive_process.terminate()
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(current_keep_alive_process.wait),
                        timeout=5
                    )
                except asyncio.TimeoutError:
                    current_keep_alive_process.kill()
                returncode = -1
                status = "error"
                message = "执行超时（超过1小时）"
            except Exception as e:
                returncode = -1
                status = "error"
                message = f"执行异常: {str(e)[:200]}"
            finally:
                # 等待读取任务完成
                try:
                    await asyncio.wait_for(read_task, timeout=2)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    read_task.cancel()
                    try:
                        await read_task
                    except:
                        pass
            
            # 读取剩余输出（由于 stderr 已合并到 stdout，这里只需要读取 stdout）
            stderr_content = ""
            try:
                # 由于 stderr 已合并到 stdout，这里不再单独读取 stderr
                # 所有输出（包括错误）都已经在 read_output 中读取了
                pass
            except Exception as e:
                logger.error(f"读取进程输出失败: {e}")
                stderr_content = str(e)
            
            output = '\n'.join(output_lines)
            
            # 提取汇总信息（包含 "总账号数"、"成功"、"失败"、"成功率"、"总耗时" 等）
            summary_lines = []
            for line in output_lines:
                if any(keyword in line for keyword in ["总账号数", "成功:", "失败:", "成功率", "总耗时", "更新统计", "=" * 10]):
                    summary_lines.append(line)
            summary_output = '\n'.join(summary_lines) if summary_lines else None
            
            # 解析统计信息
            success_match = re.search(r'成功:\s*(\d+)', output)
            fail_match = re.search(r'失败:\s*(\d+)', output)
            total_match = re.search(r'总账号数:\s*(\d+)', output)
            
            accounts_count = int(total_match.group(1)) if total_match else len(account_logs_dict)
            success_count = int(success_match.group(1)) if success_match else len([log for log in account_logs_dict.values() if log.status == "success"])
            fail_count = int(fail_match.group(1)) if fail_match else len([log for log in account_logs_dict.values() if log.status == "error"])
            
            if returncode == 0:
                status = "success"
                message = f"成功更新 {success_count}/{accounts_count} 个账号"
                # 如果有汇总信息，添加到 message 中
                if summary_output:
                    message = message + "\n\n" + summary_output
            else:
                status = "error"
                error_msg = stderr_content[:200] if stderr_content else output[-200:] if output else '未知错误'
                message = f"执行失败: {error_msg}"
                # 如果有汇总信息，也添加到 message 中
                if summary_output:
                    message = message + "\n\n" + summary_output
            
            # 更新所有账号日志的结束时间
            for acc_log in account_logs_dict.values():
                if acc_log.finished_at is None:
                    acc_log.finished_at = get_beijing_time()
                    if acc_log.status == "running":
                        acc_log.status = "error"
                        acc_log.message = (acc_log.message or "") + " (进程异常结束)"
                    db.commit()
            
            # 更新日志
            log_entry.finished_at = get_beijing_time()
            log_entry.status = status
            log_entry.message = message
            log_entry.accounts_count = accounts_count
            log_entry.success_count = success_count
            log_entry.fail_count = fail_count
            
            # 更新任务状态
            task.last_status = status
            task.last_message = message
            task.updated_at = get_beijing_time()
            
            db.commit()
            db.close()
            
            # 重新加载账号配置，确保使用最新的 csesidx 等配置
            try:
                reload_accounts_from_env_file()
                logger.info("🔄 保活任务完成后已重新加载账号配置")
            except Exception as e:
                logger.error(f"❌ 重新加载账号配置失败: {e}")
            
            logger.info(f"✅ 保活任务执行完成: {message}")
            
        except Exception as e:
            status = "error"
            message = f"执行异常: {str(e)[:200]}"
            if log_entry:
                log_entry.finished_at = get_beijing_time()
                log_entry.status = status
                log_entry.message = message
            task.last_status = status
            task.last_message = message
            db.commit()
            logger.error(f"❌ 保活任务执行异常: {e}")
        finally:
            async with keep_alive_process_lock:
                current_keep_alive_process = None
            
    except Exception as e:
        logger.error(f"❌ 保活任务执行失败: {e}")
    finally:
        db.close()


def setup_keep_alive_scheduler():
    """设置保活任务调度器"""
    db = next(get_db())
    try:
        # 获取或创建保活任务配置
        task = db.query(KeepAliveTask).first()
        if not task:
            task = KeepAliveTask(
                is_enabled=True,
                schedule_time="00:00",
                api_keepalive_enabled=True,
                api_keepalive_interval=30
            )
            db.add(task)
            db.commit()
            db.refresh(task)
        
        # 如果任务已启用，添加定时任务
        if task.is_enabled:
            # 解析时间（HH:MM格式）
            hour, minute = map(int, task.schedule_time.split(":"))
            
            # 添加每日定时任务（北京时间）
            scheduler.add_job(
                execute_keep_alive_task,
                trigger=CronTrigger(hour=hour, minute=minute, timezone=BEIJING_TZ),
                id="keep_alive_task",
                replace_existing=True
            )
            logger.info(f"✅ 保活任务已设置，每日 {task.schedule_time} (北京时间) 执行")
        else:
            logger.info("ℹ️ 保活任务已禁用")
        
        # 设置 API 保活调度器
        if getattr(task, 'api_keepalive_enabled', True):
            try:
                interval = getattr(task, 'api_keepalive_interval', 30)
                scheduler.add_job(
                    execute_api_keepalive_task,
                    trigger=CronTrigger(minute=f"*/{interval}", timezone=BEIJING_TZ),
                    id="api_keepalive_task",
                    replace_existing=True
                )
                logger.info(f"✅ API 保活任务已设置，每 {interval} 分钟执行一次")
            except Exception as e:
                logger.error(f"❌ 设置 API 保活调度器失败: {e}")
        else:
            logger.info("ℹ️ API 保活任务已禁用")
        
        # 设置自动检查调度器
        if getattr(task, 'auto_check_enabled', False):
            try:
                interval = getattr(task, 'auto_check_interval', 60)
                scheduler.add_job(
                    execute_auto_check_task,
                    trigger=CronTrigger(minute=f"*/{interval}", timezone=BEIJING_TZ),
                    id="auto_check_task",
                    replace_existing=True
                )
                logger.info(f"✅ 自动检查任务已设置，每 {interval} 分钟执行一次")
            except Exception as e:
                logger.error(f"❌ 设置自动检查调度器失败: {e}")
        else:
            logger.info("ℹ️ 自动检查任务已禁用")
            
    except Exception as e:
        logger.error(f"❌ 设置保活任务调度器失败: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    # 至少需要有一个账号配置
    if not ACCOUNTS:
        logger.error("No Gemini business accounts configured.")
        logger.error(
            "Set SECURE_C_SES/CSESIDX/CONFIG_ID or ACCOUNT1_* env variables first."
        )
        raise SystemExit(1)

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
