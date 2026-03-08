import contextlib
import logging
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Optional

import httpx
from selenium import webdriver
from selenium.webdriver.edge.service import Service


EDGE_BROWSER_ENV_KEYS = (
    "EDGE_BINARY_PATH",
    "MSEDGE_BINARY",
    "EDGE_BROWSER_PATH",
)

EDGE_DRIVER_ENV_KEYS = (
    "EDGE_DRIVER_PATH",
    "MSEDGEDRIVER",
    "WEBDRIVER_EDGE_DRIVER",
)


def _log(logger: Optional[logging.Logger], level: int, message: str) -> None:
    if logger:
        logger.log(level, message)


def _first_existing_file(*candidates: Optional[str]) -> Optional[str]:
    for candidate in candidates:
        if not candidate:
            continue
        resolved = os.path.expandvars(os.path.expanduser(candidate))
        if os.path.isfile(resolved):
            return resolved
    return None


def _clean_proxy(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return None
    cleaned = proxy.strip().strip('"').strip("'")
    return cleaned or None


def _apply_proxy_env(proxy: Optional[str], logger: Optional[logging.Logger], log_prefix: str) -> Optional[str]:
    proxy = _clean_proxy(proxy)
    if not proxy:
        _log(logger, logging.INFO, f"{log_prefix} 未配置驱动下载代理，自动下载将使用直连")
        return None

    for key in ("PROXY", "SE_PROXY", "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ[key] = proxy

    _log(logger, logging.INFO, f"{log_prefix} 驱动下载使用代理: {proxy}")
    return proxy


def _get_windows_file_version(file_path: Optional[str]) -> Optional[str]:
    if sys.platform != "win32" or not file_path or not os.path.isfile(file_path):
        return None

    escaped_path = file_path.replace("'", "''")
    command = [
        "powershell.exe",
        "-Command",
        f"(Get-Item '{escaped_path}').VersionInfo.ProductVersion",
    ]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=8, check=False)
        version = (completed.stdout or "").strip()
        return version or None
    except Exception:
        return None


def _major_triplet(version: Optional[str]) -> Optional[str]:
    if not version:
        return None
    parts = version.split(".")
    if len(parts) < 3:
        return version
    return ".".join(parts[:3])


def find_edge_binary() -> Optional[str]:
    env_candidate = _first_existing_file(*(os.environ.get(key) for key in EDGE_BROWSER_ENV_KEYS))
    if env_candidate:
        return env_candidate

    for command_name in ("msedge.exe", "msedge", "microsoft-edge"):
        command_path = shutil.which(command_name)
        if command_path:
            return command_path

    if sys.platform != "win32":
        return None

    local_app_data = os.environ.get("LOCALAPPDATA", "")
    program_files = os.environ.get("PROGRAMFILES", r"C:\Program Files")
    program_files_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    return _first_existing_file(
        os.path.join(program_files_x86, "Microsoft", "Edge", "Application", "msedge.exe"),
        os.path.join(program_files, "Microsoft", "Edge", "Application", "msedge.exe"),
        os.path.join(local_app_data, "Microsoft", "Edge", "Application", "msedge.exe"),
    )


def _find_cached_msedgedriver(browser_version: Optional[str]) -> Optional[str]:
    cache_root = Path.home() / ".cache" / "selenium" / "msedgedriver"
    if not cache_root.exists():
        return None

    candidates = []
    for driver_file in cache_root.rglob("msedgedriver.exe"):
        version_name = driver_file.parent.name
        candidates.append((version_name, str(driver_file)))

    if not candidates:
        return None

    expected_triplet = _major_triplet(browser_version)
    if expected_triplet:
        for version_name, driver_path in sorted(candidates, reverse=True):
            if version_name.startswith(expected_triplet + ".") or version_name == expected_triplet:
                return driver_path

    return sorted(candidates, reverse=True)[0][1]


def find_edge_driver_binary(browser_version: Optional[str] = None) -> Optional[str]:
    env_candidate = _first_existing_file(*(os.environ.get(key) for key in EDGE_DRIVER_ENV_KEYS))
    if env_candidate:
        return env_candidate

    for command_name in ("msedgedriver.exe", "msedgedriver"):
        command_path = shutil.which(command_name)
        if command_path:
            return command_path

    base_dir = Path(__file__).resolve().parent
    cwd = Path.cwd()
    local_candidate = _first_existing_file(
        str(base_dir / "drivers" / "msedgedriver.exe"),
        str(base_dir / "drivers" / "msedgedriver"),
        str(cwd / "drivers" / "msedgedriver.exe"),
        str(cwd / "drivers" / "msedgedriver"),
        str(cwd / "msedgedriver.exe"),
        str(cwd / "msedgedriver"),
    )
    if local_candidate:
        return local_candidate

    return _find_cached_msedgedriver(browser_version)


@contextlib.contextmanager
def _optional_stderr_redirect(enabled: bool):
    if enabled and sys.platform == "win32":
        original_stderr = sys.stderr
        with open(os.devnull, "w", encoding="utf-8", errors="ignore") as devnull:
            sys.stderr = devnull
            try:
                yield
            finally:
                sys.stderr = original_stderr
        return

    yield


def _new_edge_driver(options, service: Service, suppress_stderr: bool):
    with _optional_stderr_redirect(suppress_stderr):
        return webdriver.Edge(options=options, service=service)


def _should_try_download_fallback(error: Exception) -> bool:
    message = str(error).lower()
    return "unable to obtain driver for microsoftedge" in message or "msedgedriver" in message


def _build_manual_driver_hint(browser_version: Optional[str], project_root: Path) -> str:
    expected = _major_triplet(browser_version)
    version_hint = f"与 Edge {expected}.* 匹配" if expected else "与本机 Edge 版本匹配"
    target_path = project_root / "backend" / "drivers" / "msedgedriver.exe"
    return f"请将 {version_hint} 的 msedgedriver.exe 放到 {target_path}，或设置环境变量 EDGE_DRIVER_PATH。"


def _download_official_edge_driver(browser_version: Optional[str], proxy: Optional[str], logger: Optional[logging.Logger], log_prefix: str) -> Optional[str]:
    if not browser_version:
        return None

    cache_dir = Path.home() / ".cache" / "selenium" / "msedgedriver" / "win64" / browser_version
    driver_path = cache_dir / "msedgedriver.exe"
    if driver_path.exists():
        _log(logger, logging.INFO, f"{log_prefix} 使用已缓存的官方 EdgeDriver: {driver_path}")
        return str(driver_path)

    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / "edgedriver_win64.zip"
    download_url = f"https://msedgedriver.microsoft.com/{browser_version}/edgedriver_win64.zip"

    _log(logger, logging.INFO, f"{log_prefix} 尝试从微软官方直链下载 EdgeDriver: {download_url}")
    with httpx.Client(proxy=proxy, timeout=120, verify=False, follow_redirects=True) as client:
        with client.stream("GET", download_url) as response:
            response.raise_for_status()
            with open(zip_path, "wb") as zip_file:
                for chunk in response.iter_bytes():
                    if chunk:
                        zip_file.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        member_name = next((name for name in zip_file.namelist() if name.lower().endswith("msedgedriver.exe")), None)
        if not member_name:
            raise RuntimeError(f"下载的 EdgeDriver 压缩包中未找到 msedgedriver.exe: {zip_path}")
        with zip_file.open(member_name) as source, open(driver_path, "wb") as target:
            shutil.copyfileobj(source, target)

    _log(logger, logging.INFO, f"{log_prefix} 微软官方直链已准备 EdgeDriver: {driver_path}")
    return str(driver_path)


def create_edge_driver(options, logger: Optional[logging.Logger] = None, log_prefix: str = "", suppress_stderr: bool = False):
    edge_binary = find_edge_binary()
    browser_version = _get_windows_file_version(edge_binary) if edge_binary else None
    if edge_binary and not getattr(options, "binary_location", None):
        options.binary_location = edge_binary
        version_suffix = f" (版本 {browser_version})" if browser_version else ""
        _log(logger, logging.INFO, f"{log_prefix} 使用 Edge 浏览器: {edge_binary}{version_suffix}")

    proxy = _apply_proxy_env(
        os.environ.get("PROXY") or os.environ.get("SE_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY"),
        logger,
        log_prefix,
    )

    driver_path = find_edge_driver_binary(browser_version=browser_version)
    if driver_path:
        _log(logger, logging.INFO, f"{log_prefix} 使用本地 EdgeDriver: {driver_path}")
        service = Service(executable_path=driver_path)
    else:
        _log(logger, logging.INFO, f"{log_prefix} 未发现本地 EdgeDriver，尝试 Selenium Manager")
        service = Service()

    try:
        return _new_edge_driver(options=options, service=service, suppress_stderr=suppress_stderr)
    except Exception as primary_error:
        if driver_path or not _should_try_download_fallback(primary_error):
            raise

        try:
            downloaded_driver = _download_official_edge_driver(
                browser_version=browser_version,
                proxy=proxy,
                logger=logger,
                log_prefix=log_prefix,
            )
            if downloaded_driver:
                return _new_edge_driver(
                    options=options,
                    service=Service(executable_path=downloaded_driver),
                    suppress_stderr=suppress_stderr,
                )
        except Exception as official_download_error:
            _log(logger, logging.WARNING, f"{log_prefix} 微软官方直链下载 EdgeDriver 失败，尝试 webdriver-manager: {official_download_error}")

        try:
            from webdriver_manager.microsoft import EdgeChromiumDriverManager
        except Exception:
            raise primary_error

        _log(logger, logging.WARNING, f"{log_prefix} Selenium Manager 获取 EdgeDriver 失败，尝试 webdriver-manager")

        try:
            downloaded_driver = EdgeChromiumDriverManager(
                version=browser_version,
                url="https://msedgedriver.microsoft.com",
                latest_release_url=f"https://msedgedriver.microsoft.com/{browser_version}/RELEASES" if browser_version else "https://msedgedriver.microsoft.com",
            ).install()
        except Exception as fallback_error:
            manual_hint = _build_manual_driver_hint(browser_version, Path(__file__).resolve().parents[1])
            if proxy:
                message = f"自动下载 EdgeDriver 失败（已使用代理 {proxy}）。{manual_hint} 原始错误: {fallback_error}"
            else:
                message = f"自动下载 EdgeDriver 失败。若你在中国大陆或受限网络环境，请先配置 PROXY，再重试；否则请手动放置驱动。{manual_hint} 原始错误: {fallback_error}"
            raise RuntimeError(message) from fallback_error

        _log(logger, logging.INFO, f"{log_prefix} webdriver-manager 已准备 EdgeDriver: {downloaded_driver}")
        return _new_edge_driver(
            options=options,
            service=Service(executable_path=downloaded_driver),
            suppress_stderr=suppress_stderr,
        )
