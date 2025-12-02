"""
Gemini Business 配置更新脚本
读取配置文件中的账号，重新获取并更新配置信息
"""

import time
import re
import logging
import signal
import atexit
import threading
import sys
import os
import io
from typing import List, Dict, Optional, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import httpx
from urllib.parse import quote

# 全局变量：存储所有打开的浏览器驱动，用于中断时关闭
_active_drivers: List[webdriver.Edge] = []
_drivers_lock = threading.Lock()  # 线程锁，保护 _active_drivers 列表

def cleanup_drivers():
    """清理所有打开的浏览器驱动"""
    global _active_drivers
    with _drivers_lock:
        drivers_to_close = _active_drivers[:]  # 复制列表，避免在迭代时修改
        _active_drivers.clear()
    
    if drivers_to_close:
        logger.info(f"🛑 正在关闭 {len(drivers_to_close)} 个浏览器窗口...")
        for driver in drivers_to_close:
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"关闭浏览器时出错: {e}")
        logger.info("✅ 所有浏览器窗口已关闭")

def signal_handler(signum, frame):
    """信号处理函数：中断时关闭所有浏览器"""
    logger.info("🛑 收到中断信号，正在关闭所有浏览器...")
    try:
        cleanup_drivers()
    except Exception as e:
        logger.error(f"清理浏览器时出错: {e}")
    import sys
    sys.exit(0)

# 注册信号处理（Windows 和 Unix 都支持）
try:
    # 注册 SIGINT（Ctrl+C）
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, signal_handler)
    # 注册 SIGTERM（如果可用）
    if hasattr(signal, 'SIGTERM'):
        try:
            signal.signal(signal.SIGTERM, signal_handler)
        except (ValueError, OSError):
            # Windows 上 SIGTERM 可能不可用，忽略错误
            pass
except Exception as e:
    logger.debug(f"注册信号处理失败: {e}")

# 注册退出时清理（确保即使正常退出也能清理）
atexit.register(cleanup_drivers)

# 配置日志（确保 Unicode 字符正确显示）
# 确保 stdout 使用 UTF-8 编码（Windows 上需要）
if sys.platform == 'win32':
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass
    if hasattr(sys.stderr, 'reconfigure'):
        try:
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass

# 创建使用 UTF-8 编码的 StreamHandler
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"))

# 配置日志
logger = logging.getLogger("update-configs")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False  # 防止重复输出

# 禁用 Selenium 和浏览器驱动的冗余日志
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("selenium.webdriver").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

import warnings
warnings.filterwarnings("ignore")

# 过滤浏览器驱动的错误输出
if sys.platform == 'win32':
    try:
        original_stderr = sys.stderr
        class FilteredStderr:
            def __init__(self):
                self.original = original_stderr
            
            def write(self, s):
                # 过滤掉常见的浏览器驱动错误（这些错误不影响功能）
                # 使用更全面的匹配规则
                if not s or not isinstance(s, str):
                    return
                
                # 转换为字符串并去除首尾空白
                s_str = str(s).strip()
                
                # 如果为空字符串，直接返回
                if not s_str:
                    return
                
                filtered_keywords = [
                    # 组件错误（使用多种匹配方式）
                    'ERROR:components',
                    'ERROR:chrome\\browser',
                    'ERROR:gpu',
                    'components\\device_event_log',
                    'components\\edge_auth',
                    'chrome\\browser\\importer',
                    'chrome\\browser\\task_manager',
                    'gpu\\command_buffer',
                    'components\\segmentation_platform',
                    'device_event_log_impl.cc',
                    'edge_auth_errors.cc',
                    'fallback_task_provider.cc',
                    # USB相关
                    'USB:',
                    'usb_service_win.cc',
                    'SetupDiGetDeviceProperty',
                    'failed: 鎵句笉鍒板厓绱',  # USB错误的中文部分
                    '0x490',  # USB错误代码
                    # Edge身份验证相关
                    'EDGE_IDENTITY',
                    'Get Default OS Account failed',
                    'kTokenRequestFailed',
                    'kTokenFetchUserInteractionRequired',
                    'edge_auth',
                    # 其他常见错误
                    'QQBrowser user data path not found',
                    'Processing error occured',
                    'CustomInputError',
                    'fill policy',
                    'Every renderer should have at least one task',
                    'crbug.com',
                ]
                
                # 检查是否包含任何过滤关键词（不区分大小写）
                s_lower = s_str.lower()
                if any(keyword.lower() in s_lower for keyword in filtered_keywords):
                    return  # 忽略这些错误
                
                # 额外过滤：匹配以 [ 开头的浏览器内部错误行
                # 格式: [PID:TID:时间:ERROR:路径]
                if s_str.startswith('[') and ':ERROR:' in s_str:
                    # 检查是否是浏览器组件错误
                    if any(comp in s_str for comp in [
                        'components',
                        'chrome\\browser',
                        'gpu',
                    ]):
                        return
                
                # 过滤包含 ERROR: 且是浏览器内部错误的行
                if ':ERROR:' in s_str:
                    if any(comp in s_str for comp in [
                        'components',
                        'chrome\\browser',
                        'gpu',
                        'edge_auth',
                        'device_event_log',
                        'task_manager',
                    ]):
                        return
                
                self.original.write(s)
            
            def flush(self):
                self.original.flush()
        
        sys.stderr = FilteredStderr()
        os.environ['EDGE_LOG_FILE'] = os.devnull
        os.environ['EDGE_CRASHDUMP'] = os.devnull
    except:
        pass

# ==================== 配置区域 ====================
CONFIG_FILE = "gemini_business_configs.txt"  # 配置文件路径
HEADLESS_MODE = False  # True=无头模式，False=有头模式
THREAD_COUNT = 3       # 线程数（建议不超过3）

# GPTMail API 配置
GPTMAIL_BASE_URL = "https://mail.chatgpt.org.uk"
GPTMAIL_API_KEY = "gpt-test"  # 测试 Key
# ==================================================


class GPTMailClient:
    """GPTMail 临时邮箱客户端 - 用于接收验证码"""
    
    def __init__(self, base_url: str = GPTMAIL_BASE_URL, driver: Optional[webdriver.Edge] = None, 
                 account_index: int = 0, total_accounts: int = 0):
        self.base_url = base_url
        self.client = httpx.Client(timeout=30.0, follow_redirects=True)
        self.driver = driver
        self.email_address: Optional[str] = None
        self.account_index = account_index
        self.total_accounts = total_accounts
        
        self.client.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': f'{base_url}/',
            'X-API-Key': GPTMAIL_API_KEY
        })
    
    def get_emails(self, email: str) -> list:
        """获取指定邮箱的邮件列表"""
        try:
            encoded_email = quote(email)
            url = f"{self.base_url}/api/emails?email={encoded_email}"
            
            response = self.client.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict):
                    if data.get("success") and "data" in data:
                        emails_data = data["data"]
                        if isinstance(emails_data, dict):
                            emails = emails_data.get("emails", [])
                        else:
                            emails = []
                    elif "emails" in data:
                        emails = data.get("emails", [])
                    else:
                        emails = []
                else:
                    emails = []
                return emails
            return []
        except Exception as e:
            prefix = f"[{self.account_index}/{self.total_accounts}]"
            logger.debug(f"{prefix} 获取邮件异常: {e}")
            return []
    
    def wait_for_verification_code(self, email: str, max_wait: int = 30, check_interval: int = 3) -> Optional[str]:
        """等待并提取验证码"""
        prefix = f"[{self.account_index}/{self.total_accounts}]"
        logger.info(f"{prefix} ⏳ 等待验证邮件... (最多等待 {max_wait} 秒)")
        start_time = time.time()
        last_log_time = 0
        
        while time.time() - start_time < max_wait:
            emails = self.get_emails(email)
            
            if emails:
                for email_item in emails:
                    from_addr = (email_item.get("from_address", "") or email_item.get("from", "")).lower()
                    subject = email_item.get("subject", "").lower()
                    
                    if "accountverification.business.gemini.google" in from_addr or "验证码" in subject:
                        content = (
                            email_item.get("html_content", "") or 
                            email_item.get("htmlContent", "") or 
                            email_item.get("content", "")
                        )
                        
                        code = self._extract_verification_code(content)
                        if code:
                            elapsed = int(time.time() - start_time)
                            logger.info(f"{prefix} ✅ 找到验证码: {code} (耗时: {elapsed} 秒)")
                            return code
            
            elapsed = int(time.time() - start_time)
            if elapsed - last_log_time >= 10:
                logger.info(f"{prefix} ⏳ 等待中... ({elapsed}/{max_wait} 秒)")
                last_log_time = elapsed
            time.sleep(check_interval)
        
        logger.error(f"{prefix} ❌ 等待邮件超时 ({max_wait} 秒)")
        return None
    
    def _extract_verification_code(self, content: str) -> Optional[str]:
        """从邮件内容中提取验证码"""
        if not content:
            return None
        
        code_patterns = [
            r'验证码[：:]\s*([A-Z0-9]{6})',
            r'一次性验证码[：:]\s*([A-Z0-9]{6})',
            r'验证码为[：:]\s*([A-Z0-9]{6})',
            r'为[：:]\s*([A-Z0-9]{6})',
            r'verification code[：:]\s*([A-Z0-9]{6})',
            r'code[：:]\s*([A-Z0-9]{6})',
            r'>([A-Z0-9]{6})<',
            r'\b([A-Z0-9]{6})\b',
            r'(\d{6})',
        ]
        
        for pattern in code_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                code = match.group(1).upper()
                if re.match(r'^[A-Z0-9]{6}$', code):
                    return code
        
        return None
    
    def close(self):
        """关闭客户端"""
        self.client.close()


def parse_config_file(file_path: str) -> List[Dict[str, str]]:
    """
    解析配置文件，提取所有账号信息
    
    Args:
        file_path: 配置文件路径
        
    Returns:
        账号列表，每个账号是一个字典
    """
    accounts = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 按分隔符分割账号
        account_blocks = re.split(r'# -{60}', content)
        
        for block in account_blocks:
            if not block.strip() or block.strip().startswith('#'):
                continue
            
            account = {}
            lines = block.strip().split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    account[key] = value
            
            if account.get('Name'):  # 至少要有邮箱
                accounts.append(account)
        
        logger.info(f"📋 从配置文件读取到 {len(accounts)} 个账号")
        return accounts
        
    except Exception as e:
        logger.error(f"❌ 读取配置文件失败: {e}")
        return []


def extract_config_from_browser(driver: webdriver.Edge, email: str, account_index: int = 0, total_accounts: int = 0) -> Optional[Dict[str, str]]:
    """
    从浏览器中提取配置信息
    
    Args:
        driver: 浏览器驱动
        email: 邮箱地址
        account_index: 账号索引
        total_accounts: 总账号数
        
    Returns:
        配置信息字典，如果失败返回 None
    """
    prefix = f"[{account_index}/{total_accounts}]"
    try:
        # 等待页面加载
        time.sleep(5)
        
        # 获取当前URL
        current_url = driver.current_url
        logger.debug(f"{prefix} 📄 当前页面: {current_url}")
        
        # 检查是否在正确的页面
        if "business.gemini.google" not in current_url:
            logger.warning(f"{prefix} ⚠️ 当前不在 Gemini Business 页面: {current_url}")
            return None
        
        # 提取 CONFIG_ID (从路径 /cid/ 后面)
        config_id = None
        path_parts = current_url.split('/')
        for i, part in enumerate(path_parts):
            if part == 'cid' and i + 1 < len(path_parts):
                config_id = path_parts[i + 1]
                break
        
        # 提取 CSESIDX (从 URL 参数)
        csesidx = None
        if '?' in current_url:
            url_params = current_url.split('?')[1]
            params = url_params.split('&')
            for param in params:
                if param.startswith('csesidx='):
                    csesidx = param.split('=')[1]
                    break
        
        # 提取 Cookie 信息（包括过期时间）
        cookies = driver.get_cookies()
        secure_c_ses = None
        host_c_oses = None
        cookie_expires_at = None
        
        for cookie in cookies:
            if cookie['name'] == '__Secure-C_SES':
                secure_c_ses = cookie['value']
                # 尝试获取 Cookie 过期时间（如果浏览器提供了）
                if 'expiry' in cookie and cookie['expiry']:
                    # Selenium 返回的 expiry 是 Unix 时间戳（秒）
                    from datetime import datetime, timezone, timedelta
                    expires_timestamp = cookie['expiry']
                    # 转换为 datetime 对象（北京时间，naive）
                    expires_dt = datetime.fromtimestamp(expires_timestamp, tz=timezone(timedelta(hours=8)))
                    cookie_expires_at = expires_dt.replace(tzinfo=None)
                    logger.debug(f"{prefix} 从浏览器 Cookie 获取过期时间: {cookie_expires_at}")
            elif cookie['name'] == '__Host-C_OSES' and cookie.get('domain', '').endswith('gemini.google'):
                host_c_oses = cookie['value']
                # 如果 HOST_C_OSES 有过期时间且更晚，使用它
                if 'expiry' in cookie and cookie['expiry']:
                    from datetime import datetime, timezone, timedelta
                    expires_timestamp = cookie['expiry']
                    expires_dt = datetime.fromtimestamp(expires_timestamp, tz=timezone(timedelta(hours=8)))
                    host_expires = expires_dt.replace(tzinfo=None)
                    if not cookie_expires_at or host_expires > cookie_expires_at:
                        cookie_expires_at = host_expires
                        logger.debug(f"{prefix} 从浏览器 Cookie (HOST_C_OSES) 获取过期时间: {cookie_expires_at}")
        
        # 如果信息不完整，等待并重试
        if not config_id or not csesidx or not secure_c_ses:
            logger.info(f"{prefix} ⏳ 等待页面完全加载...")
            time.sleep(10)
            current_url = driver.current_url
            
            # 重新提取
            path_parts = current_url.split('/')
            for i, part in enumerate(path_parts):
                if part == 'cid' and i + 1 < len(path_parts):
                    config_id = path_parts[i + 1]
                    break
            
            if '?' in current_url:
                url_params = current_url.split('?')[1]
                params = url_params.split('&')
                for param in params:
                    if param.startswith('csesidx='):
                        csesidx = param.split('=')[1]
                        break
            
            # 重新获取 Cookie
            cookies = driver.get_cookies()
            for cookie in cookies:
                if cookie['name'] == '__Secure-C_SES':
                    secure_c_ses = cookie['value']
                elif cookie['name'] == '__Host-C_OSES' and cookie.get('domain', '').endswith('gemini.google'):
                    host_c_oses = cookie['value']
        
        if config_id and csesidx and secure_c_ses:
            return {
                'Name': email,
                'SECURE_C_SES': secure_c_ses,
                'CSESIDX': csesidx,
                'CONFIG_ID': config_id,
                'HOST_C_OSES': host_c_oses or ''
            }
        else:
            logger.warning(f"{prefix} ⚠️ 配置信息不完整: CONFIG_ID={config_id}, CSESIDX={csesidx}, SECURE_C_SES={'已找到' if secure_c_ses else '未找到'}")
            return None
            
    except Exception as e:
        logger.error(f"{prefix} ❌ 提取配置信息失败: {e}")
        import traceback
        logger.debug(f"{prefix} {traceback.format_exc()}")
        return None


def login_and_update_config(account: Dict[str, str], account_index: int, total_accounts: int) -> Optional[Dict[str, str]]:
    """
    登录账号并更新配置信息
    
    Args:
        account: 账号信息字典
        account_index: 账号索引
        total_accounts: 总账号数
        
    Returns:
        更新后的配置信息，如果失败返回 None
    """
    email = account.get('Name', '')
    if not email:
        logger.error(f"❌ [{account_index}/{total_accounts}] 账号信息中缺少邮箱")
        return None
    
    logger.info(f"📝 [{account_index}/{total_accounts}] 开始更新账号: {email}")
    
    driver = None
    try:
        # 初始化浏览器（使用 Selenium 管理的 Edge，不使用本机 Edge）
        edge_options = Options()
        
        # 使用隐私模式
        edge_options.add_argument("--inprivate")
        edge_options.add_argument("--no-sandbox")
        edge_options.add_argument("--disable-dev-shm-usage")
        edge_options.add_argument("--disable-blink-features=AutomationControlled")
        edge_options.add_argument("--disable-logging")
        edge_options.add_argument("--log-level=3")
        edge_options.add_argument("--disable-gpu")
        edge_options.add_argument("--silent")
        edge_options.add_argument("--disable-component-update")
        edge_options.add_argument("--disable-default-apps")
        edge_options.add_argument("--disable-sync")
        edge_options.add_argument("--no-first-run")
        edge_options.add_argument("--no-default-browser-check")
        edge_options.add_argument("--disable-features=TranslateUI")
        edge_options.add_argument("--disable-ipc-flooding-protection")
        edge_options.add_argument("--disable-extensions")
        edge_options.add_argument("--disable-infobars")
        edge_options.add_argument("--disable-background-networking")
        edge_options.add_argument("--disable-logging")
        edge_options.add_argument("--log-level=3")  # 只显示致命错误
        
        # 禁用各种日志和错误报告
        prefs = {
            'logging': {
                'prefs': {
                    'browser.enable_spellchecking': False
                }
            }
        }
        edge_options.add_experimental_option('prefs', prefs)
        edge_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        edge_options.add_experimental_option('useAutomationExtension', False)
        
        if HEADLESS_MODE:
            edge_options.add_argument("--headless")
        
        # 启动浏览器（FilteredStderr 已经在模块级别设置，会自动过滤错误）
        service = Service()
        driver = webdriver.Edge(options=edge_options, service=service)
        
        # 将驱动添加到全局列表，用于中断时关闭
        with _drivers_lock:
            _active_drivers.append(driver)
        
        # 有头模式下最小化窗口
        if not HEADLESS_MODE:
            try:
                driver.minimize_window()
                logger.info(f"[{account_index}/{total_accounts}] 🔽 浏览器窗口已最小化")
            except Exception as e:
                logger.debug(f"[{account_index}/{total_accounts}] 最小化窗口失败: {e}")
        
        # 访问 Google Business 登录页面
        login_url = "https://auth.business.gemini.google/login?continueUrl=https://business.gemini.google/"
        logger.info(f"🔗 [{account_index}/{total_accounts}] 访问登录页面...")
        driver.get(login_url)
        time.sleep(3)
        
        # 输入邮箱
        wait = WebDriverWait(driver, 20)
        email_input = wait.until(EC.presence_of_element_located((By.ID, "email-input")))
        email_input.clear()
        email_input.send_keys(email)
        time.sleep(1)
        
        # 点击继续按钮
        continue_button = wait.until(EC.element_to_be_clickable((By.ID, "log-in-button")))
        driver.execute_script("arguments[0].click();", continue_button)
        logger.info(f"✅ [{account_index}/{total_accounts}] 已提交邮箱，等待跳转...")
        
        # 等待跳转到验证页面或主页面
        time.sleep(5)
        
        # 检查是否需要输入验证码（如果跳转到验证页面）
        current_url = driver.current_url
        if "verify" in current_url.lower() or "verification" in current_url.lower():
            logger.info(f"📧 [{account_index}/{total_accounts}] 需要验证码，开始自动获取...")
            
            # 创建 GPTMail 客户端并等待验证码
            gptmail = GPTMailClient(driver=driver, account_index=account_index, total_accounts=total_accounts)
            
            # 验证码重试机制（最多重试1次）
            max_retries = 1
            verification_success = False
            
            for retry_count in range(max_retries + 1):
                if retry_count > 0:
                    logger.info(f"🔄 [{account_index}/{total_accounts}] 第 {retry_count + 1} 次尝试验证码...")
                
                verification_code = gptmail.wait_for_verification_code(
                    email=email,
                    max_wait=30,
                    check_interval=3
                )
                
                if verification_code:
                    logger.info(f"🔐 [{account_index}/{total_accounts}] 提交验证码: {verification_code}")
                    
                    # 提交验证码
                    try:
                        # 查找验证码输入框
                        code_selectors = [
                            (By.CSS_SELECTOR, "input[name='pinInput']"),
                            (By.CSS_SELECTOR, "input[jsname='ovqh0b']"),
                        ]
                        
                        code_input = None
                        for by, value in code_selectors:
                            try:
                                code_input = wait.until(EC.presence_of_element_located((by, value)))
                                break
                            except:
                                continue
                        
                        if code_input:
                            # 输入完整验证码
                            code_input.clear()
                            code_input.send_keys(verification_code)
                            logger.info(f"✅ [{account_index}/{total_accounts}] 已输入验证码")
                        else:
                            # 尝试6个独立输入框
                            code_inputs = driver.find_elements(By.CSS_SELECTOR, "div.f7wZi[data-index='0-5'] span.hLMukf")
                            if len(code_inputs) == 6:
                                for i, char in enumerate(verification_code):
                                    try:
                                        code_inputs[i].click()
                                        time.sleep(0.1)
                                        code_inputs[i].send_keys(char)
                                        time.sleep(0.1)
                                    except:
                                        pass
                                logger.info(f"✅ [{account_index}/{total_accounts}] 已输入验证码到6个独立输入框")
                        
                        # 查找并点击提交按钮
                        submit_selectors = [
                            (By.CSS_SELECTOR, "button[jsname='XooR8e']"),
                            (By.XPATH, "//button[contains(@aria-label, '验证')]"),
                            (By.CSS_SELECTOR, "button[type='submit']"),
                        ]
                        
                        submit_button = None
                        for by, value in submit_selectors:
                            try:
                                submit_button = wait.until(EC.element_to_be_clickable((by, value)))
                                if submit_button.is_displayed() and submit_button.is_enabled():
                                    break
                            except:
                                continue
                        
                        if submit_button:
                            driver.execute_script("arguments[0].click();", submit_button)
                            logger.info(f"✅ [{account_index}/{total_accounts}] 已提交验证码")
                            time.sleep(5)  # 等待跳转
                            
                            # 检查是否仍在验证页面
                            current_url_after = driver.current_url
                            if "verify" in current_url_after.lower() or "verification" in current_url_after.lower():
                                logger.warning(f"⚠️ [{account_index}/{total_accounts}] 提交验证码后仍停留在验证页面，尝试重新发送验证码...")
                                
                                # 如果还有重试机会，点击重新发送按钮
                                if retry_count < max_retries:
                                    try:
                                        # 使用固定的重新发送验证码按钮选择器
                                        resend_button_xpath = "//span[contains(text(), '重新发送验证码')]"
                                        resend_button = wait.until(EC.element_to_be_clickable((By.XPATH, resend_button_xpath)))
                                        driver.execute_script("arguments[0].click();", resend_button)
                                        logger.info(f"✅ [{account_index}/{total_accounts}] 已点击重新发送验证码按钮")
                                        time.sleep(3)  # 等待新验证邮件
                                        continue  # 继续下一次重试
                                    except Exception as e:
                                        logger.error(f"❌ [{account_index}/{total_accounts}] 点击重新发送验证码按钮失败: {e}")
                                        break
                                else:
                                    # 已用完重试次数，判定为被限制
                                    logger.error(f"❌ [{account_index}/{total_accounts}] 验证码提交失败，判定为被限制，跳过该账号")
                                    gptmail.close()
                                    return None
                            else:
                                # 成功跳转，验证码验证成功
                                verification_success = True
                                break
                        else:
                            logger.warning(f"⚠️ [{account_index}/{total_accounts}] 未找到提交按钮")
                            if retry_count < max_retries:
                                continue
                            else:
                                break
                    
                    except Exception as e:
                        logger.error(f"❌ [{account_index}/{total_accounts}] 提交验证码失败: {e}")
                        if retry_count < max_retries:
                            continue
                        else:
                            break
                else:
                    logger.error(f"❌ [{account_index}/{total_accounts}] 未收到验证码")
                    if retry_count < max_retries:
                        # 尝试点击重新发送按钮
                        try:
                            resend_button_xpath = "//span[contains(text(), '重新发送验证码')]"
                            resend_button = wait.until(EC.element_to_be_clickable((By.XPATH, resend_button_xpath)))
                            driver.execute_script("arguments[0].click();", resend_button)
                            logger.info(f"✅ [{account_index}/{total_accounts}] 已点击重新发送验证码按钮")
                            time.sleep(3)  # 等待新验证邮件
                            continue
                        except Exception as e:
                            logger.debug(f"[{account_index}/{total_accounts}] 未找到重新发送按钮: {e}")
                    break
            
            gptmail.close()
            
            if not verification_success:
                logger.error(f"❌ [{account_index}/{total_accounts}] 验证码验证失败，跳过该账号")
                return None
        
        # 等待跳转到主页面
        time.sleep(5)
        
        # 提取配置信息
        config = extract_config_from_browser(driver, email, account_index, total_accounts)
        
        if config:
            logger.info(f"✅ [{account_index}/{total_accounts}] 配置信息提取成功")
            return config
        else:
            logger.error(f"❌ [{account_index}/{total_accounts}] 配置信息提取失败")
            return None
            
    except Exception as e:
        logger.error(f"❌ [{account_index}/{total_accounts}] 更新过程出错: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None
        
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.debug(f"[{account_index}/{total_accounts}] 关闭浏览器时出错: {e}")
            finally:
                # 从全局列表中移除
                with _drivers_lock:
                    if driver in _active_drivers:
                        _active_drivers.remove(driver)


def update_config_file(accounts: List[Dict[str, str]], file_path: str):
    """
    更新配置文件
    
    Args:
        accounts: 更新后的账号列表
        file_path: 配置文件路径
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("# Gemini Business 配置信息\n")
            f.write("# 格式: Name=邮箱, SECURE_C_SES=..., CSESIDX=..., CONFIG_ID=..., HOST_C_OSES=...\n")
            f.write("# " + "=" * 60 + "\n\n")
            
            for i, account in enumerate(accounts):
                if i > 0:
                    f.write("# " + "-" * 60 + "\n\n")
                
                f.write(f"Name={account.get('Name', '')}\n")
                f.write(f"SECURE_C_SES={account.get('SECURE_C_SES', '')}\n")
                f.write(f"CSESIDX={account.get('CSESIDX', '')}\n")
                f.write(f"CONFIG_ID={account.get('CONFIG_ID', '')}\n")
                f.write(f"HOST_C_OSES={account.get('HOST_C_OSES', '')}\n")
                f.write("\n")
        
        logger.info(f"✅ 配置文件已更新: {file_path}")
        
    except Exception as e:
        logger.error(f"❌ 更新配置文件失败: {e}")


def update_single_account(account: Dict[str, str], account_index: int, total_accounts: int) -> Optional[Dict[str, str]]:
    """
    更新单个账号的配置
    
    Args:
        account: 账号信息
        account_index: 账号索引
        total_accounts: 总账号数
        
    Returns:
        更新后的配置信息
    """
    start_time = time.time()
    
    new_config = login_and_update_config(account, account_index, total_accounts)
    
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    if new_config:
        logger.info(f"⏱️  [{account_index}/{total_accounts}] 耗时: {minutes}分{seconds}秒 ({int(elapsed_time)}秒)")
        return new_config
    else:
        logger.warning(f"⏱️  [{account_index}/{total_accounts}] 耗时: {minutes}分{seconds}秒 ({int(elapsed_time)}秒) - 失败")
        # 如果更新失败，返回原配置
        return account


def main():
    """主函数"""
    logger.info("🚀 开始更新 Gemini Business 配置信息...")
    
    # 读取配置文件
    accounts = parse_config_file(CONFIG_FILE)
    
    if not accounts:
        logger.error("❌ 未找到任何账号信息")
        return
    
    logger.info(f"📋 找到 {len(accounts)} 个账号，开始更新...")
    
    # 记录总开始时间
    total_start_time = time.time()
    
    # 使用线程池执行更新
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    updated_accounts = []
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = {
            executor.submit(update_single_account, account, i + 1, len(accounts)): i
            for i, account in enumerate(accounts)
        }
        
        # 等待所有任务完成
        results = [None] * len(accounts)
        for future in as_completed(futures):
            account_index = futures[future]
            try:
                result = future.result()
                results[account_index] = result
                # 检查是否真正更新成功：需要新配置存在且与原配置不同
                original_account = accounts[account_index]
                if result and result.get('CONFIG_ID'):
                    # 比较新旧配置的 CONFIG_ID，如果相同说明没有真正更新
                    original_config_id = original_account.get('CONFIG_ID', '')
                    new_config_id = result.get('CONFIG_ID', '')
                    if new_config_id and new_config_id != original_config_id:
                        success_count += 1
                        logger.info(f"✅ 账号 {account_index + 1} 更新成功")
                    else:
                        fail_count += 1
                        logger.error(f"❌ 账号 {account_index + 1} 更新失败（配置未变化或提取失败）")
                else:
                    fail_count += 1
                    logger.error(f"❌ 账号 {account_index + 1} 更新失败")
            except Exception as e:
                fail_count += 1
                logger.error(f"❌ 账号 {account_index + 1} 更新异常: {e}")
                # 保留原配置
                results[account_index] = accounts[account_index]
    
    # 过滤掉 None 值
    updated_accounts = [acc for acc in results if acc]
    
    # 更新配置文件
    if updated_accounts:
        update_config_file(updated_accounts, CONFIG_FILE)
    
    # 计算总耗时
    total_elapsed_time = time.time() - total_start_time
    total_minutes = int(total_elapsed_time // 60)
    total_seconds = int(total_elapsed_time % 60)
    
    # 输出统计信息
    logger.info("=" * 60)
    logger.info("📊 更新统计:")
    logger.info(f"   总账号数: {len(accounts)}")
    logger.info(f"   成功: {success_count}")
    logger.info(f"   失败: {fail_count}")
    if len(accounts) > 0:
        logger.info(f"   成功率: {success_count / len(accounts) * 100:.1f}%")
    logger.info(f"   总耗时: {total_minutes}分{total_seconds}秒 ({int(total_elapsed_time)}秒)")
    logger.info("=" * 60)
    logger.info(f"✅ 配置文件已更新: {CONFIG_FILE}")


if __name__ == "__main__":
    main()

