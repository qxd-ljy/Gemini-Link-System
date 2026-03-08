"""
Google Business (Gemini Business) 自动登录脚本 - Selenium 版本
使用 GPTMail 临时邮箱接收验证码
使用 Selenium 模拟浏览器操作

登录流程（与注册类似，但不需要填写姓名）：
1. 输入邮箱
2. 接收验证码
3. 填入验证码后直接到主页
"""

import time
import re
import logging
import random
import string
import threading
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import httpx
from edge_driver_utils import create_edge_driver

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gemini-business-login")

# 禁用 Selenium 和浏览器驱动的冗余日志
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("selenium.webdriver").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("httpcore").setLevel(logging.ERROR)
logging.getLogger("httpcore.http11").setLevel(logging.ERROR)
logging.getLogger("httpcore.connection").setLevel(logging.ERROR)

# 禁用 Edge/Chrome 驱动的日志输出
import warnings
warnings.filterwarnings("ignore")

# 禁用 DevTools 相关日志
import os
os.environ['WDM_LOG_LEVEL'] = '0'
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
os.environ['EDGE_LOG_FILE'] = os.devnull
os.environ['EDGE_CRASHDUMP'] = os.devnull

# 过滤浏览器驱动的错误输出
import sys
if sys.platform == 'win32':
    try:
        original_stderr = sys.stderr
        class FilteredStderr:
            def __init__(self):
                self.original = original_stderr
            
            def write(self, s):
                # 过滤掉常见的浏览器驱动错误（这些错误不影响功能）
                filtered_keywords = [
                    'ERROR:components\\device_event_log',
                    'ERROR:components\\edge_auth',
                    'ERROR:chrome\\browser\\importer',
                    'ERROR:gpu\\command_buffer',
                    'ERROR:components\\segmentation_platform',
                    'ERROR:chrome\\browser\\task_manager',
                    'device_event_log_impl.cc',
                    'edge_auth_errors.cc',
                    'fallback_task_provider.cc',
                    'USB: usb_service_win.cc',
                    'usb_service_win.cc',
                    'SetupDiGetDeviceProperty',
                    'failed: 鎵句笉鍒板厓绱',
                    'EDGE_IDENTITY:',
                    'Get Default OS Account failed',
                    'kTokenRequestFailed',
                    'kTokenFetchUserInteractionRequired',
                    'edge_auth',
                    'QQBrowser user data path not found',
                    'Processing error occured',
                    'CustomInputError',
                    'fill policy',
                    'Every renderer should have at least one task',
                    'crbug.com',
                ]
                
                if any(keyword in s for keyword in filtered_keywords):
                    return
                
                if s.strip().startswith('[') and 'ERROR:' in s:
                    if any(comp in s for comp in [
                        'components\\',
                        'chrome\\browser\\',
                        'gpu\\',
                    ]):
                        return
                
                self.original.write(s)
            
            def flush(self):
                self.original.flush()
        
        sys.stderr = FilteredStderr()
    except:
        pass

# ==================== 配置区域 ====================
# 在这里修改配置，控制登录行为

# 登录配置
HEADLESS_MODE = False  # True=无头模式（不显示浏览器），False=有头模式（显示浏览器）
THREAD_COUNT = 3       # 线程数（同时登录的账号数，建议不超过5）

# GPTMail API 基础 URL
GPTMAIL_BASE_URL = "https://mail.chatgpt.org.uk"
# GPTMail API Key（测试 Key，正式环境需要申请正式 Key）
GPTMAIL_API_KEY = "gpt-test"  # 测试 Key，每日调用限制视情况调整
# Google Business 登录 URL
GOOGLE_BUSINESS_LOGIN_URL = "https://auth.business.gemini.google/login?continueUrl=https://business.gemini.google/"

# 账号邮箱列表（需要登录的邮箱）
# 可以在这里直接配置邮箱列表，或者从文件读取
ACCOUNT_EMAILS: List[str] = [
    # 在这里添加需要登录的邮箱
    # "example1@gptmail.org",
    # "example2@gptmail.org",
]

# 或者从文件读取邮箱列表
ACCOUNT_EMAILS_FILE = "login_emails.txt"  # 每行一个邮箱
# ==================================================


class GPTMailClient:
    """GPTMail 临时邮箱客户端 - 用于接收验证码"""
    
    def __init__(self, email_address: str, base_url: str = GPTMAIL_BASE_URL, driver: Optional[webdriver.Edge] = None, account_index: int = 0, total_accounts: int = 1):
        self.base_url = base_url.rstrip('/')
        
        # 尝试使用 curl_cffi 绕过 SSL EOF 错误
        try:
            from curl_cffi import requests
            self.session = requests.Session()
            self.use_curl = True
            logger.info(f"[{account_index}/{total_accounts}] ✅ GPTMail 已加载 curl_cffi 指纹混淆")
        except ImportError:
            import requests
            self.session = requests.Session()
            self.use_curl = False
            logger.warning(f"[{account_index}/{total_accounts}] ⚠️ 未找到 curl_cffi，回退到 requests (可能触发 SSL 错误)")
            
        self.driver = driver
        self.email_address: str = email_address  # 登录时使用指定的邮箱
        self.account_index = account_index
        self.total_accounts = total_accounts
        self.api_key = GPTMAIL_API_KEY
        
        # 设置基础请求头
        self.session.headers.update({
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def _log_prefix(self) -> str:
        return f"[{self.account_index}/{self.total_accounts}]"
    
    def get_emails(self) -> list:
        """获取邮件列表"""
        if not self.email_address:
            return []
        
        try:
            url = f"{self.base_url}/api/emails"
            params = {'email': self.email_address}
            
            if self.use_curl:
                response = self.session.get(url, params=params, timeout=30, impersonate="chrome110")
            else:
                response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    emails = data.get("data", {}).get("emails", [])
                    if emails:
                        logger.info(f"{self._log_prefix()} 📧 收到 {len(emails)} 封邮件")
                    return emails
            return []
        except Exception as e:
            logger.debug(f"{self._log_prefix()} 获取邮件异常: {e}")
            return []
    
    def wait_for_email(self, sender_filter: Optional[str] = None, subject_filter: Optional[str] = None, max_wait: int = 120, check_interval: int = 5) -> Optional[str]:
        """循环等待验证码"""
        logger.info(f"{self._log_prefix()} ⏳ 等待验证邮件... (最多等待 {max_wait} 秒)")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            emails = self.get_emails()
            if emails:
                # 遍历每封邮件寻找验证码
                for email_item in emails:
                    # 获取多维度内容进行搜索
                    content = f"{email_item.get('subject', '')} {email_item.get('content', '')} {email_item.get('html_content', '')}"
                    
                    # 提取 6 位混合验证码
                    code = self._extract_verification_code(content)
                    if code:
                        logger.info(f"{self._log_prefix()} ✅ 成功提取验证码: {code}")
                        return code
            
            time.sleep(check_interval)
            
        logger.error(f"{self._log_prefix()} ❌ 等待邮件超时")
        return None
    
    def _extract_verification_code(self, content: str) -> Optional[str]:
        """提取 6 位大写字母数字混合验证码"""
        if not content: return None
        patterns = [
            r'验证码[：:]\s*([A-Z0-9]{6})',
            r'一次性验证码[：:]\s*([A-Z0-9]{6})',
            r'验证码为[：:]\s*([A-Z0-9]{6})',
            r'为[：:]\s*([A-Z0-9]{6})',
            r'verification code[：:\s]*([A-Z0-9]{6})',
            r'code[：:\s]*([A-Z0-9]{6})',
            r'>([A-Z0-9]{6})<',  # HTML 标签中的验证码
            r'\b([A-Z0-9]{6})\b',
            r'(\d{6})',
        ]
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                code = match.group(1).upper()
                if re.match(r'^[A-Z0-9]{6}$', code):
                    # 排除常见干扰词
                    if code not in ['GOOGLE', 'GEMINI', 'UPDATE']:
                        return code
        return None
    
    def close(self):
        """关闭会话"""
        if hasattr(self, 'session'):
            self.session.close()


class GoogleBusinessLoginSelenium:
    """Google Business 登录客户端（Selenium 版本）"""
    
    def __init__(self, email: str = "", headless: bool = False, proxy: Optional[str] = None, account_index: int = 0, total_accounts: int = 1):
        self.email = email
        self.headless = headless
        self.proxy = proxy
        self.driver: Optional[webdriver.Edge] = None
        self.account_index = account_index
        self.total_accounts = total_accounts
    
    def _log_prefix(self) -> str:
        """返回日志前缀"""
        return f"[{self.account_index}/{self.total_accounts}]"
    
    def init_driver(self):
        """初始化浏览器驱动（使用本机 Edge 浏览器的隐私模式）"""
        edge_options = Options()
        
        import os
        import platform
        
        if platform.system() == "Windows":
            # 常见的 Edge 浏览器安装路径
            edge_paths = [
                r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
                os.path.expanduser(r"~\AppData\Local\Microsoft\Edge\Application\msedge.exe"),
            ]
            
            # 查找可用的 Edge 浏览器路径
            edge_binary = None
            for path in edge_paths:
                if os.path.exists(path):
                    edge_binary = path
                    break
            
            if edge_binary:
                edge_options.binary_location = edge_binary
                logger.info(f"{self._log_prefix()} ✅ 使用本机 Edge 浏览器: {edge_binary}")
            else:
                logger.warning(f"{self._log_prefix()} ⚠️ 未找到本机 Edge 浏览器，将使用系统默认路径")
        
        # 为每个线程创建独立的临时数据目录，实现真正的进程隔离
        import tempfile
        try:
            self.temp_user_data_dir = tempfile.mkdtemp(prefix=f'gemini_edge_login_{self.account_index}_')
            edge_options.add_argument(f"--user-data-dir={self.temp_user_data_dir}")
            logger.info(f"{self._log_prefix()} 📁 已分配独立数据目录: {self.temp_user_data_dir}")
        except Exception as e:
            logger.warning(f"{self._log_prefix()} ⚠️ 创建临时目录失败: {e}")

        # 启用无痕模式（InPrivate）- 这是 Edge 的隐私模式
        edge_options.add_argument("--inprivate")
        logger.info(f"{self._log_prefix()} 🔒 已启用 Edge 隐私模式（InPrivate）")
        
        if self.headless:
            edge_options.add_argument("--headless")
        
        # 减少控制台输出
        edge_options.add_argument("--no-sandbox")
        edge_options.add_argument("--disable-dev-shm-usage")
        edge_options.add_argument("--disable-blink-features=AutomationControlled")
        edge_options.add_argument("--disable-logging")
        edge_options.add_argument("--log-level=3")
        edge_options.add_argument("--disable-gpu")
        edge_options.add_argument("--disable-extensions")
        edge_options.add_argument("--disable-infobars")
        edge_options.add_argument("--silent")
        edge_options.add_argument("--disable-background-networking")
        edge_options.add_argument("--disable-component-update")
        edge_options.add_argument("--disable-default-apps")
        edge_options.add_argument("--disable-sync")
        edge_options.add_argument("--no-first-run")
        edge_options.add_argument("--no-default-browser-check")
        edge_options.add_argument("--disable-features=TranslateUI")
        edge_options.add_argument("--disable-ipc-flooding-protection")
        
        # 禁用 DevTools 日志和错误输出
        edge_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        edge_options.add_experimental_option('useAutomationExtension', False)
        
        prefs = {
            'logging': {
                'prefs': {
                    'browser.enable_spellchecking': False
                }
            }
        }
        edge_options.add_experimental_option('prefs', prefs)
        
        import os
        os.environ['EDGE_LOG_FILE'] = os.devnull
        os.environ['EDGE_CRASHDUMP'] = os.devnull
        
        # 设置用户代理（Edge）
        edge_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0")
        
        # 设置代理（如果需要）
        if self.proxy:
            edge_options.add_argument(f"--proxy-server={self.proxy}")
        
        try:
            self.driver = create_edge_driver(
                options=edge_options,
                logger=logger,
                log_prefix=self._log_prefix(),
                suppress_stderr=True,
            )
            
            # 执行反检测脚本
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                '''
            })
            
            # 如果有头模式，最小化窗口
            if not self.headless:
                try:
                    self.driver.minimize_window()
                    logger.info(f"{self._log_prefix()} 📦 浏览器窗口已最小化")
                except Exception as e:
                    logger.warning(f"{self._log_prefix()} ⚠️ 无法最小化窗口: {e}")
            
            logger.info(f"{self._log_prefix()} ✅ Edge 浏览器驱动初始化成功（无痕模式）")
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 浏览器驱动初始化失败: {e}")
            logger.error(f"{self._log_prefix()} 💡 请确保已安装 Edge 浏览器和 EdgeDriver")
            raise
    
    def start_login(self) -> bool:
        """
        开始登录流程
        
        Returns:
            是否成功
        """
        try:
            if not self.driver:
                self.init_driver()
            
            logger.info(f"{self._log_prefix()} 🔗 访问 Google Business 登录页面...")
            self.driver.get(GOOGLE_BUSINESS_LOGIN_URL)
            
            # 等待页面加载完成
            wait = WebDriverWait(self.driver, 20)
            try:
                wait.until(EC.presence_of_element_located((By.ID, "email-input")))
                logger.info(f"{self._log_prefix()} ✅ 页面加载完成")
            except TimeoutException:
                logger.warning(f"{self._log_prefix()} ⚠️ 未检测到邮箱输入框，等待页面加载...")
                time.sleep(5)
            
            logger.info(f"{self._log_prefix()} ✅ 成功访问登录页面")
            return True
            
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 开始登录失败: {e}")
            return False
    
    def submit_email(self) -> bool:
        """
        提交邮箱地址
        
        Returns:
            是否成功
        """
        try:
            logger.info(f"{self._log_prefix()} 📧 提交邮箱: {self.email}")
            
            wait = WebDriverWait(self.driver, 20)
            
            # 尝试多种可能的选择器
            email_selectors = [
                (By.ID, "email-input"),
                (By.NAME, "loginHint"),
                (By.CSS_SELECTOR, "input#email-input"),
                (By.CSS_SELECTOR, "input[name='loginHint']"),
                (By.ID, "identifierId"),
                (By.NAME, "identifier"),
                (By.CSS_SELECTOR, "input[type='text'][autofocus]"),
                (By.XPATH, "//input[@id='email-input']"),
            ]
            
            email_input = None
            for by, value in email_selectors:
                try:
                    email_input = wait.until(EC.presence_of_element_located((by, value)))
                    logger.info(f"{self._log_prefix()} ✅ 找到邮箱输入框: {by}={value}")
                    break
                except TimeoutException:
                    continue
            
            if not email_input:
                logger.error(f"{self._log_prefix()} ❌ 未找到邮箱输入框")
                self.driver.save_screenshot("error_email_input.png")
                return False
            
            # 输入邮箱
            email_input.clear()
            email_input.send_keys(self.email)
            time.sleep(1)
            
            # 检查 reCAPTCHA 状态
            logger.info(f"{self._log_prefix()} ⏳ 检查 reCAPTCHA 验证状态...")
            try:
                recaptcha_response = self.driver.execute_script(
                    "return document.getElementById('g-recaptcha-response')?.value || '';"
                )
                if recaptcha_response:
                    logger.info(f"{self._log_prefix()} ✅ reCAPTCHA 已自动完成")
                else:
                    logger.info(f"{self._log_prefix()} ⏳ reCAPTCHA 可能需要手动验证")
                    time.sleep(3)
            except Exception as e:
                logger.warning(f"{self._log_prefix()} ⚠️ 检查 reCAPTCHA 时出错: {e}")
                time.sleep(2)
            
            # 查找并点击"继续"按钮
            continue_selectors = [
                (By.ID, "log-in-button"),
                (By.CSS_SELECTOR, "button#log-in-button[type='submit']"),
                (By.CSS_SELECTOR, "button[jsname='jXw9Fb']"),
                (By.XPATH, "//button[@id='log-in-button']"),
                (By.XPATH, "//button[@jsname='jXw9Fb']"),
                (By.XPATH, "//button[contains(@aria-label, '使用邮箱继续')]"),
                (By.XPATH, "//button[contains(@aria-label, 'Continue')]"),
                (By.ID, "identifierNext"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//button[contains(text(), '继续')]"),
            ]
            
            continue_button = None
            for by, value in continue_selectors:
                try:
                    continue_button = self.driver.find_element(by, value)
                    if continue_button.is_displayed() and continue_button.is_enabled():
                        logger.info(f"{self._log_prefix()} ✅ 找到继续按钮: {by}={value}")
                        break
                except NoSuchElementException:
                    continue
            
            if not continue_button:
                logger.error(f"{self._log_prefix()} ❌ 未找到继续按钮")
                self.driver.save_screenshot("error_continue_button.png")
                return False
            
            if not continue_button.is_enabled():
                logger.warning(f"{self._log_prefix()} ⚠️ 按钮不可用，可能 reCAPTCHA 未完成")
                logger.info(f"{self._log_prefix()} 💡 请手动完成 reCAPTCHA 验证")
                input("按 Enter 键继续（如果已手动完成验证）...")
            
            # 点击继续按钮
            try:
                self.driver.execute_script("arguments[0].click();", continue_button)
                logger.info(f"{self._log_prefix()} ✅ 使用 JavaScript 点击按钮")
            except Exception as e:
                logger.warning(f"{self._log_prefix()} ⚠️ JavaScript 点击失败，尝试普通点击: {e}")
                continue_button.click()
                logger.info(f"{self._log_prefix()} ✅ 使用普通点击")
            
            logger.info(f"{self._log_prefix()} ⏳ 等待页面响应...")
            time.sleep(5)
            
            current_url = self.driver.current_url
            if "verify" in current_url.lower() or "code" in current_url.lower():
                logger.info(f"{self._log_prefix()} ✅ 已跳转到验证页面")
            elif current_url != GOOGLE_BUSINESS_LOGIN_URL:
                logger.info(f"{self._log_prefix()} ✅ 页面已跳转: {current_url}")
            else:
                logger.warning(f"{self._log_prefix()} ⚠️ 页面可能未跳转，请检查是否需要手动处理")
            
            logger.info(f"{self._log_prefix()} ✅ 邮箱提交流程完成")
            return True
            
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 提交邮箱失败: {e}")
            self.driver.save_screenshot("error_submit_email.png")
            return False
    
    def submit_verification_code(self, code: str) -> bool:
        """
        提交验证码
        
        Args:
            code: 验证码（6位数字）
            
        Returns:
            是否成功
        """
        try:
            logger.info(f"{self._log_prefix()} 🔐 提交验证码: {code}")
            
            if len(code) != 6:
                logger.error(f"{self._log_prefix()} ❌ 验证码长度不正确，应为6位，当前为{len(code)}位")
                return False
            
            if not re.match(r'^[A-Z0-9]{6}$', code.upper()):
                logger.warning(f"{self._log_prefix()} ⚠️ 验证码格式可能不正确: {code}，但继续尝试提交")
            
            wait = WebDriverWait(self.driver, 20)
            
            # 方法1: 尝试使用隐藏的输入框
            try:
                pin_input = wait.until(EC.presence_of_element_located((By.NAME, "pinInput")))
                logger.info(f"{self._log_prefix()} ✅ 找到隐藏的验证码输入框 (pinInput)")
                
                pin_input.clear()
                pin_input.send_keys(code)
                time.sleep(0.5)
                logger.info(f"{self._log_prefix()} ✅ 已输入验证码到隐藏输入框: {code}")
            except TimeoutException:
                # 方法2: 尝试使用 jsname 选择器
                try:
                    pin_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[jsname='ovqh0b']")))
                    logger.info(f"{self._log_prefix()} ✅ 找到验证码输入框 (jsname='ovqh0b')")
                    pin_input.clear()
                    pin_input.send_keys(code)
                    time.sleep(0.5)
                except TimeoutException:
                    # 方法3: 尝试逐个输入到6个独立输入框
                    logger.info(f"{self._log_prefix()} ⚠️ 未找到隐藏输入框，尝试使用6个独立输入框")
                    try:
                        code_inputs = []
                        for i in range(6):
                            try:
                                input_elem = wait.until(
                                    EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, f"div.f7wZi[data-index='{i}'] span.hLMukf")
                                    )
                                )
                                code_inputs.append(input_elem)
                            except TimeoutException:
                                input_elem = self.driver.find_element(
                                    By.CSS_SELECTOR, 
                                    f"div.f7wZi[jsname='neThFe']:nth-child({i+1}) span.hLMukf"
                                )
                                code_inputs.append(input_elem)
                        
                        if len(code_inputs) == 6:
                            for i, char in enumerate(code):
                                try:
                                    code_inputs[i].click()
                                    time.sleep(0.1)
                                    code_inputs[i].send_keys(char)
                                    time.sleep(0.1)
                                except Exception as e:
                                    logger.warning(f"{self._log_prefix()} ⚠️ 输入第{i+1}位字符失败: {e}")
                                    self.driver.execute_script(
                                        f"arguments[0].textContent = '{char}';",
                                        code_inputs[i]
                                    )
                            
                            logger.info(f"{self._log_prefix()} ✅ 已输入验证码到6个独立输入框: {code}")
                        else:
                            raise Exception(f"未找到足够的输入框，只找到{len(code_inputs)}个")
                    except Exception as e:
                        logger.error(f"{self._log_prefix()} ❌ 无法找到验证码输入框: {e}")
                        self.driver.save_screenshot("error_code_input.png")
                        return False
            
            time.sleep(1)
            
            # 查找并点击提交按钮
            submit_selectors = [
                (By.CSS_SELECTOR, "button[jsname='XooR8e']"),
                (By.XPATH, "//button[@aria-label='验证']"),
                (By.XPATH, "//button[contains(@aria-label, '验证')]"),
                (By.CSS_SELECTOR, "button[type='submit'][aria-label='验证']"),
                (By.ID, "verifyNext"),
                (By.ID, "next"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//button[contains(@aria-label, 'Verify')]"),
                (By.XPATH, "//button[contains(text(), '验证')]"),
            ]
            
            submit_button = None
            for by, value in submit_selectors:
                try:
                    submit_button = wait.until(EC.element_to_be_clickable((by, value)))
                    if submit_button.is_displayed() and submit_button.is_enabled():
                        logger.info(f"{self._log_prefix()} ✅ 找到提交按钮: {by}={value}")
                        break
                except (TimeoutException, NoSuchElementException):
                    continue
            
            if not submit_button:
                logger.error(f"{self._log_prefix()} ❌ 未找到提交按钮")
                self.driver.save_screenshot("error_submit_button.png")
                return False
            
            # 点击提交按钮
            try:
                self.driver.execute_script("arguments[0].click();", submit_button)
                logger.info(f"{self._log_prefix()} ✅ 使用 JavaScript 点击提交按钮")
            except Exception as e:
                logger.warning(f"{self._log_prefix()} ⚠️ JavaScript 点击失败，尝试普通点击: {e}")
                submit_button.click()
                logger.info(f"{self._log_prefix()} ✅ 使用普通点击提交按钮")
            
            logger.info(f"{self._log_prefix()} ⏳ 等待验证结果...")
            time.sleep(5)
            
            # 检查是否成功（登录后直接到主页，不需要填姓名）
            current_url = self.driver.current_url
            if "business.gemini.google" in current_url and "verify" not in current_url.lower() and "verification" not in current_url.lower():
                logger.info(f"{self._log_prefix()} ✅ 登录成功，已跳转到业务页面")
                return True
            elif "error" in current_url.lower() or "fail" in current_url.lower():
                logger.warning(f"{self._log_prefix()} ⚠️ 可能登录失败，请检查页面")
                self.driver.save_screenshot("login_result.png")
                return False
            elif "verify" in current_url.lower() or "verification" in current_url.lower():
                logger.warning(f"{self._log_prefix()} ⚠️ 验证后仍停留在验证页面，可能验证码无效或被限制")
                return "STUCK"
            else:
                logger.info(f"{self._log_prefix()} 📄 当前页面: {current_url}")
                time.sleep(3)
                current_url = self.driver.current_url
                if "verify" in current_url.lower() or "verification" in current_url.lower():
                    return "STUCK"
                return True
            
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 提交验证码失败: {e}")
            self.driver.save_screenshot("error_submit_code.png")
            return False
    
    def resend_verification_code(self) -> bool:
        """
        点击"重新发送验证码"按钮
        
        Returns:
            是否成功点击
        """
        try:
            logger.info(f"{self._log_prefix()} 🔄 尝试重新发送验证码...")
            wait = WebDriverWait(self.driver, 10)
            
            resend_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), '重新发送验证码')]")))
            
            if not resend_button.is_displayed() or not resend_button.is_enabled():
                logger.warning(f"{self._log_prefix()} ⚠️ 重新发送验证码按钮不可用")
                return False
            
            try:
                self.driver.execute_script("arguments[0].click();", resend_button)
                logger.info(f"{self._log_prefix()} ✅ 已点击重新发送验证码按钮")
                time.sleep(2)
                return True
            except Exception as e:
                logger.warning(f"{self._log_prefix()} ⚠️ JavaScript 点击失败，尝试普通点击: {e}")
                resend_button.click()
                logger.info(f"{self._log_prefix()} ✅ 已点击重新发送验证码按钮")
                time.sleep(2)
                return True
                
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"{self._log_prefix()} ⚠️ 未找到重新发送验证码按钮")
            return False
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 重新发送验证码失败: {e}")
            return False
    
    def extract_config_info(self, email: str = "", output_file: str = "gemini_business_configs.txt") -> bool:
        """
        提取 Gemini Business 配置信息并追加到文件
        
        提取：
        - Name (邮箱地址)
        - SECURE_C_SES (从 Cookie)
        - CSESIDX (从 URL 参数)
        - CONFIG_ID (从 URL 路径)
        - HOST_C_OSES (从 Cookie)
        
        Args:
            email: 登录邮箱地址
            output_file: 输出文件路径
            
        Returns:
            是否成功
        """
        try:
            logger.info(f"{self._log_prefix()} 📋 开始提取 Gemini Business 配置信息...")
            
            time.sleep(5)
            
            current_url = self.driver.current_url
            logger.info(f"{self._log_prefix()} 📄 当前页面: {current_url}")
            
            if "business.gemini.google" not in current_url:
                logger.warning(f"{self._log_prefix()} ⚠️ 当前不在 Gemini Business 页面")
                return False
            
            # 提取 CONFIG_ID
            config_id = None
            path_parts = current_url.split('/')
            for i, part in enumerate(path_parts):
                if part == 'cid' and i + 1 < len(path_parts):
                    config_id = path_parts[i + 1]
                    break
            
            # 提取 CSESIDX
            csesidx = None
            if '?' in current_url:
                url_params = current_url.split('?')[1]
                params = url_params.split('&')
                for param in params:
                    if param.startswith('csesidx='):
                        csesidx = param.split('=')[1]
                        break
            
            # 提取 Cookie 信息
            cookies = self.driver.get_cookies()
            secure_c_ses = None
            host_c_oses = None
            
            for cookie in cookies:
                if cookie['name'] == '__Secure-C_SES':
                    secure_c_ses = cookie['value']
                elif cookie['name'] == '__Host-C_OSES' and cookie.get('domain', '').endswith('gemini.google'):
                    host_c_oses = cookie['value']
            
            # 验证必要数据
            if not config_id or not csesidx or not secure_c_ses:
                logger.warning(f"{self._log_prefix()} ⚠️ 配置信息不完整")
                logger.info(f"{self._log_prefix()}    CONFIG_ID: {config_id}")
                logger.info(f"{self._log_prefix()}    CSESIDX: {csesidx}")
                logger.info(f"{self._log_prefix()}    SECURE_C_SES: {'已找到' if secure_c_ses else '未找到'}")
                logger.info(f"{self._log_prefix()}    HOST_C_OSES: {'已找到' if host_c_oses else '未找到'}")
                
                if not config_id or not csesidx:
                    logger.info(f"{self._log_prefix()} ⏳ 等待页面完全加载...")
                    time.sleep(10)
                    current_url = self.driver.current_url
                    logger.info(f"{self._log_prefix()} 📄 更新后的页面: {current_url}")
                    
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
                
                cookies = self.driver.get_cookies()
                for cookie in cookies:
                    if cookie['name'] == '__Secure-C_SES':
                        secure_c_ses = cookie['value']
                    elif cookie['name'] == '__Host-C_OSES' and cookie.get('domain', '').endswith('gemini.google'):
                        host_c_oses = cookie['value']
            
            # 构建配置内容
            config_content = f"""Name={email or ''}
SECURE_C_SES={secure_c_ses or ''}
CSESIDX={csesidx or ''}
CONFIG_ID={config_id or ''}
HOST_C_OSES={host_c_oses or ''}

"""
            
            # 追加到文件
            try:
                import os
                file_exists = os.path.exists(output_file)
                
                with open(output_file, 'a', encoding='utf-8') as f:
                    if not file_exists:
                        f.write("# Gemini Business 配置信息（登录）\n")
                        f.write("# 格式: Name=邮箱, SECURE_C_SES=..., CSESIDX=..., CONFIG_ID=..., HOST_C_OSES=...\n")
                        f.write("# " + "=" * 60 + "\n\n")
                    
                    if file_exists:
                        f.write("# " + "-" * 60 + "\n\n")
                    
                    f.write(config_content)
                
                logger.info(f"{self._log_prefix()} ✅ 配置信息已追加到: {output_file}")
                return True
            except Exception as e:
                logger.error(f"{self._log_prefix()} ❌ 保存文件失败: {e}")
                return False
                
        except Exception as e:
            logger.error(f"{self._log_prefix()} ❌ 提取配置信息失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def close(self):
        """关闭浏览器并清理临时目录"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info(f"{self._log_prefix()} ✅ 浏览器已关闭")
            
            if hasattr(self, 'temp_user_data_dir') and self.temp_user_data_dir:
                import shutil
                import os
                if os.path.exists(self.temp_user_data_dir):
                    shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                    logger.info(f"{self._log_prefix()} 🧹 临时数据目录已清理")
        except Exception as e:
            logger.warning(f"{self._log_prefix()} ⚠️ 关闭浏览器或清理目录时出错: {e}")


def login_single_account(email: str, account_index: int, headless: bool = False, total_accounts: int = 1, close_browser: bool = True) -> bool:
    """
    登录单个账号
    
    Args:
        email: 账号邮箱
        account_index: 账号索引（从1开始）
        headless: 是否无头模式
        total_accounts: 总账号数
        close_browser: 登录成功后是否关闭浏览器（默认True，API调用时设为False）
        
    Returns:
        是否成功
    """
    start_time = time.time()
    logger.info(f"🔑 [{account_index}/{total_accounts}] 开始登录账号: {email}")
    
    login_client = GoogleBusinessLoginSelenium(email=email, headless=headless, account_index=account_index, total_accounts=total_accounts)
    gptmail = None
    
    try:
        # 1. 初始化浏览器驱动
        login_client.init_driver()
        
        # 2. 创建 GPTMail 客户端（使用已有邮箱接收验证码）
        gptmail = GPTMailClient(email_address=email, driver=login_client.driver, account_index=account_index, total_accounts=total_accounts)
        
        # 3. 访问 Google Business 登录页面
        if not login_client.start_login():
            logger.error(f"❌ [{account_index}/{total_accounts}] 无法访问登录页面")
            return False
        
        # 4. 提交邮箱并等待验证码
        if not login_client.submit_email():
            logger.error(f"❌ [{account_index}/{total_accounts}] 邮箱提交失败")
            return False
        
        # 等待验证邮件
        logger.info(f"⏳ [{account_index}/{total_accounts}] 等待 Google 验证邮件...")
        
        verification_code = gptmail.wait_for_email(
            sender_filter="accountverification.business.gemini.google",
            subject_filter="验证码",
            max_wait=120,
            check_interval=5
        )
        
        if not verification_code:
            logger.error(f"❌ [{account_index}/{total_accounts}] 未收到验证邮件或无法提取验证码")
            return False
        
        # 5. 提交验证码（登录时，验证码通过后直接到主页，不需要填姓名）
        submit_result = login_client.submit_verification_code(verification_code)
        
        # 如果验证后仍停留在验证页面，尝试重新发送验证码
        if submit_result == "STUCK":
            logger.warning(f"⚠️ [{account_index}/{total_accounts}] 验证后仍停留在验证页面，尝试重新发送验证码...")
            
            if login_client.resend_verification_code():
                logger.info(f"⏳ [{account_index}/{total_accounts}] 等待新的验证邮件...")
                time.sleep(5)
                
                new_verification_code = gptmail.wait_for_email(
                    sender_filter="accountverification.business.gemini.google",
                    subject_filter="验证码",
                    max_wait=30,
                    check_interval=3
                )
                
                if new_verification_code:
                    logger.info(f"✅ [{account_index}/{total_accounts}] 收到新的验证码，重新提交...")
                    submit_result = login_client.submit_verification_code(new_verification_code)
                    
                    if submit_result == "STUCK":
                        logger.error(f"❌ [{account_index}/{total_accounts}] 重新发送验证码后仍无法验证，账号可能被限制，跳过此账号")
                        return False
                else:
                    logger.error(f"❌ [{account_index}/{total_accounts}] 重新发送后未收到新的验证邮件，账号可能被限制，跳过此账号")
                    return False
            else:
                logger.error(f"❌ [{account_index}/{total_accounts}] 无法点击重新发送验证码按钮，账号可能被限制，跳过此账号")
                return False
        
        if not submit_result or submit_result == False:
            logger.error(f"❌ [{account_index}/{total_accounts}] 验证码提交失败")
            return False
        
        # 6. 登录成功后直接提取配置信息（不需要填姓名步骤）
        logger.info(f"⏳ [{account_index}/{total_accounts}] 等待页面完全加载...")
        time.sleep(5)
        
        # 提取配置信息
        config_file = "gemini_business_login_configs.txt"
        if login_client.extract_config_info(email=email, output_file=config_file):
            logger.info(f"✅ [{account_index}/{total_accounts}] 配置信息已追加到: {config_file}")
        else:
            logger.warning(f"⚠️ [{account_index}/{total_accounts}] 配置信息提取失败")
        
        # 计算耗时
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        logger.info(f"✅ [{account_index}/{total_accounts}] 登录流程完成！")
        logger.info(f"📧 [{account_index}/{total_accounts}] 登录邮箱: {email}")
        logger.info(f"⏱️  [{account_index}/{total_accounts}] 耗时: {minutes}分{seconds}秒 ({int(elapsed_time)}秒)")
        return True
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        logger.error(f"❌ [{account_index}/{total_accounts}] 登录过程出错: {e}")
        logger.info(f"⏱️  [{account_index}/{total_accounts}] 耗时: {minutes}分{seconds}秒 ({int(elapsed_time)}秒)")
        import traceback
        logger.debug(traceback.format_exc())
        return False
    
    finally:
        # 根据 close_browser 参数决定是否关闭浏览器
        if close_browser:
            if login_client:
                login_client.close()
        else:
            logger.info(f"🌐 [{account_index}/{total_accounts}] 浏览器保持打开状态")
        if gptmail:
            gptmail.close()


def load_emails_from_file(filepath: str) -> List[str]:
    """
    从文件加载邮箱列表
    
    Args:
        filepath: 文件路径，每行一个邮箱
        
    Returns:
        邮箱列表
    """
    emails = []
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):  # 忽略空行和注释
                        emails.append(line)
            logger.info(f"📁 从文件 {filepath} 加载了 {len(emails)} 个邮箱")
        except Exception as e:
            logger.error(f"❌ 读取邮箱文件失败: {e}")
    return emails


def main():
    """主函数"""
    total_start_time = time.time()
    
    # 使用代码中的配置
    headless = HEADLESS_MODE
    thread_count = THREAD_COUNT
    
    # 优先从环境变量读取单个邮箱（用于 API 调用）
    single_email = os.environ.get('LOGIN_SINGLE_ACCOUNT')
    # API 调用时不关闭浏览器
    close_browser_after_login = not bool(single_email)
    
    if single_email:
        emails = [single_email]
        logger.info(f"📧 从环境变量读取单个邮箱: {single_email}")
        logger.info(f"🌐 登录成功后浏览器将保持打开状态")
    else:
        # 获取邮箱列表
        emails = ACCOUNT_EMAILS.copy()
        
        # 如果代码中没有配置邮箱，尝试从文件读取
        if not emails:
            # 优先从环境变量指定的文件读取
            email_file = os.environ.get('LOGIN_EMAIL_FILE', ACCOUNT_EMAILS_FILE)
            emails = load_emails_from_file(email_file)
    
    if not emails:
        logger.error("❌ 没有找到需要登录的邮箱")
        logger.info("💡 请在代码中的 ACCOUNT_EMAILS 列表添加邮箱")
        logger.info(f"💡 或者创建 {ACCOUNT_EMAILS_FILE} 文件，每行一个邮箱")
        return
    
    account_count = len(emails)
    
    logger.info("🚀 开始 Google Business 批量登录流程")
    logger.info(f"⚙️  配置: 无头模式={headless}, 线程数={thread_count}, 账号数={account_count}")
    
    if thread_count > account_count:
        logger.warning(f"⚠️ 线程数({thread_count})大于账号数({account_count})，将线程数调整为{account_count}")
        thread_count = account_count
    
    if headless and thread_count > 1:
        logger.info("💡 提示：无头模式 + 多线程可以显著提高登录速度")
    
    # 使用线程池执行批量登录
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        # 提交所有任务
        futures = {
            executor.submit(login_single_account, email, i + 1, headless, account_count, close_browser_after_login): (i + 1, email)
            for i, email in enumerate(emails)
        }
        
        # 等待所有任务完成
        for future in as_completed(futures):
            account_index, email = futures[future]
            try:
                success = future.result()
                if success:
                    success_count += 1
                    logger.info(f"✅ 账号 {account_index} ({email}) 登录成功")
                else:
                    fail_count += 1
                    logger.error(f"❌ 账号 {account_index} ({email}) 登录失败")
            except Exception as e:
                fail_count += 1
                logger.error(f"❌ 账号 {account_index} ({email}) 登录异常: {e}")
    
    # 计算总耗时
    total_elapsed_time = time.time() - total_start_time
    total_hours = int(total_elapsed_time // 3600)
    total_minutes = int((total_elapsed_time % 3600) // 60)
    total_seconds = int(total_elapsed_time % 60)
    
    # 输出统计信息
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 批量登录统计报告")
    logger.info("=" * 70)
    logger.info(f"📋 总账号数:        {account_count}")
    logger.info(f"✅ 成功登录:        {success_count}")
    logger.info(f"❌ 登录失败:        {fail_count}")
    if account_count > 0:
        success_rate = success_count / account_count * 100
        logger.info(f"📈 成功率:          {success_rate:.1f}%")
    
    logger.info("-" * 70)
    if total_hours > 0:
        logger.info(f"⏱️  总耗时:          {total_hours}小时{total_minutes}分{total_seconds}秒 ({int(total_elapsed_time)}秒)")
    else:
        logger.info(f"⏱️  总耗时:          {total_minutes}分{total_seconds}秒 ({int(total_elapsed_time)}秒)")
    
    if success_count > 0:
        avg_time = total_elapsed_time / account_count
        avg_minutes = int(avg_time // 60)
        avg_seconds = int(avg_time % 60)
        logger.info(f"📊 平均耗时:        {avg_minutes}分{avg_seconds}秒 ({int(avg_time)}秒/账号)")
    
    if success_count > 0 and total_elapsed_time > 0:
        speed = success_count / (total_elapsed_time / 60)
        logger.info(f"⚡ 登录速度:        {speed:.2f} 账号/分钟")
    
    logger.info("-" * 70)
    logger.info(f"📁 配置文件:        gemini_business_login_configs.txt")
    logger.info("=" * 70)
    logger.info("")
    
    # 如果不关闭浏览器（API调用），保持脚本运行
    if not close_browser_after_login and success_count > 0:
        logger.info("🌐 浏览器保持打开状态，脚本将继续运行...")
        logger.info("💡 请在浏览器中完成操作后手动关闭浏览器")
        # 无限等待，直到进程被外部终止
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("👋 收到退出信号，脚本结束")


if __name__ == "__main__":
    main()
