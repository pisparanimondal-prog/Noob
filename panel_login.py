"""
Panel Auto-Login
Uses Chromium + Xvfb virtual display to login to the SMS panel and extract
session credentials (PHPSESSID + sesskey) without requiring a physical screen.
"""
import re
import time
import logging
from urllib.parse import urlparse

logger = logging.getLogger('PanelLogin')


def auto_login_panel(base_url, username, password, progress_cb=None):
    """
    Log into the SMS panel using a real browser with virtual display.

    Args:
        base_url : Panel base URL, e.g. "http://51.68.180.239/ints"
        username : Panel username
        password : Panel password
        progress_cb: Optional callable(str) for live status updates

    Returns:
        dict: {phpsessid, sesskey, data_url, referer, name, base_url}

    Raises:
        Exception on login failure or credential extraction failure.
    """

    def log(msg):
        logger.info(msg)
        if progress_cb:
            try:
                progress_cb(msg)
            except Exception:
                pass

    display = None
    driver = None

    try:
        # ── 1. Virtual display (Xvfb) ────────────────────────────────────────
        from pyvirtualdisplay import Display
        display = Display(visible=0, size=(1366, 768))
        display.start()
        log("🖥 Virtual display started")

        # ── 2. Browser setup ─────────────────────────────────────────────────
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        import shutil
        chromedriver_path = shutil.which('chromedriver') or 'chromedriver'
        chromium_path = shutil.which('chromium') or 'chromium'

        opts = Options()
        opts.binary_location = chromium_path
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1366,768")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-extensions")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=opts)

        # Mask automation flag
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
        })
        log("🌐 Browser started (Chromium)")

        # ── 3. Navigate to login ─────────────────────────────────────────────
        login_url = base_url.rstrip('/') + '/login'
        driver.get(login_url)
        log(f"🔑 Login page loaded")
        time.sleep(4)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )

        username_field = driver.find_element(By.NAME, "username")
        password_field = driver.find_element(By.NAME, "password")
        username_field.clear()
        password_field.clear()
        username_field.send_keys(username)
        password_field.send_keys(password)
        log("✏️ Credentials entered")
        time.sleep(2)

        # ── 4. CAPTCHA (math: What is X + Y?) ───────────────────────────────
        page_src = driver.page_source
        captcha_match = re.search(r'What is\s*(\d+)\s*\+\s*(\d+)', page_src)

        if captcha_match:
            num1 = int(captcha_match.group(1))
            num2 = int(captcha_match.group(2))
            answer = num1 + num2
            log(f"🧮 CAPTCHA: {num1} + {num2} = {answer}")

            captcha_field = None
            for selector in ["input[name='capt']", "input[name='captcha']"]:
                try:
                    f = driver.find_element(By.CSS_SELECTOR, selector)
                    if f.is_displayed():
                        captcha_field = f
                        break
                except Exception:
                    continue

            if not captcha_field:
                for inp in driver.find_elements(By.TAG_NAME, "input"):
                    if inp.is_displayed() and inp.get_attribute("type") in ("text", "number"):
                        field_name = inp.get_attribute("name") or ""
                        if "username" not in field_name and "password" not in field_name:
                            captcha_field = inp
                            break

            if captcha_field:
                captcha_field.clear()
                captcha_field.send_keys(str(answer))
                captcha_field.send_keys(Keys.RETURN)
                log("✅ CAPTCHA solved and submitted")
            else:
                password_field.send_keys(Keys.RETURN)
                log("⚠️ CAPTCHA field not found, submitted without it")
        else:
            password_field.send_keys(Keys.RETURN)
            log("ℹ️ No CAPTCHA detected, form submitted")

        time.sleep(5)

        # ── 5. Verify login success ──────────────────────────────────────────
        current_url = driver.current_url
        log(f"🌐 Post-login URL: {current_url}")
        if "login" in current_url.lower():
            raise Exception(
                "Login failed — wrong username/password, CAPTCHA error, or panel blocked the request."
            )
        log("✅ Login successful!")

        # ── 6. Extract PHPSESSID ─────────────────────────────────────────────
        cookies = driver.get_cookies()
        phpsessid = next((c['value'] for c in cookies if c['name'] == 'PHPSESSID'), '')
        if not phpsessid:
            raise Exception("PHPSESSID not found in browser cookies after login.")
        log(f"🍪 PHPSESSID: {phpsessid[:8]}...")

        # ── 7. Navigate to SMS CDR reports and capture sesskey ───────────────
        parsed = urlparse(base_url)
        host_base = base_url.rstrip('/')
        referer_url = host_base + '/agent/SMSCDRReports'
        data_url = host_base + '/agent/res/data_smscdr.php'

        driver.get(referer_url)
        log("📊 Loaded SMS CDR Reports page — waiting for AJAX...")
        time.sleep(6)

        sesskey = ''

        # Method A: scan performance resource entries for data_smscdr.php URL
        try:
            urls_js = driver.execute_script(
                "return window.performance.getEntriesByType('resource')"
                "  .map(e => e.name)"
                "  .filter(n => n.indexOf('data_smscdr') > -1);"
            )
            for url in (urls_js or []):
                m = re.search(r'sesskey=([^&]+)', url)
                if m:
                    sesskey = m.group(1)
                    log(f"🔑 Sesskey from network log: {sesskey[:10]}...")
                    break
        except Exception:
            pass

        # Method B: search page source for sesskey value
        if not sesskey:
            page_src2 = driver.page_source
            for pattern in [
                r'["\']sesskey["\']\s*[:=]\s*["\']([^"\']{4,})["\']',
                r'sesskey=([A-Za-z0-9+/=]{4,})',
            ]:
                m = re.search(pattern, page_src2)
                if m:
                    sesskey = m.group(1)
                    log(f"🔑 Sesskey from page source: {sesskey[:10]}...")
                    break

        if sesskey:
            log(f"✅ All credentials extracted successfully")
        else:
            log(f"⚠️ Sesskey not found — PHPSESSID extracted but sesskey missing")

        hostname = parsed.hostname or host_base
        return {
            'phpsessid': phpsessid,
            'sesskey': sesskey,
            'data_url': data_url,
            'referer': referer_url,
            'name': f'Panel {hostname}',
            'base_url': base_url,
        }

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            log("🔒 Browser closed")
        if display:
            try:
                display.stop()
            except Exception:
                pass
            log("🖥 Virtual display stopped")
