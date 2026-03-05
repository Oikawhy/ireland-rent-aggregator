"""
Undetected ChromeDriver wrapper for Cloudflare-protected sites.

This module provides a Playwright-like interface for undetected-chromedriver,
specifically designed for Daft.ie and other Cloudflare-protected sites.

Includes human-like behavior simulation to bypass CAPTCHA challenges:
- Random mouse movements before page interactions
- Natural delays between actions
- Cloudflare challenge auto-wait
"""

import asyncio
import logging
import random
import time
from typing import List, Optional, Any
from dataclasses import dataclass

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementNotInteractableException,
)

from packages.observability.logger import get_logger


logger = get_logger("uc_driver")

# Shared User-Agent — MUST match between daft_cookie_login.py and Docker collector.
# Cloudflare binds cf_clearance to UA + IP + TLS fingerprint.
STEALTH_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)


def _human_delay(min_s: float = 0.3, max_s: float = 1.2) -> None:
    """Sleep a random human-like duration (synchronous)."""
    time.sleep(random.uniform(min_s, max_s))


@dataclass
class UCElement:
    """Wrapper for Selenium WebElement to provide Playwright-like interface."""
    _element: Any

    def get_attribute(self, name: str) -> Optional[str]:
        """Get element attribute."""
        try:
            return self._element.get_attribute(name)
        except StaleElementReferenceException:
            return None

    @property
    def text(self) -> str:
        """Get element text content."""
        try:
            return self._element.text
        except StaleElementReferenceException:
            return ""

    async def inner_text(self) -> str:
        """Get element text (async for Playwright compatibility)."""
        return self.text

    async def inner_html(self) -> str:
        """Get element inner HTML (async for Playwright compatibility)."""
        try:
            return self._element.get_attribute("innerHTML") or ""
        except StaleElementReferenceException:
            return ""

    async def click(self) -> None:
        """Click element with human-like delay."""
        _human_delay(0.1, 0.4)
        try:
            self._element.click()
        except ElementNotInteractableException:
            # Try JS click as fallback
            driver = self._element.parent
            driver.execute_script("arguments[0].click();", self._element)

    async def query_selector(self, selector: str) -> Optional["UCElement"]:
        """Find child element by CSS selector."""
        try:
            elem = self._element.find_element(By.CSS_SELECTOR, selector)
            return UCElement(_element=elem)
        except (NoSuchElementException, StaleElementReferenceException):
            return None

    async def query_selector_all(self, selector: str) -> List["UCElement"]:
        """Find all child elements by CSS selector."""
        try:
            elements = self._element.find_elements(By.CSS_SELECTOR, selector)
            return [UCElement(_element=e) for e in elements]
        except StaleElementReferenceException:
            return []


class UCPage:
    """Wrapper for Selenium WebDriver to provide Playwright Page-like interface."""

    def __init__(self, driver: uc.Chrome):
        self._driver = driver
        self._default_timeout = 30000  # ms

    def _simulate_human_mouse(self) -> None:
        """Simulate random mouse movements to appear human-like."""
        try:
            actions = ActionChains(self._driver)
            # Move mouse to 2-4 random positions
            for _ in range(random.randint(2, 4)):
                x = random.randint(100, 800)
                y = random.randint(100, 500)
                actions.move_by_offset(
                    random.randint(-50, 50),
                    random.randint(-50, 50),
                )
                _human_delay(0.1, 0.3)
            actions.perform()
        except Exception:
            pass  # Non-critical — don't fail scrape over mouse movement

    async def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = None, **kwargs) -> None:
        """Navigate to URL with cookie injection and human simulation.

        If Redis has saved cookies for the domain (from manual login),
        they are injected before navigation to bypass Cloudflare.
        """
        # Try to load and inject cookies before the real navigation
        cookies_injected = await self._inject_saved_cookies(url)

        def _navigate():
            if timeout:
                self._driver.set_page_load_timeout(timeout / 1000)
            self._driver.get(url)
            # Wait for document ready
            wait_sec = (timeout / 1000) if timeout else (self._default_timeout / 1000)
            WebDriverWait(self._driver, wait_sec).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

        await asyncio.get_event_loop().run_in_executor(None, _navigate)

        # Wait for page to settle + simulate human presence
        await asyncio.sleep(random.uniform(1.5, 3.0))

        # Simulate mouse movement (makes Cloudflare think a human is present)
        await asyncio.get_event_loop().run_in_executor(
            None, self._simulate_human_mouse
        )

        # Check for Cloudflare challenge page and wait if needed
        await self._wait_for_cloudflare_challenge()

    async def _inject_saved_cookies(self, target_url: str) -> bool:
        """Load cookies from Redis session store and inject into browser.

        Must visit the domain first (empty page) to set cookie scope,
        then add cookies and the real navigation will use them.
        """
        try:
            from urllib.parse import urlparse
            from services.collector.session_store import SessionStore

            # Determine source from URL
            domain = urlparse(target_url).hostname or ""
            source = None
            if "daft" in domain:
                source = "daft"
            elif "rent" in domain:
                source = "rent"
            elif "property" in domain:
                source = "property"

            if not source:
                return False

            store = SessionStore()
            session_data = store.load_session(source)
            if not session_data:
                logger.info("No saved cookies found", source=source)
                return False

            cookies = session_data.get("cookies", [])
            if not cookies:
                return False

            # Navigate to domain first (blank page to set cookie scope)
            base_url = f"https://{domain}/"
            def _visit_base():
                self._driver.get(base_url)
                time.sleep(2)

            await asyncio.get_event_loop().run_in_executor(None, _visit_base)

            # Inject cookies
            def _add_cookies():
                added = 0
                for cookie in cookies:
                    try:
                        selenium_cookie = {
                            "name": cookie["name"],
                            "value": cookie["value"],
                            "domain": cookie.get("domain", domain),
                            "path": cookie.get("path", "/"),
                            "secure": cookie.get("secure", False),
                        }
                        # Only add httpOnly if True (some drivers reject False)
                        if cookie.get("httpOnly"):
                            selenium_cookie["httpOnly"] = True
                        # Convert expires (float timestamp) to expiry (int)
                        if "expires" in cookie and cookie["expires"]:
                            selenium_cookie["expiry"] = int(cookie["expires"])

                        self._driver.add_cookie(selenium_cookie)
                        added += 1
                    except Exception as e:
                        logger.debug("Cookie inject failed", name=cookie.get("name"), error=str(e))
                return added

            added = await asyncio.get_event_loop().run_in_executor(None, _add_cookies)
            logger.info("Cookies injected from Redis", source=source, count=added, total=len(cookies))
            return added > 0

        except Exception as e:
            logger.warning("Cookie injection failed", error=str(e))
            return False

    async def save_cookies_to_store(self, source: str) -> None:
        """Save current browser cookies to Redis for future use."""
        try:
            from services.collector.session_store import SessionStore
            from datetime import datetime, timedelta

            def _get_cookies():
                return self._driver.get_cookies()

            selenium_cookies = await asyncio.get_event_loop().run_in_executor(None, _get_cookies)

            # Convert to Playwright format
            cookies = []
            for sc in selenium_cookies:
                pc = {
                    "name": sc["name"],
                    "value": sc["value"],
                    "domain": sc.get("domain", ""),
                    "path": sc.get("path", "/"),
                    "secure": sc.get("secure", False),
                    "httpOnly": sc.get("httpOnly", False),
                }
                if "expiry" in sc:
                    pc["expires"] = sc["expiry"]
                cookies.append(pc)

            store = SessionStore()
            store.save_session(
                source=source,
                data={
                    "cookies": cookies,
                    "created_at": datetime.utcnow().isoformat(),
                    "expires_at": (datetime.utcnow() + timedelta(hours=72)).isoformat(),
                },
                ttl_hours=72,
            )
            logger.info("UC cookies saved to Redis", source=source, count=len(cookies))

        except Exception as e:
            logger.warning("Failed to save UC cookies", error=str(e))

    async def _wait_for_cloudflare_challenge(self, max_wait: int = 45) -> None:
        """Wait for Cloudflare challenge to auto-resolve.

        Timeout increased to 45s to give Turnstile time to verify.
        Also attempts to click the Turnstile checkbox if found.
        """
        start = time.time()
        turnstile_clicked = False

        while time.time() - start < max_wait:
            try:
                title = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._driver.title.lower()
                )
                page_source = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._driver.page_source[:500].lower()
                )

                # Check if we're on a challenge page
                is_challenge = any(kw in title for kw in [
                    "just a moment", "attention required", "checking your browser",
                    "security check", "cloudflare",
                ])
                is_challenge = is_challenge or "cf-challenge" in page_source
                is_challenge = is_challenge or "turnstile" in page_source

                if not is_challenge:
                    return  # Challenge passed or not present

                # Try to click Turnstile checkbox (once)
                if not turnstile_clicked:
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self._click_turnstile_checkbox
                        )
                        turnstile_clicked = True
                    except Exception:
                        pass

                elapsed = time.time() - start
                logger.debug(f"Cloudflare challenge detected, waiting... elapsed={elapsed:.1f}s")

                # Simulate mouse movement while waiting (helps pass challenge)
                await asyncio.get_event_loop().run_in_executor(
                    None, self._simulate_human_mouse
                )
                await asyncio.sleep(random.uniform(1.0, 2.0))

            except Exception:
                await asyncio.sleep(1)

        logger.warning(f"Cloudflare challenge wait exceeded wait_seconds={max_wait}")

    def _click_turnstile_checkbox(self) -> None:
        """Try to click the Cloudflare Turnstile checkbox inside its iframe."""
        try:
            # Find Turnstile iframe
            iframes = self._driver.find_elements(By.TAG_NAME, "iframe")
            logger.info("Looking for Turnstile iframe", iframe_count=len(iframes))
            for iframe in iframes:
                src = iframe.get_attribute("src") or ""
                logger.debug("Checking iframe", src=src[:80])
                if "challenges.cloudflare.com" in src or "turnstile" in src:
                    logger.info("Found Turnstile iframe", src=src[:80])
                    self._driver.switch_to.frame(iframe)
                    try:
                        # Try to click the checkbox
                        checkbox = self._driver.find_element(
                            By.CSS_SELECTOR, "input[type='checkbox'], .cb-lb"
                        )
                        _human_delay(0.5, 1.0)
                        checkbox.click()
                        logger.info("Clicked Turnstile checkbox")
                    except NoSuchElementException:
                        # Try clicking center of iframe as fallback
                        body = self._driver.find_element(By.TAG_NAME, "body")
                        ActionChains(self._driver).move_to_element(body).click().perform()
                        logger.info("Clicked Turnstile iframe body")
                    finally:
                        self._driver.switch_to.default_content()
                    return
            logger.warning("No Turnstile iframe found among iframes")
        except Exception as e:
            logger.warning("Turnstile click failed", error=str(e))

    async def query_selector(self, selector: str) -> Optional[UCElement]:
        """Find element by CSS selector."""
        def _find():
            try:
                elem = self._driver.find_element(By.CSS_SELECTOR, selector)
                return UCElement(_element=elem)
            except NoSuchElementException:
                return None

        return await asyncio.get_event_loop().run_in_executor(None, _find)

    async def query_selector_all(self, selector: str) -> List[UCElement]:
        """Find all elements by CSS selector."""
        def _find_all():
            elements = self._driver.find_elements(By.CSS_SELECTOR, selector)
            return [UCElement(_element=e) for e in elements]

        return await asyncio.get_event_loop().run_in_executor(None, _find_all)

    async def wait_for_selector(
        self,
        selector: str,
        timeout: int = 30000,
        state: str = "visible",
    ) -> Optional[UCElement]:
        """Wait for element to appear."""
        def _wait():
            try:
                if state == "visible":
                    condition = EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, selector)
                    )
                else:
                    condition = EC.presence_of_element_located(
                        (By.CSS_SELECTOR, selector)
                    )
                elem = WebDriverWait(self._driver, timeout / 1000).until(condition)
                return UCElement(_element=elem)
            except TimeoutException:
                return None

        return await asyncio.get_event_loop().run_in_executor(None, _wait)

    async def wait_for_timeout(self, ms: int) -> None:
        """Wait for specified milliseconds (Playwright compatibility)."""
        await asyncio.sleep(ms / 1000)

    async def evaluate(self, script: str) -> Any:
        """Execute JavaScript and return result."""
        def _eval():
            if script.strip().startswith("(") or script.strip().startswith("return"):
                return self._driver.execute_script(script)
            else:
                return self._driver.execute_script(f"return {script}")

        return await asyncio.get_event_loop().run_in_executor(None, _eval)

    async def scroll_to_bottom(self) -> None:
        """Scroll to bottom of page with human-like behavior."""
        await self.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(0.5, 1.5))

    async def wait_for_load_state(self, state: str = "domcontentloaded") -> None:
        """Wait for page load state (Playwright compatibility shim)."""
        await asyncio.sleep(random.uniform(1.5, 3.0))
        # Simulate mouse movement after page load
        await asyncio.get_event_loop().run_in_executor(
            None, self._simulate_human_mouse
        )

    async def close(self) -> None:
        """Close this page/tab."""
        pass  # UC driver handles single page


class UCDriver:
    """
    Undetected ChromeDriver manager with Playwright-like interface.

    Includes anti-detection features:
    - Random window size variation
    - Human-like interaction simulation
    - Cloudflare challenge auto-wait

    Usage:
        driver = UCDriver()
        await driver.start()
        page = await driver.new_page()
        await page.goto("https://daft.ie")
        await driver.stop()
    """

    def __init__(self):
        self._driver: Optional[uc.Chrome] = None
        self._page: Optional[UCPage] = None
        self._xvfb_proc = None
        self._xvfb_display: Optional[str] = None
        self.logger = get_logger("uc_driver.manager")

    @staticmethod
    def _detect_chrome_version() -> int | None:
        """Detect installed Chrome/Chromium version to avoid chromedriver mismatch."""
        import subprocess
        for cmd in [
            ["google-chrome", "--version"],
            ["google-chrome-stable", "--version"],
            ["chromium-browser", "--version"],
            ["chromium", "--version"],
        ]:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    import re
                    match = re.search(r"(\d+)\.", result.stdout)
                    if match:
                        version = int(match.group(1))
                        logger.info(f"Detected Chrome version: {version}")
                        return version
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue
        logger.warning("Could not detect Chrome version, using auto-detect")
        return None

    async def start(self) -> None:
        """Start the undetected Chrome browser with anti-detection settings.

        In Docker: uses Xvfb virtual display instead of headless mode.
        Cloudflare Turnstile cannot be bypassed in headless — it requires
        a visible window with real mouse interaction.
        """
        def _start_browser():
            import os
            import pathlib
            import subprocess

            options = uc.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")

            # Detect Docker environment
            is_docker = (
                pathlib.Path("/.dockerenv").exists()
                or os.environ.get("DOCKER_CONTAINER", "") == "true"
            )

            if is_docker:
                # Start Xvfb virtual display (fake screen for Chrome)
                # This lets UC Chrome run non-headless in Docker,
                # which is required for solving Cloudflare Turnstile
                try:
                    display_num = random.randint(10, 99)
                    self._xvfb_display = f":{display_num}"
                    self._xvfb_proc = subprocess.Popen(
                        ["Xvfb", self._xvfb_display, "-screen", "0", "1280x900x24",
                         "-nolisten", "tcp", "-ac"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    os.environ["DISPLAY"] = self._xvfb_display
                    time.sleep(0.5)  # Let Xvfb start
                    self.logger.info("Xvfb started", display=self._xvfb_display)
                except Exception as e:
                    self.logger.warning("Xvfb failed, falling back to headless", error=str(e))
                    options.add_argument("--headless=new")
                    self._xvfb_proc = None

                options.add_argument("--disable-software-rasterizer")

            # Randomize window size slightly to avoid fingerprinting
            width = random.randint(1050, 1250)
            height = random.randint(700, 850)
            options.add_argument(f"--window-size={width},{height}")

            options.add_argument("--lang=en-IE")
            options.add_argument("--disable-blink-features=AutomationControlled")

            # Additional stealth flags
            options.add_argument("--disable-infobars")

            # Fixed User-Agent (must match daft_cookie_login.py)
            options.add_argument(f"--user-agent={STEALTH_USER_AGENT}")

            chrome_kwargs = dict(
                options=options,
                use_subprocess=not is_docker,
                version_main=self._detect_chrome_version(),
            )

            if is_docker:
                # Use writable copies of chromium & chromedriver.
                # undetected-chromedriver patches the chromedriver binary in-place
                # (removes $cdc_ markers), so it needs write access.
                # /usr/bin/ is owned by root; /app/ copies are owned by agpars.
                chromium_path = "/app/chromium"
                chromedriver_path = "/app/chromedriver"
                if os.path.exists(chromium_path):
                    chrome_kwargs["browser_executable_path"] = chromium_path
                    self.logger.info("Using system Chromium", path=chromium_path)
                if os.path.exists(chromedriver_path):
                    chrome_kwargs["driver_executable_path"] = chromedriver_path
                    self.logger.info("Using system chromedriver", path=chromedriver_path)

            self._driver = uc.Chrome(**chrome_kwargs)
            self._driver.set_page_load_timeout(60)

            # Inject stealth JS to remove automation markers
            self._driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-IE', 'en-GB', 'en']});
                        window.chrome = { runtime: {} };
                    """
                },
            )

        await asyncio.get_event_loop().run_in_executor(None, _start_browser)
        self._page = UCPage(self._driver)
        self.logger.info("UC Driver started")

    async def new_page(self) -> UCPage:
        """Get the current page (UC uses single page model)."""
        if not self._page:
            raise RuntimeError("Driver not started. Call start() first.")
        return self._page

    async def stop(self) -> None:
        """Stop the browser and clean up Xvfb."""
        def _stop():
            if self._driver:
                try:
                    self._driver.quit()
                except Exception as e:
                    self.logger.warning(f"Error closing driver: {e}")
            # Kill Xvfb if running
            if self._xvfb_proc:
                try:
                    self._xvfb_proc.terminate()
                    self._xvfb_proc.wait(timeout=5)
                    self.logger.info("Xvfb stopped")
                except Exception:
                    self._xvfb_proc.kill()

        await asyncio.get_event_loop().run_in_executor(None, _stop)
        self._driver = None
        self._page = None
        self.logger.info("UC Driver stopped")

    @property
    def is_running(self) -> bool:
        """Check if driver is running."""
        return self._driver is not None
