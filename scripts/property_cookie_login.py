#!/usr/bin/env python3
"""
Property.ie Cookie Login Script

Opens Chrome locally (visible window), navigates to Property.ie,
waits for you to solve the Cloudflare challenge manually,
then saves the cookies to Redis for the Docker collector to use.

Usage:
    # From project root, with venv activated:
    python scripts/property_cookie_login.py

    # Or specify Redis host if Docker Redis is on a different host:
    REDIS_HOST=localhost REDIS_PORT=16379 python scripts/property_cookie_login.py

After running:
    1. Chrome opens → Property.ie loads
    2. Solve the Cloudflare "Verify you are human" challenge
    3. Wait for the listing page to load
    4. Press ENTER in the terminal
    5. Cookies are saved to Redis → Docker collector will use them
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    """Open Chrome, let user solve CF, save cookies to Redis."""
    import undetected_chromedriver as uc
    import redis

    # ── Config ────────────────────────────────────────────────────────────────
    PROPERTY_URL = "https://www.property.ie/property-to-let/ireland/price_international_rental-onceoff_standard/"
    REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.environ.get("REDIS_PORT", 16379))
    REDIS_DB = int(os.environ.get("REDIS_DB", 0))
    COOKIE_TTL_HOURS = 72
    SOURCE = "property"

    print("=" * 60)
    print("  Property.ie Cookie Login — Cloudflare Bypass")
    print("=" * 60)
    print()

    # ── Start Chrome ──────────────────────────────────────────────────────────
    print("[1/4] Starting Chrome (visible window)...")
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--lang=en-IE")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")

    # Detect Chrome version
    chrome_version = None
    import subprocess as _sp
    for chrome_bin in ["google-chrome", "chromium-browser", "chromium", "google-chrome-stable"]:
        try:
            out = _sp.check_output([chrome_bin, "--version"], text=True, stderr=_sp.DEVNULL)
            ver = int(out.strip().split()[-1].split(".")[0])
            chrome_version = ver
            print(f"      Detected Chrome version: {ver}")
            break
        except Exception:
            continue

    driver = uc.Chrome(
        options=options,
        use_subprocess=True,
        version_main=chrome_version,
    )
    driver.set_page_load_timeout(60)

    # Inject stealth JS
    driver.execute_cdp_cmd(
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

    # ── Navigate ──────────────────────────────────────────────────────────────
    print(f"[2/4] Navigating to Property.ie")
    print("      Solve the Cloudflare challenge in the browser window...")
    print()
    driver.get(PROPERTY_URL)

    # ── Wait for user ─────────────────────────────────────────────────────────
    print("⏳ Waiting for Cloudflare to be solved...")
    print("   (The title will change when you pass the challenge)")
    print()

    max_wait = 300  # 5 minutes
    start = time.time()
    solved = False

    while time.time() - start < max_wait:
        try:
            title = driver.title.lower()
            if "just a moment" not in title and "cloudflare" not in title:
                print(f"   ✅ Challenge solved! Page title: '{driver.title}'")
                solved = True
                # Wait a moment for all cookies to settle
                time.sleep(3)
                break
        except Exception:
            pass
        time.sleep(1)

    if not solved:
        print("   ⚠️  Timeout waiting for challenge. Trying to save cookies anyway...")

    # ── Extra: let user browse if needed ──────────────────────────────────────
    print()
    input("   Press ENTER when ready to save cookies and close Chrome... ")

    # ── Extract cookies ───────────────────────────────────────────────────────
    print("[3/4] Extracting cookies...")
    selenium_cookies = driver.get_cookies()
    print(f"      Found {len(selenium_cookies)} cookies")

    # Convert Selenium cookies to Playwright format (for compatibility)
    playwright_cookies = []
    for sc in selenium_cookies:
        pc = {
            "name": sc["name"],
            "value": sc["value"],
            "domain": sc.get("domain", ""),
            "path": sc.get("path", "/"),
            "secure": sc.get("secure", False),
            "httpOnly": sc.get("httpOnly", False),
        }
        # Expiry → expires (Playwright uses float timestamp)
        if "expiry" in sc:
            pc["expires"] = sc["expiry"]
        playwright_cookies.append(pc)

    # Show key cookies
    cf_cookies = [c for c in playwright_cookies if "cf_" in c["name"].lower() or "clearance" in c["name"].lower()]
    if cf_cookies:
        print(f"      🔑 Cloudflare cookies found: {[c['name'] for c in cf_cookies]}")
    else:
        print("      ⚠️  No cf_clearance cookie found (challenge may not have been solved)")

    # ── Save to Redis ─────────────────────────────────────────────────────────
    print(f"[4/4] Saving to Redis ({REDIS_HOST}:{REDIS_PORT}/{REDIS_DB})...")

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()

        session_data = {
            "cookies": playwright_cookies,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(hours=COOKIE_TTL_HOURS)).isoformat(),
        }

        key = f"session:{SOURCE}"
        r.setex(key, COOKIE_TTL_HOURS * 3600, json.dumps(session_data, default=str))

        print(f"      ✅ Saved {len(playwright_cookies)} cookies to Redis key '{key}'")
        print(f"      ⏰ TTL: {COOKIE_TTL_HOURS} hours")
        print(f"      📋 Expires at: {session_data['expires_at']}")

    except redis.ConnectionError:
        print(f"      ❌ Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        print("         Make sure Redis is running (or use Docker Redis with port forwarding)")
        print()
        # Fallback: save to file
        fallback_path = os.path.join(os.path.dirname(__file__), "property_cookies.json")
        with open(fallback_path, "w") as f:
            json.dump(playwright_cookies, f, indent=2, default=str)
        print(f"      📁 Cookies saved to file instead: {fallback_path}")
        print("         You can manually load these into Redis later.")

    # ── Cleanup ───────────────────────────────────────────────────────────────
    driver.quit()
    print()
    print("=" * 60)
    print("  Done! Docker collector will use these cookies next run.")
    print("=" * 60)


if __name__ == "__main__":
    main()
