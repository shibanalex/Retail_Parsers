import os
import time
from selenium import webdriver
from selenium_stealth import stealth
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


current_dir = os.path.dirname(os.path.abspath(__file__))

parser_root = os.path.dirname(current_dir)

PROFILE_DIR = os.path.join(parser_root, "magazinnoff_profile")


def init_driver(headless=False):
    print(f"🌐 Профиль Chrome: {PROFILE_DIR}")
    
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={PROFILE_DIR}") # Путь к профилю
    
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-popup-blocking")
    
    if headless:
        options.add_argument("--headless=new")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"❌ Не удалось запустить драйвер: {e}")
        raise e

    stealth(driver,
        languages=["ru-RU", "ru"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver

def wait_for_humanity(driver, timeout=30):
    """Ожидание прохождения защиты Cloudflare/AntiBot"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: "Just a moment" not in d.title and 
                      "Checking your browser" not in d.page_source
        )
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/magazin/'], footer"))
        )
        time.sleep(1)
        return True
    except Exception:
        return False

def save_debug_html(driver, name_prefix):
    debug_dir = os.path.join(parser_root, "debug")
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)
        
    filename = os.path.join(debug_dir, f"{name_prefix}.html")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(driver.page_source)