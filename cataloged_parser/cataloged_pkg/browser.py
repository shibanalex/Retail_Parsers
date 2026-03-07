import os
import time
import shutil
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

current_dir = os.path.dirname(os.path.abspath(__file__))
parser_root = os.path.dirname(current_dir)
PROFILE_DIR = os.path.join(parser_root, "cataloged_profile")

def init_driver(headless=False):
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={PROFILE_DIR}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-popup-blocking")
    
    if headless:
        options.add_argument("--headless=new")

    try:
        driver = uc.Chrome(
            options=options, 
            version_main=145, 
            use_subprocess=True
        )
    except Exception:
        print("⚠️ Ошибка драйвера 145. Пробую авто-версию...")
        driver = uc.Chrome(options=options, use_subprocess=True)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    
    return driver

def wait_for_antibot(driver, timeout=30):
    try:
        WebDriverWait(driver, 5).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        
        src = driver.page_source
        if "Loading..." in src or "Я не робот" in src or "adblock-blocker" in src:
            
            try:
                btn = driver.find_element(By.XPATH, "//div[contains(text(), 'Я не робот')]")
                if btn:
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(2)
            except:
                pass

            WebDriverWait(driver, timeout).until(
                lambda d: "Loading..." not in d.page_source and 
                          ("rec__item" in d.page_source or "promo__item" in d.page_source)
            )
            time.sleep(2)
    except Exception:
        pass