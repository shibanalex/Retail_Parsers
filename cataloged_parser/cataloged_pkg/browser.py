import os
import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

current_dir = os.path.dirname(os.path.abspath(__file__))
parser_root = os.path.dirname(current_dir)
PROFILE_DIR = os.path.join(parser_root, "cataloged_profile")

def get_options(headless):
    """Генерация чистых опций для каждого запуска"""
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-popup-blocking")
    
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    if headless:
        options.add_argument("--headless=new")
    return options

def init_driver(headless=False):
    print(f"🌐 Профиль Chrome: {PROFILE_DIR}")
    
    try:
        driver = uc.Chrome(
            options=get_options(headless), 
            user_data_dir=PROFILE_DIR, 
            version_main=145, 
            use_subprocess=True
        )
    except Exception:
        driver = uc.Chrome(
            options=get_options(headless), 
            user_data_dir=PROFILE_DIR, 
            use_subprocess=True
        )

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    
    return driver

def wait_for_antibot(driver, timeout=40):

    try:
        WebDriverWait(driver, 5).until(lambda d: d.execute_script('return document.readyState') == 'complete')
        time.sleep(1)
        
        src = driver.page_source
        
        if "Я не робот" in src or "Loading..." in src or "adblock-blocker" in src:
            
            try:
                buttons = driver.find_elements(By.XPATH, "//div[contains(text(), 'Я не робот')]")
                for btn in buttons:
                    if btn.is_displayed():
                        print("   🤖 Нажимаю кнопку 'Я не робот'...")
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                        break
            except:
                pass
            
            WebDriverWait(driver, timeout).until(
                lambda d: "Loading..." not in d.page_source and 
                          ("rec__item" in d.page_source or "promo__item" in d.page_source)
            )
            time.sleep(1.5)
    except Exception:
        pass