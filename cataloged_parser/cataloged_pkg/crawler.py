import time
import requests
import config
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import update_retail_points
from .browser import init_driver, wait_for_antibot
from .html_parser import transliterate_city, parse_shops_list, parse_products_page, get_max_page, get_category_from_html

def is_product_match(name, keywords):
    if not keywords: return True 
    n_low = name.lower().replace("ё", "е")
    for q in keywords:
        req_words = q.lower().replace("ё", "е").split()
        if all(word in n_low for word in req_words):
            return True
    return False

def fetch_page_fast(session, url, keywords, city, shop_name, parser_type):
    try:
        resp = session.get(url, timeout=20)
        if "Loading..." in resp.text or "adblock-blocker" in resp.text:
            return []
        
        products = parse_products_page(resp.text)
        matches = []
        
        for p in products:
            if is_product_match(p['name'], keywords):
                cat = None
                try:
                    if p['link']:
                        r = session.get(p['link'], timeout=10)
                        cat = get_category_from_html(r.text)
                except:
                    pass

                matches.append({
                    "Номер": 0,
                    "Сеть": shop_name,
                    "Тип магазина": parser_type,
                    "Адрес Торговой точки": city,
                    "Бренд": None,
                    "Название продукта": p['name'],
                    "Цена": p['price'],
                    "Цена по акции": None,
                    "Фото товара": p['img'],
                    "Ссылка на страницу": p['link'],
                    "Рейтинг": None,
                    "Объем": None,
                    "Вес": None,
                    "Остаток": None,
                    "Категория": cat
                })
        return matches
    except:
        return []

def run_collection():
    cities = getattr(config, 'cities', [])
    keywords = getattr(config, 'search_req', []) + getattr(config, 'brand', [])
    targets = getattr(config, 'agrigator', [])
    
    parser_key = "https://www.cataloged.ru/"
    parsers_dict = getattr(config, 'parsers', {})
    PARSER_TYPE = parsers_dict.get(parser_key, "Cataloged")
    
    if not cities: return []
    
    driver = init_driver(headless=False)
    all_results = []

    try:
        for city in cities:
            slug = transliterate_city(city)
            try:
                driver.get(f"https://www.cataloged.ru/gorod/{slug}/")
                wait_for_antibot(driver)
            except: continue
            
            shops = parse_shops_list(driver.page_source, targets)
            city_total = 0

            for shop_name, shop_url in shops.items():
                base_url = shop_url.rstrip('/') + "/?filter=produkty"
                driver.get(base_url)
                wait_for_antibot(driver)
                
                max_p = get_max_page(driver.page_source)
                if max_p == 1: max_p = 50 

                session = requests.Session()
                ua = driver.execute_script("return navigator.userAgent;")
                session.headers.update({
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": base_url
                })
                for c in driver.get_cookies():
                    session.cookies.set(c['name'], c['value'])

                tasks = []
                for p in range(1, max_p + 1):
                    u = base_url if p == 1 else f"{base_url}&page{p}"
                    tasks.append(u)

                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [
                        executor.submit(fetch_page_fast, session, u, keywords, city, shop_name, PARSER_TYPE) 
                        for u in tasks
                    ]
                    for f in as_completed(futures):
                        res = f.result()
                        if res:
                            all_results.extend(res)
                            city_total += len(res)
                            print(f"      + {len(res)} шт.")

            update_retail_points("Cataloged", city, city_total)

    except Exception:
        pass
    finally:
        driver.quit()
        
    for idx, item in enumerate(all_results, 1):
        item["Номер"] = idx

    return all_results