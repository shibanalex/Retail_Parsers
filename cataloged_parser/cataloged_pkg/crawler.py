import time
import requests
import config
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from parsers_core.utils import update_retail_points
from .browser import init_driver, wait_for_antibot
from .html_parser import transliterate_city, parse_shops_list, parse_products_page, get_max_page, get_category_from_html

PARSER_NAME = "Cataloged"

def is_product_match(name, query):
    if not query: return True 
    n_low = name.lower().replace("ё", "е").replace(",", ".").replace("%", "")
    req_words = query.lower().replace("ё", "е").replace(",", ".").replace("%", "").split()
    return all(word in n_low for word in req_words)

def extract_weight_volume(name):
    name = name.lower()
    weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(г|кг)', name)
    if weight_match:
        val, unit = weight_match.groups()
        return f"{val} {unit}"
    
    vol_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(мл|л)', name)
    if vol_match:
        val, unit = vol_match.groups()
        return f"{val} {unit}"
    return None

def fetch_page_all_items(session, url):
    try:
        time.sleep(random.uniform(0.5, 1.5))
        resp = session.get(url, timeout=20)
        if "Loading..." in resp.text or "adblock-blocker" in resp.text or "Я не робот" in resp.text:
            return []
        return parse_products_page(resp.text)
    except:
        return []

def get_category_safe(session, url):
    try:
        resp = session.get(url, timeout=10)
        return get_category_from_html(resp.text)
    except:
        return None

def run_collection():
    cities = getattr(config, 'cities', [])
    search_req = getattr(config, 'search_req', [])
    brands = getattr(config, 'brand', [])
    targets = getattr(config, 'agrigator', [])
    
    queries = []
    if search_req and brands:
        for p in search_req:
            for b in brands: 
                queries.append(f"{p} {b}")
    else:
        queries = list(set(search_req + brands))
    
    if not cities or not queries: return []
    
    parser_key = "https://www.cataloged.ru/"
    parsers_dict = getattr(config, 'parsers', {})
    PARSER_TYPE = parsers_dict.get(parser_key, PARSER_NAME)
    
    driver = init_driver(headless=False)
    all_results = []

    try:
        for city in cities:
            slug = transliterate_city(city)
            city_url = f"https://www.cataloged.ru/gorod/{slug}/"
            
            try:
                driver.get(city_url)
                wait_for_antibot(driver)
            except: continue
            
            shops = parse_shops_list(driver.page_source, targets)
            
            if shops:
                print(f"🏙️ Город: {city} ({city_url})")
                print(f"✅ Магазинов для обработки: {len(shops)}")
            
            city_total_tt = 0

            for shop_name, shop_url in shops.items():
                print(f"  🏪 {shop_name}...")
                
                base_url = shop_url.rstrip('/') + "/?filter=produkty"
                driver.get(base_url)
                wait_for_antibot(driver)
                
                max_p = get_max_page(driver.page_source)
                if max_p == 1: max_p = 100 

                session = requests.Session()
                ua = driver.execute_script("return navigator.userAgent;")
                session.headers.update({
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": base_url,
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
                })
                for c in driver.get_cookies():
                    session.cookies.set(c['name'], c['value'])

                tasks = []
                pages_to_fetch = min(max_p, 400) 
                for p in range(1, pages_to_fetch + 1):
                    u = base_url if p == 1 else f"{base_url}&page{p}"
                    tasks.append(u)

                all_shop_products = []
                
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [executor.submit(fetch_page_all_items, session, u) for u in tasks]
                    for f in as_completed(futures):
                        res = f.result()
                        if res:
                            all_shop_products.extend(res)
                
                if not all_shop_products:
                    for q in queries:
                        print(f"    🔎 [{q}]: 0 шт.")
                    continue

                shop_total_matches = 0

                for q in queries:
                    q_matches = 0
                    for p in all_shop_products:
                        if is_product_match(p['name'], q):
                            cat = get_category_safe(session, p['link']) if p['link'] else None
                            weight_vol = extract_weight_volume(p['name'])
                            
                            item = {
                                "Номер": 0,
                                "Сеть": shop_name,
                                "Тип магазина": PARSER_TYPE,
                                "Адрес Торговой точки": city,
                                "Бренд": None,
                                "Название продукта": p['name'],
                                "Цена": p['price'],
                                "Цена по акции": None,
                                "Фото товара": p['img'],
                                "Ссылка на страницу": p['link'],
                                "Рейтинг": None,
                                "Объем": weight_vol if weight_vol and ('л' in weight_vol) else None,
                                "Вес": weight_vol if weight_vol and ('г' in weight_vol) else None,
                                "Остаток": None,
                                "Категория": cat,
                                "Дата": time.strftime('%Y-%m-%d'),
                                "Время": time.strftime('%H:%M:%S'),
                                "Парсер": PARSER_NAME
                            }
                            all_results.append(item)
                            q_matches += 1
                            shop_total_matches += 1
                            
                    print(f"    🔎 [{q}]: {q_matches} шт.")

                if shop_total_matches > 0:
                    city_total_tt += 1

            update_retail_points(PARSER_NAME, city, city_total_tt)

    except Exception:
        pass
    finally:
        driver.quit()
        
    for idx, item in enumerate(all_results, 1):
        item["Номер"] = idx

    return all_results