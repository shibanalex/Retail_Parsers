import time
import traceback
import config
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from parsers_core.utils import update_retail_points
from .browser import init_driver, wait_for_humanity, save_debug_html
from .html_parser import transliterate_city, parse_stores, parse_search_results, parse_product_details

BASE_URL = "https://www.magazinnoff.ru"

def run_collection():
    cities = getattr(config, 'cities', [])
    products = getattr(config, 'search_req', [])
    brands = getattr(config, 'brand', [])
    targets = getattr(config, 'agrigator', [])

    parser_key = "https://www.magazinnoff.ru/"
    parsers_dict = getattr(config, 'parsers', {})
    PARSER_NAME = parsers_dict.get(parser_key, "Magazinnoff")

    queries = []
    if products and brands:
        for p in products:
            for b in brands: queries.append(f"{p} {b}")
    else:
        queries = list(set(products + brands))

    if not cities or not queries:
        print("⚠️ Нет городов или запросов в config.py")
        return []

    driver = init_driver(headless=False)
    all_results = []

    try:
        print("🌐 Браузер запущен. Переход на главную...")
        driver.get(BASE_URL)
        wait_for_humanity(driver)

        for city in cities:
            slug = transliterate_city(city)
            city_url = f"{BASE_URL}/category/produkty/city/{slug}"
            
            print(f"🏙️ Город: {city}")
            driver.get(city_url)
            
            if not wait_for_humanity(driver):
                continue

            stores_map = parse_stores(driver.page_source, targets)
            
            if not stores_map:
                print(f"⚠️ Магазины не найдены в {city}")
                continue
            
            city_total_items = 0

            for s_slug, s_name in stores_map.items():
                print(f"  🏪 {s_name}...")

                for q in queries:
                    try:
                        shop_url = f"{BASE_URL}/magazin/{s_slug}/c/{slug}"
                        driver.get(shop_url)
                        time.sleep(1.5)

                        js_search = f"""
                        var f=document.createElement('form');
                        f.method='POST';
                        f.action='/magazin/{s_slug}/search';
                        var i=document.createElement('input');
                        i.type='hidden';i.name='search_name';i.value='{q}';
                        f.appendChild(i);
                        document.body.appendChild(f);
                        f.submit();
                        """
                        driver.execute_script(js_search)

                        try:
                            WebDriverWait(driver, 6).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "strip"))
                            )
                        except:
                            pass

                        wait_for_humanity(driver, timeout=10)

                        items_list = parse_search_results(driver.page_source, s_name)
                        if items_list:
                            print(f"    🔎 [{q}]: {len(items_list)} шт.")

                        for item in items_list:
                            brand, weight, volume, exact_price, category = None, None, None, None, None
                            
                            if item.get('link'):
                                try:
                                    driver.get(item['link'])
                                    wait_for_humanity(driver, timeout=5)
                                    brand, weight, volume, exact_price, category = parse_product_details(
                                        driver.page_source, item['name']
                                    )
                                except Exception:
                                    pass

                            final_price = exact_price if exact_price else item['price']

                            record = {
                                "Номер": 0, 
                                "Сеть": s_name,
                                "Тип магазина": PARSER_NAME,
                                "Адрес Торговой точки": city,
                                "Бренд": brand,
                                "Название продукта": item['name'],
                                "Цена": final_price,
                                "Цена по акции": None,
                                "Фото товара": item['img'],
                                "Ссылка на страницу": item['link'],
                                "Рейтинг": None,
                                "Объем": volume,
                                "Вес": weight,
                                "Остаток": None,
                                "Категория": category,
                                "Дата": time.strftime('%Y-%m-%d'),
                                "Время": time.strftime('%H:%M:%S'),
                                "Парсер": PARSER_NAME
                            }
                            all_results.append(record)
                            city_total_items += 1

                    except Exception:
                        continue
            
            update_retail_points("Magazinnoff", city, city_total_items)

    except Exception as e:
        print(f"❌ Ошибка коллектора Magazinnoff: {e}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            
    for idx, item in enumerate(all_results, 1):
        item["Номер"] = idx
    
    return all_results