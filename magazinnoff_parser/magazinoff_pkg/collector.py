import time
import traceback
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import config
from utils import update_retail_points

from .browser import init_driver, wait_for_humanity, save_debug_html
from .html_parser import transliterate_city, parse_stores, parse_search_results, parse_product_details

BASE_URL = "https://www.magazinnoff.ru"
PARSER_NAME = "Magazinnoff"


def run_collection():
    cities = getattr(config, 'cities', [])
    products = getattr(config, 'search_req', [])
    brands = getattr(config, 'brand', [])
    targets = getattr(config, 'agrigator', [])

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

            print(f"🏙️ Город: {city} ({city_url})")
            driver.get(city_url)

            if not wait_for_humanity(driver):
                print(f"❌ Не удалось загрузить страницу города {city}")
                continue

            stores_map = parse_stores(driver.page_source, targets)

            if not stores_map:
                print(f"⚠️ Магазины не найдены в {city}. См. debug_no_stores.html")
                save_debug_html(driver, f"no_stores_{slug}")
                continue

            print(f"✅ Магазинов для обработки: {len(stores_map)}")
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
                                except Exception as e:
                                    print(f"    ⚠️ Ошибка деталей товара: {e}")


                            final_price = exact_price if exact_price else item['price']

                            record = {
                                "Дата": time.strftime('%Y-%m-%d'),
                                "Время": time.strftime('%H:%M:%S'),
                                "Парсер": PARSER_NAME,
                                "Город": city,
                                "Сеть": s_name,
                                "Категория": category,
                                "Бренд": brand,
                                "Название продукта": item['name'],
                                "Цена": final_price,
                                "Цена по акции": None,
                                "Вес": weight,
                                "Объем": volume,
                                "Ссылка": item['link'],
                                "Фото": item['img']
                            }
                            all_results.append(record)
                            city_total_items += 1

                    except Exception as e:
                        print(f"    ❌ Ошибка обработки запроса '{q}': {e}")
                        continue

            update_retail_points(PARSER_NAME, city, city_total_items)

    except Exception as e:
        print(f"❌ Глобальная ошибка коллектора: {e}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

    return all_results