import re
from bs4 import BeautifulSoup

BASE_URL = "https://www.cataloged.ru"

def transliterate_city(city_name):
    symbols = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya', 
        ' ': '-', '-': '-'
    }
    return ''.join(symbols.get(c, c) for c in city_name.lower()).strip('-')

def clean_shop_name(raw_name):
    name = raw_name.split(" в ")[0].strip()
    
    name = re.sub(r'Акции\s+', '', name, flags=re.IGNORECASE)
    
    popular_nets = ["ОКЕЙ", "Ашан", "Адмирал", "Высшая Лига", "Десяточка", "Магнит", "Пятерочка", "Лента"]
    for net in popular_nets:
        if net.lower() in name.lower():
            return net
            
    return name.strip()

def parse_shops_list(html, target_names):
    soup = BeautifulSoup(html, "lxml")
    shops = {}
    for item in soup.find_all("div", class_="promo__item"):
        link_tag = item.find("a", class_="promo__item--title")
        if not link_tag: continue
        
        raw_name = link_tag.get_text(strip=True)
        clean_name = clean_shop_name(raw_name)
        href = link_tag.get("href")
        
        if target_names and not any(t.lower() in clean_name.lower() for t in target_names):
            continue
            
        if href and not href.startswith("http"):
            href = BASE_URL + href
        shops[clean_name] = href
    return shops

def get_max_page(html):
    soup = BeautifulSoup(html, "lxml")
    max_p = 1
    links = soup.find_all("a", class_="page-numbers")
    for l in links:
        t = l.get_text(strip=True)
        clean_t = re.sub(r'[^\d]', '', t)
        if clean_t.isdigit():
            max_p = max(max_p, int(clean_t))
    return max_p

def parse_products_page(html):
    soup = BeautifulSoup(html, "lxml")
    items = []
    
    product_cards = soup.find_all("div", class_="rec__item")
    if not product_cards:
        product_cards = soup.find_all("div", class_="item")

    for card in product_cards:
        try:
            name_div = card.find("div", class_="rec__item--text")
            if not name_div: continue
            name = name_div.get_text(strip=True)
            
            link_tag = card.find("a", href=True)
            link = link_tag['href'] if link_tag else None
            if link and not link.startswith("http"): link = BASE_URL + link
            
            price = None
            price_div = card.find("p", class_="rec__item--price")
            if price_div:
                raw = price_div.get_text()
                clean = re.sub(r"\s+", "", raw)
                clean = re.sub(r"[^\d,.]", "", clean).replace(",", ".")
                if clean:
                    try: price = float(clean)
                    except: pass
            
            img_tag = card.find("img", class_="rec__item--img")
            img = img_tag.get("src") if img_tag else None
            if img and not img.startswith("http"): img = BASE_URL + img

            items.append({"name": name, "price": price, "link": link, "img": img})
        except: continue
            
    return items

def get_category_from_html(html):
    soup = BeautifulSoup(html, "lxml")
    breadcrumbs = soup.find("div", class_="breadcrumbs")
    if breadcrumbs:
        spans = breadcrumbs.find_all("span", attrs={"itemprop": "name"})
        if len(spans) >= 2:
            return spans[-2].get_text(strip=True)
    return None