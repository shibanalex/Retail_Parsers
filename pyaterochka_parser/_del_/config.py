#  comment	💭 Комментарий сценария 
#		выводится в Телеграмм логирование
comment = [
"Тест упавших парсеров через Proxy"
]    

emoji_agent = "🍁"

# Включение отладочного режима !
# куча print'ов  и .debug\parser\ xxxx.json 
#debug_mode = False
debug_mode = True

# Backward compatibility для старых парсеров
#debug_mode = DEBUG_MODE
#DEBUG_Mode = DEBUG_MODE


#  Использовать True / False  или нет Словарь ассоциаций товара по категориям
#  Молок =  Молочные продукты
#  Молоко сгущеное = Консервы 
#  Птичье молоко = контитерка
use_assoc = True
assoc_debug = False				# default False	 Основные + поля категорий (для отладки ассоциаций)
assoc_auto_train = True				#  Выключатель Автоматического обучения словаря

auto_enrich = True  				# Включить обогащение 		default True
auto_enrich_existing = False			# Обогащать существующие MDR_  	default False
remove_mdr_after_enrich = False			# Удалять MDR_ после обогащения default False


#  frequency	🧠 ЧТО ОЗНАЧАЮТ ЗНАЧЕНИЯ 
#   Значение	Поведение Scheduler
#	-1	❌ Отключён — сценарий НИКОГДА не запускается
#	0	🔁 Каждый проход Scheduler (несколько раз в день)
#	1	🕒 Раз в день
#	7	📅 Раз в 7 дней (неделя)
#	30	🗓 Раз в месяц (условно)
frequency = 0

#  block_time  	⏰ Время блокировки: 23:00–06:30 (через полночь — поддерживается)
#  		📅 Дни: только будни
#		🧠 reason: человеко-читаемая причина (идёт в лог и Telegram)
#		Если days не указаны → блок действует каждый день.
#		Если block_time отсутствует → RPC всегда разрешён.
block_time: {
  "from": "03:00",
  "to":   "06:30",
  "days": ["mon", "tue", "wed", "thu", "fri"],
  "reason": "Антибот / ночные баны Ленты",
}


#                f"📦 Записей: {len(parser_data)}\n"
#                f"🏙️ Города: {len(cities)} | 🏪 Торг Точек: {tt_count} \n"
#                f"🏷️ Бренды: {len(brands)} | 🛒 Товары: {len(products)}\n"
#                f"🔎 Запросы: {len(queries)}\n"

#  cities 	🏙️ Список городов 
cities = [
    "Иваново",
#    "Фурманов",
#    "Родники",
#    "Шуя",
#    "Кострома",
#    "Самара",
#    "Тольятти",
#    "Владимир",
#    "Ярославль",
]

search_req = [
    # "молоко 2.5",
     "масло 82",
    "Кефир",
    # "Ряженка",
    # "Варенец"
     "Сметана",
    # "Молоко",
    # "Йогурт",
    # "Сырок",
    # "Вино сух",
    # "Виски",
 ]

brand = [
#    "Тольяттимолоко",
#    "Молоковье",
#    "Пестравка",
#    "Домашкино",
#     "Домик в деревне",
#    "Купино",
#    "Ласкава",
#    "Румелко",
 ]


proxy = []							#   соединения через Proxy  Отключены
#proxy = "./proxys.db"						#   Локальная БД с адресами Proxy серверов
#proxy = "P:/_SrvShare_/proxys_mob.db"				#    Сетевая  БД с адресами Proxy серверов
try_fall = 3							# Количество немедленных попыток для каждого парсера (0, 1, 2, 3...)

retry_wave = True  						# Включить повторную волну для упавших парсеров ! Упавший меняет proxy и пытается парсить снова. итак count_retry_wave  раз
retry_wave_delay = 30  						# Пауза между волнами (попытками) в секундах  default = 30
count_retry_wave = 3						# Кол-во повторений для механизма "Вторая волна" - добавляет упавшие в конец списка и повторяет их retry_wave раз def =2



TelegramBot = {
    "TOKEN": "7464704844:AAErV72XLVnlvkQX1qRuITFkfMN7VYSaXww",
    "CHAT_ID": "786185421",
    "send": True,
    "MY_ID": "Lab_OUT",
}

parsers = {
#    "https://auchan.ru/": "Ашан",
#    "https://okeydostavka.ru/": "OKey",
#    "https://lenta.com/": "Лента",
#    "https://metro-cc.ru/": "Метро",
#    "https://myspar.ru/": "СПАР",
#    "https://perekrestok.ru/": "Перекресток",
#    "https://online.globus.ru": "Глобус",
#    "https://bristol.ru/": "Бристоль",
#    "https://magnit.ru/": "Магнит",
    "https://5ka.ru/": "Пятерочка",
#    "https://chizhik.club/": "Чижик",
#    "https://krasnoeibeloe.ru/": "К&Б",
#    "https://dixy.ru/": "Дикси",
#    "https://vliga.com": "Высшая Лига",
#    "https://riat-market.ru/": "Риат",
#    "https://smart.swnn.ru/": "Smart",
#    "https://maxi-retail.ru/": "Maxi",   
}

# bristol settings
refresh_bristol_shops = False
bristol_search_req = [
#     "молоко 2.5",
     "масло 82",
         "Вино белое сух",
#	 "Грюнер",
#         "Cremant",
#         "Prosecco",
#    "Простоквашино",
]

#db_retail_map = "./output/DB_Retail_Points_MAP.xlsx"

db_retail_map = "retail_points_stats.xlsx"
sqlite_file = "./output/TEMP_SQLite_Falling_SHIT_del.db"
table_name = "./output/Test_FallingTEST_SHIT_del"

SQL = {
#    "port": 5432,   	-  Будет Работать запись в Postgree
#    "port": 4534,	-  НЕ Будет писать в Postgree
    "host": "192.168.0.113",
#    "port": 5432,
     "port": 4534,
    "user": "postgres",
    "password": "Qq123456789",
    "TABLE_Draft": "retail_draft",
    "DB_Draft": "retail-test",
    "DB_Work": "retail_work",	
}
