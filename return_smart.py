import json
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine, NVARCHAR, DateTime, Integer
import urllib
import json



def get_cookies_from_browser(url):
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Зайдите на сайт и нажмите Enter после авторизации...")
    cookies = driver.get_cookies()
    driver.quit()
    return {cookie['name']: cookie['value'] for cookie in cookies}


def fetch_and_flatten(data_url):
    cookies = get_cookies_from_browser("https://smartup.online")
    print("⬇️ Загружаем данные...")
    response = requests.get(data_url, cookies=cookies)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                data = value
                break
        else:
            raise ValueError("❌ Не найден список в структуре JSON")

    order_df = pd.json_normalize(data, sep="_", max_level=1)

    order_products_list = []
    for order in data:
        order_id = order.get("deal_id")
        for product in order.get("return_products", []):
            product["order_id"] = order_id
            order_products_list.append(product)
    order_products_df = pd.DataFrame(order_products_list)

    details_list = []
    for product in order_products_list:
        product_id = product.get("product_unit_id")
        order_id = product.get("order_id")
        if isinstance(product.get("details"), list):
            for detail in product.get("details", []):
                detail["product_id"] = product_id
                detail["order_id"] = order_id
                details_list.append(detail)
    details_df = pd.DataFrame(details_list)

    print(f"✅ Получено: {len(order_df)} возвратов, {len(order_products_df)} товаров, {len(details_df)} деталей")

    return {
        "anor_return": order_df,
        "anor_returnproducts": order_products_df,
        "anor_details": details_df
    }


def upload_to_sql(df_dict):
    print("🔌 Подключение к SQL Server...")
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=SOFT;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    for table_name, df in df_dict.items():
        if df.empty or len(df.columns) == 0:
            print(f"⏭ Таблица {table_name} пуста — пропущено.")
            continue

        # 🔄 Приводим типы
        for col in df.columns:
            if "id" in col.lower():
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif "date" in col.lower() or "time" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # 📝 Получаем маппинг для информации (но не добавляем в таблицу)
        type_schema = {col: str(df[col].dtype) for col in df.columns}
        print(f"📋 Схема типов для {table_name}: {json.dumps(type_schema, ensure_ascii=False)}")

        # Маппинг типов для SQL
        dtype_map = {col: NVARCHAR(255) for col in df.columns}
        for col in df.columns:
            if "id" in col.lower():
                dtype_map[col] = Integer()
            elif "date" in col.lower() or "time" in col.lower():
                dtype_map[col] = DateTime()

        print(f"📥 Загрузка в таблицу: {table_name} ({len(df)} строк)")
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists="append",
                dtype=dtype_map
            )
            print(f"✅ Таблица {table_name} успешно загружена.")
        except Exception as e:
            print(f"❌ Ошибка при записи в SQL таблицу {table_name}: {e}")


if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mdeal/return$export"
    df_dict = fetch_and_flatten(DATA_URL)
    if df_dict:
        upload_to_sql(df_dict)
