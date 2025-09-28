import re
import time
import urllib.parse
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = "https://ishonchsavdo.uz/ru/branches"


# --- Утилиты ---
def clean_work_time(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r'(Открыт[ао]?|Закрыт[ао]?|Ochiq|Yopiq|Open|Closed)', '', raw, flags=re.I)
    raw = re.sub(r'[\n\r\t]+', ' ', raw).strip()
    return re.sub(r'\s{2,}', ' ', raw)


# --- Selenium скрапинг ---
def scrape_with_selenium(url: str):
    print("🔁 Запуск Selenium (headless Chrome)...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    
    # ищем последнюю страницу по кнопкам внизу
    pagination = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li a")  
    if pagination:
        try:
            last_page = max([int(p.text) for p in pagination if p.text.isdigit()])
        except:
            last_page = 1
    else:   
        last_page = 1
    
    print(f"🔎 Найдено страниц: {last_page}")
    results = []

    try:
        for page in range(1, last_page + 1):
            print(f"Страница {page}: парсим данные...")

            soup = BeautifulSoup(driver.page_source, "lxml")
            cards = soup.select("div.flex.flex-1.flex-col")

            found = 0
            for block in cards:
                try:
                    name = block.find("p").get_text(strip=True)
                except:
                    name = ""

                try:
                    location = block.select_one("div.ml-4.flex.flex-col p").get_text(" ", strip=True)
                except:
                    location = ""

                try:
                    work_time = " ".join([p.get_text(strip=True) for p in block.select("div.ml-4.flex.flex-col p") if re.search(r'\d{1,2}[:.]\d{2}', p.get_text())])
                    work_time = clean_work_time(work_time)
                except:
                    work_time = ""

                try:
                    phone = block.find("a", href=re.compile(r"^tel:")).get_text(strip=True)
                except:
                    phone = ""

                try:
                    adress_tag = block.find("a", href=re.compile(r"^https://maps"))
                    adress = adress_tag["href"] if adress_tag else ""
                except:
                    adress = ""

                results.append({
                    "name": name,
                    "location": location,
                    "work_time": work_time,
                    "phone": phone,
                    "adress": adress
                })
                found += 1

            print(f"Страница {page}: найдено {found} записей.")

            # переход на следующую страницу
            if page < last_page:
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, f"ul.pagination li a[aria-label='Page {page+1}']")
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(2)
                except:
                    print("📌 Не смогли кликнуть следующую страницу")
                    break
    finally:
        driver.quit()

    return results


# --- SQL MERGE ---
def upload_to_sql(data):
    print("🔌 Подключение к SQL Server...")
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=SOFT;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


    df = pd.DataFrame(data)

    expected_cols = ["name", "location", "work_time", "phone", "adress"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[expected_cols]

    with engine.begin() as conn:
        # создаём таблицу если нет
        conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Branches' AND xtype='U')
        CREATE TABLE Branches (
    name NVARCHAR(255),
    location NVARCHAR(255),
    work_time NVARCHAR(50),
    phone NVARCHAR(50),
    adress NVARCHAR(MAX)
        )
        """))

        # MERGE для вставки без дублей
        for _, row in df.iterrows():
            conn.execute(text("""
            MERGE Branches AS target
            USING (SELECT :name AS name, :location AS location, :work_time AS work_time, :phone AS phone, :adress AS adress) AS src
            ON (target.name = src.name AND target.location = src.location AND target.phone = src.phone)
            WHEN NOT MATCHED THEN
                INSERT (name, location, work_time, phone, adress)
                VALUES (src.name, src.location, src.work_time, src.phone, src.adress);
            """), {
                "name": row["name"],
                "location": row["location"],
                "work_time": row["work_time"],
                "phone": row["phone"],
                "adress": row["adress"]
            })

    print(f"✅ Данные ({len(df)} строк) синхронизированы в таблицу Branches.")


# --- Main ---
def main():
    data = scrape_with_selenium(BASE_URL)

    if not data:
        print("❗ Данных нет, проверяй сайт.")
        return

    # удаляем дубликаты внутри Python
    unique, seen = [], set()
    for row in data:
        key = (row["name"], row["location"], row["phone"])
        if key not in seen:
            seen.add(key)
            row["phone"] = re.sub(r'[^0-9\+\-\s\(\)]', '', row["phone"]).strip()
            row["work_time"] = clean_work_time(row["work_time"])
            unique.append(row)

    df = pd.DataFrame(unique)
    print(f"Всего уникальных записей: {len(df)}")
    print(df.head(10).to_string(index=False))

    upload_to_sql(df)


if __name__ == "__main__":
    main()
