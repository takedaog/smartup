import warnings
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import Float, Integer, String, DateTime, Boolean, NVARCHAR
import math
import urllib

pd.set_option('future.no_silent_downcasting', True)
warnings.filterwarnings(
    "ignore",
    message="Could not infer format",
    category=UserWarning
)

def get_cookies_from_browser(url: str) -> dict:
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    input("🌐 Авторизуйтесь в браузере и нажмите Enter...")
    cookies = {c['name']: c['value'] for c in driver.get_cookies()}
    driver.quit()
    return cookies

def auto_cast_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        # сначала пробуем числа без errors="ignore"
        try:
            df[col] = pd.to_numeric(df[col], downcast="integer")
        except (ValueError, TypeError):
            pass
        # потом даты
        if pd.api.types.is_object_dtype(df[col]):
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                if parsed.notna().mean() >= 0.6:
                    df[col] = parsed
            except Exception:
                pass
    return df



def fetch_inventory(data_url: str, cookies: dict) -> dict:
    print("⬇️ Загрузка inventory...")
    r = requests.post(data_url, cookies=cookies, json={}, headers={"Content-Type": "application/json"})
    r.raise_for_status()
    data = r.json()
    items = data.get("inventory", [])
    print("📡 Всего элементов в inventory (raw):", len(items))
    if not items:
        return {}
    inv_df = pd.json_normalize(items, sep="_", max_level=1)
    print("🔎 После json_normalize:", inv_df.shape)

    groups_list, kinds_list, sectors_list = [], [], []
    for it in items:
        pid = it.get("product_id")
        for g in it.get("groups", []):
            g["product_id"] = pid
            groups_list.append(g)
        for k in it.get("inventory_kinds", []):
            k["product_id"] = pid
            kinds_list.append(k)
        for s in it.get("sector_codes", []):
            s["product_id"] = pid
            sectors_list.append(s)

    return {
        "inventory_main":   inv_df,
        "inventory_groups": pd.DataFrame(groups_list),
        "inventory_kinds":  pd.DataFrame(kinds_list),
        "inventory_sectors":pd.DataFrame(sectors_list)
    }

UNIQUE_KEYS = {
    "inventory_main":   ["product_id"],
    "inventory_groups": ["product_id", "group_id"],
    "inventory_kinds":  ["product_id", "kind_id"],
    "inventory_sectors":["product_id", "sector_code"],
}

def build_dtype_map(df: pd.DataFrame, table_name: str) -> dict:
    """
    • bool → Boolean
    • int  → Integer
    • float → Float
    • datetime → DateTime
    • object/строки → String(n) (с расчётом длины)
    • inventory_groups.group_code / type_code → NVARCHAR
    """
    dtype_map: dict[str, object] = {}

    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            # Пустая колонка — хотя бы String(50)
            dtype_map[col] = String(50)
            continue

        # Спец-правило
        if table_name == "inventory_groups" and col in ("group_code", "type_code"):
            dtype_map[col] = NVARCHAR()
            continue

        # pandas dtype
        dt = df[col].dtype

        if pd.api.types.is_bool_dtype(dt):
            dtype_map[col] = Boolean()
        elif pd.api.types.is_integer_dtype(dt):
            # Int64/Int32 → Integer
            dtype_map[col] = Integer()
        elif pd.api.types.is_float_dtype(dt):
            dtype_map[col] = Float()
        elif pd.api.types.is_datetime64_any_dtype(dt):
            dtype_map[col] = DateTime()
        else:
            # текст: вычисляем максимальную длину, небольшой запас
            max_len = s.astype(str).str.len().max()
            size = max(50, math.ceil(max_len * 1.2))
            dtype_map[col] = String(size)

    return dtype_map

def upload_to_sql(df_dict: dict):
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=Epco;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    inspector = inspect(engine)

    with engine.begin() as conn:
        for table_name, df in df_dict.items():
            if df is None or df.empty:
                print(f"⏭ {table_name} пуст – пропущено.")
                continue

            # приведение типов
            df = auto_cast_dataframe(df)

            keys = [k for k in UNIQUE_KEYS.get(table_name, []) if k in df.columns]

            # 🔑 сначала строим карту типов – пока ключи ещё в «родном» dtype
            dtype_map = build_dtype_map(df, table_name)

            if keys:
                # чистим пробелы только для текстовых ключей
                for k in keys:
                    if pd.api.types.is_string_dtype(df[k]):
                        df[k] = df[k].str.strip()
                df = df.drop_duplicates(subset=keys)

            # --- загрузка ---
            if not inspector.has_table(table_name):
                df.to_sql(
                    table_name,
                    con=conn,
                    index=False,
                    if_exists="replace",
                    dtype=dtype_map
                )
                print(f"🆕 {table_name} создана и загружено {len(df)} строк.")
                continue

            if keys:
                stg = f"{table_name}_stg"
                df.to_sql(stg, con=conn, index=False,
                          if_exists="replace", dtype=dtype_map)
                on_clause = " AND ".join([f"t.[{k}] = s.[{k}]" for k in keys])
                cols = [f"[{c}]" for c in df.columns]
                insert_cols = ", ".join(cols)
                insert_vals = ", ".join([f"s.[{c}]" for c in df.columns])
                merge_sql = f"""
MERGE INTO dbo.{table_name} AS t
USING dbo.{stg} AS s
ON {on_clause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols}) VALUES ({insert_vals});
DROP TABLE dbo.{stg};
"""
                conn.execute(text(merge_sql))
                print(f"📥 {table_name} → добавлены только новые строки ({len(df)} проверено).")
            else:
                df.to_sql(
                    table_name,
                    con=conn,
                    index=False,
                    if_exists="append",
                    dtype=dtype_map
                )
                print(f"⚠️ {table_name}: нет ключей, добавлены все {len(df)} строк.")

if __name__ == "__main__":
    DATA_URL = "https://smartup.online/b/anor/mxsx/mr/inventory$export"
    cookies = get_cookies_from_browser("https://smartup.online")
    result = fetch_inventory(DATA_URL, cookies)
    if result:
        upload_to_sql(result)
