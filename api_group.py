# -*- coding: utf-8 -*-
import hashlib
import json
import platform
import sys
from datetime import datetime, timedelta
import pyodbc
import requests

print(sys.getdefaultencoding())  # utf-8 bo'lishi kerak

# ====== KONFIG ======
URL = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATE_FORMAT = "%d.%m.%Y"

SQL_SERVER = "localhost" 
SQL_DATABASE = "SmartUpDB"
SQL_TRUSTED = "Yes"  # Windows auth

BEGIN_DATE_STR = "15.02.2025"
END_DATE_STR = "15.04.2025"
FILIAL_WAREHOUSE_JSON = "filial_warehouse.json"

# --- Yangi sxema: 2 jadval ---
FACT_TABLE = "dbo.FactBalance"
GROUP_TABLE = "dbo.BalanceGroup"

# Collation (kiril/lotin uchun)
COLLATION = "Cyrillic_General_CI_AS"


# ====== UTIL ======
def daterange(start_date: datetime, end_date: datetime):
    """Boshlang'ich va tugash sanalari orasida oyma-oy interval qaytaradi."""
    # boshlanish sanasini oyning 1-kuniga olib kelamiz
    current = datetime(start_date.year, start_date.month, 1).date()
    while current <= end_date.date():
        # keyingi oyning birinchi kuni
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        # shu oyning oxirgi kuni yoki end_date
        last_day = min(next_month - timedelta(days=1), end_date.date())
        yield current, last_day
        current = next_month



def _pick_driver():
    drivers = [d.strip() for d in pyodbc.drivers()]
    for name in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]:
        if name in drivers:
            return name, drivers
    return None, drivers


def connect_sql():
    driver, all_drivers = _pick_driver()
    if not driver:
        arch = platform.architecture()[0]
        raise RuntimeError(
            f"ODBC drayver topilmadi. Python: {arch}. O'rnatilganlar: {all_drivers}\n"
            "Iltimos, 'ODBC Driver 17/18 for SQL Server' ni o'rnating."
        )
    print(f"➡️  Using ODBC driver: {{{driver}}}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection={SQL_TRUSTED};"
        "Encrypt=No;"
        "TrustServerCertificate=Yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    # Unicode
    conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-16le")
    conn.setencoding(encoding="utf-16le")
    return conn


def to_date(val):
    """Matn/iso datetime -> date (yaroqsiz bo'lsa None)."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s[:19]).date()
    except Exception:
        return None


def to_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_balance_id(warehouse_id, product_id, batch_number, balance_date) -> str:
    # balance_date str (yyyy-mm-dd) yoki date object bo‘lishi mumkin
    if isinstance(balance_date, datetime):
        date_str = balance_date.date().isoformat()
    elif hasattr(balance_date, "isoformat"):
        date_str = balance_date.isoformat()
    else:
        date_str = str(balance_date or "")
    parts = [
        str(warehouse_id or ""),
        str(product_id or ""),
        str(batch_number or ""),
        date_str or ""
    ]
    return sha256("|".join(parts))


# ====== DB Objects ======
def ensure_tables(cursor):
    """2 ta jadvalni yaratadi (agar bo'lmasa). PK/FK/indekslarni ham borligini tekshiradi."""
    # FactBalance
    cursor.execute(f"""
IF OBJECT_ID('{FACT_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {FACT_TABLE} (
<<<<<<< HEAD
    balance_id       CHAR(64)      NOT NULL PRIMARY KEY,
    inventory_kind   VARCHAR(5)    NULL,
    balance_date     DATE          NULL,
    warehouse_id     INT           NULL,
    warehouse_code   NVARCHAR(200) COLLATE {COLLATION} NULL,
    product_code     NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_barcode  NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_id       NVARCHAR(50)  COLLATE {COLLATION} NULL,
    card_code        NVARCHAR(100) COLLATE {COLLATION} NULL,
    expiry_date      DATE          NULL,
    serial_number    NVARCHAR(100) COLLATE {COLLATION} NULL,
    batch_number     NVARCHAR(100) COLLATE {COLLATION} NULL,
    quantity         DECIMAL(18,4) NULL,
    measure_code     NVARCHAR(50)  COLLATE {COLLATION} NULL,
    input_price      DECIMAL(18,4) NULL,
    filial_id        INT           NULL,
    filial_code      NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_conditions NVARCHAR(10) COLLATE {COLLATION} NULL
);

=======
        balance_id      CHAR(64)     NOT NULL PRIMARY KEY,
        inventory_kind  VARCHAR(5)   NULL,
        balance_date    DATE         NULL,
        warehouse_id    INT          NULL,
        warehouse_code  NVARCHAR(200) COLLATE {COLLATION} NULL,
        product_code    NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_barcode NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_id      NVARCHAR(50)  COLLATE {COLLATION} NULL,
        card_code       NVARCHAR(100) COLLATE {COLLATION} NULL,
        expiry_date     DATE          NULL,
        serial_number   NVARCHAR(100) COLLATE {COLLATION} NULL,
        batch_number    NVARCHAR(100) COLLATE {COLLATION} NULL,
        quantity        DECIMAL(18,4) NULL,
        measure_code    NVARCHAR(50)  COLLATE {COLLATION} NULL,
        input_price     DECIMAL(18,4) NULL,
        filial_id       INT           NULL,
        filial_code     NVARCHAR(100) COLLATE {COLLATION} NULL
    );
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
END
""")

    # Indexes (helpful for queries)
    cursor.execute(f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'IX_FactBalance_Product' AND object_id = OBJECT_ID('{FACT_TABLE}')
)
    CREATE INDEX IX_FactBalance_Product
      ON {FACT_TABLE}(product_id, warehouse_id, batch_number, balance_date);
""")

    # BalanceGroup
    cursor.execute(f"""
IF OBJECT_ID('{GROUP_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {GROUP_TABLE} (
        balance_id  CHAR(64)      NOT NULL,
        group_code  NVARCHAR(100) COLLATE {COLLATION} NOT NULL,
        type_code   NVARCHAR(200) COLLATE {COLLATION} NULL,
        CONSTRAINT PK_BalanceGroup PRIMARY KEY (balance_id, group_code),
        CONSTRAINT FK_BalanceGroup_FactBalance
            FOREIGN KEY (balance_id) REFERENCES {FACT_TABLE}(balance_id)
    );
END
""")

    cursor.execute(f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'IX_BalanceGroup_Code' AND object_id = OBJECT_ID('{GROUP_TABLE}')
)
    CREATE INDEX IX_BalanceGroup_Code ON {GROUP_TABLE}(group_code);
""")
CONDITIONS_JSON = "conditions.json"

def fetch_balance_chunks(filial_warehouse_list, begin_date: datetime, end_date: datetime, allowed_conditions: set):
    session = requests.Session()
    fact_rows, group_rows = [], []
    seen_balance_ids, seen_group_pairs = set(), set()
<<<<<<< HEAD
    
=======

>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
    seen_balance_ids = set()  # Fact darajasida dublikat bo‘lmasin
    seen_group_pairs = set()  # (balance_id, group_code) darajasi

    for entry in filial_warehouse_list:
        filial_id = entry["filial_id"]
        filial_code = entry["filial_code"]
        warehouse_id = entry["warehouse_id"]
        warehouse_code = entry["warehouse_code"]
<<<<<<< HEAD
        

        for start, finish in daterange(begin_date, end_date):
=======

        for start, finish in daterange(begin_date, end_date, step_days=30):
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
            params = {"filial_id": filial_id, } 
            payload = {
                "warehouse_codes": [{"warehouse_code": warehouse_code}],
                "filial_code": filial_code,
                "begin_date": start.strftime(DATE_FORMAT),
                "end_date": finish.strftime(DATE_FORMAT)
            }

            try:
                resp = session.post(
                    URL,
                    params=params,
                    auth=(USERNAME, PASSWORD),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    timeout=90,
                )
                resp.encoding = "utf-8"
                resp.raise_for_status()
                data = resp.json()
                balance = data.get("balance", [])

                added_f, added_g = 0, 0
                for item in balance:
                    # Enrichment
                    inv_kind = item.get("inventory_kind")
                    bal_date = to_date(item.get("date"))
                    prod_code = item.get("product_code")
                    prod_barcode = item.get("product_barcode")
                    prod_id = item.get("product_id")
                    card_code = item.get("card_code")
                    expiry_date = to_date(item.get("expiry_date"))
                    serial_num = item.get("serial_number")
                    batch_num = item.get("batch_number")
                    qty = to_float(item.get("quantity"))
                    measure_code = item.get("measure_code")
                    input_price = to_float(item.get("input_price"))
<<<<<<< HEAD
                    product_conditions = inv_kind 
                    inv_kind = (item.get("inventory_kind") or "").strip().upper()  
=======
                    if inv_kind not in allowed_conditions:
                        continue
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
                
                    # Deterministik ID
                    balance_id = make_balance_id(warehouse_id, prod_id, batch_num, bal_date)

                    # Fact — dublikatni tekshirish
                    if balance_id not in seen_balance_ids:
                        fact_rows.append((
                            balance_id, inv_kind, bal_date, int(warehouse_id) if warehouse_id else None,
                            warehouse_code, prod_code, prod_barcode, prod_id, card_code, expiry_date,
                            serial_num, batch_num, qty, measure_code, input_price,
<<<<<<< HEAD
                            int(filial_id) if filial_id else None, filial_code,
                            product_conditions
=======
                            int(filial_id) if filial_id else None, filial_code
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
                        ))
                        seen_balance_ids.add(balance_id)
                        added_f += 1

                    # Groups — bo‘sh bo‘lsa ham 1 qator None bilan kiritamiz (ixtiyoriy)
                    groups = item.get("groups") or [{"group_code": None, "type_code": None}]
                    for g in groups:
                        gc = g.get("group_code")
                        tc = g.get("type_code")
                        key = (balance_id, gc)
                        if key not in seen_group_pairs:
                            group_rows.append((balance_id, gc, tc))
                            seen_group_pairs.add(key)
                            added_g += 1

                print(f"📅 Oyma-oy: {start.strftime(DATE_FORMAT)} → {finish.strftime(DATE_FORMAT)} | "
      f"filial={filial_code}, warehouse={warehouse_code} | "
      f"{len(balance)} items → +Fact:{added_f}, +Group:{added_g}")


            except Exception as e:
                print(f"⚠️ API xatosi | filial={filial_code} | warehouse={warehouse_code} | "
                      f"{start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | {e}")

    return fact_rows, group_rows


# ====== MAIN ======
def main():
    # 1) JSON ni UTF-8 da o‘qiymiz
    with open(FILIAL_WAREHOUSE_JSON, "r", encoding="utf-8") as f:
        filial_warehouse_list = json.load(f)

    with open(CONDITIONS_JSON, "r", encoding="utf-8") as f:
        cond_data = json.load(f)
    # превращаем [{"product_conditions":"T"}, …] → {"T","B","F"}
    allowed_conditions = {c["product_conditions"] for c in cond_data}
    print(f"✅ Фильтр по inventory_kind: {allowed_conditions}")

    begin_date = datetime.strptime(BEGIN_DATE_STR, DATE_FORMAT)
    end_date = datetime.strptime(END_DATE_STR, DATE_FORMAT)

    # 2) SQL ga ulanib, jadvallarni tekshiramiz
    conn = connect_sql()
    cursor = conn.cursor()
    print("✅ SQL Serverga ulandik")

    ensure_tables(cursor)
    conn.commit()

    # 3) API dan ma’lumotlarni yig‘amiz (fact/group alohida)
    fact_rows, group_rows = fetch_balance_chunks(filial_warehouse_list, begin_date, end_date, allowed_conditions)
    if not fact_rows:
        print("ℹ️ Fact bo‘yicha yangi yozuvlar topilmadi.")
    if not group_rows:
        print("ℹ️ Group bo‘yicha yangi yozuvlar topilmadi.")
    if not fact_rows and not group_rows:
        cursor.close();
        conn.close()
        return

    # 4) Temp jadvallar (Unicode/collation bilan)
    cursor.execute(f"""
IF OBJECT_ID('tempdb..#TmpFact') IS NOT NULL DROP TABLE #TmpFact;
CREATE TABLE #TmpFact (
    balance_id      CHAR(64)     NOT NULL,
    inventory_kind  VARCHAR(5)   NULL,
    balance_date    DATE         NULL,
    warehouse_id    INT          NULL,
    warehouse_code  NVARCHAR(200) COLLATE {COLLATION} NULL,
    product_code    NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_barcode NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_id      NVARCHAR(50)  COLLATE {COLLATION} NULL,
    card_code       NVARCHAR(100) COLLATE {COLLATION} NULL,
    expiry_date     DATE          NULL,
    serial_number   NVARCHAR(100) COLLATE {COLLATION} NULL,
    batch_number    NVARCHAR(100) COLLATE {COLLATION} NULL,
    quantity        DECIMAL(18,4) NULL,
    measure_code    NVARCHAR(50)  COLLATE {COLLATION} NULL,
    input_price     DECIMAL(18,4) NULL,
    filial_id       INT           NULL,
<<<<<<< HEAD
    filial_code     NVARCHAR(100) COLLATE {COLLATION} NULL,
    product_conditions NVARCHAR(10) COLLATE {COLLATION} NULL
=======
    filial_code     NVARCHAR(100) COLLATE {COLLATION} NULL
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
);
IF OBJECT_ID('tempdb..#TmpGroup') IS NOT NULL DROP TABLE #TmpGroup;
CREATE TABLE #TmpGroup (
    balance_id  CHAR(64)      NOT NULL,
    group_code  NVARCHAR(100) COLLATE {COLLATION} NULL,
    type_code   NVARCHAR(200) COLLATE {COLLATION} NULL
);
""")

    # 5) Bulk insert (Unicode safe) — muammo bo'lsa fallback
    try:
        cursor.fast_executemany = True
        if fact_rows:
            cursor.executemany("""
<<<<<<< HEAD
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
=======
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
            """, fact_rows)
        if group_rows:
            cursor.executemany("""
                INSERT INTO #TmpGroup VALUES (?,?,?)
            """, group_rows)
    except pyodbc.Error as e:
        print(f"⚠️ fast_executemany muammo: {e}. Fallback bilan davom etamiz.")
        cursor.fast_executemany = False
        if fact_rows:
            cursor.executemany("""
<<<<<<< HEAD
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
=======
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
            """, fact_rows)
        if group_rows:
            cursor.executemany("""
                INSERT INTO #TmpGroup VALUES (?,?,?)
            """, group_rows)

    # 6) MERGE: Fact upsert (PRIMARY KEY = balance_id)
    cursor.execute(f"""
MERGE {FACT_TABLE} AS T
USING (
    SELECT DISTINCT *
    FROM #TmpFact
) AS S
ON (T.balance_id = S.balance_id)
WHEN MATCHED THEN UPDATE SET
<<<<<<< HEAD
    inventory_kind    = S.inventory_kind,
    balance_date      = S.balance_date,
    warehouse_id      = S.warehouse_id,
    warehouse_code    = S.warehouse_code,
    product_code      = S.product_code,
    product_barcode   = S.product_barcode,
    product_id        = S.product_id,
    card_code         = S.card_code,
    expiry_date       = S.expiry_date,
    serial_number     = S.serial_number,
    batch_number      = S.batch_number,
    quantity          = S.quantity,
    measure_code      = S.measure_code,
    input_price       = S.input_price,
    filial_id         = S.filial_id,
    filial_code       = S.filial_code,
    product_conditions = S.product_conditions
WHEN NOT MATCHED THEN
    INSERT (balance_id, inventory_kind, balance_date, warehouse_id, warehouse_code,
        product_code, product_barcode, product_id, card_code, expiry_date,
        serial_number, batch_number, quantity, measure_code, input_price,
        filial_id, filial_code, product_conditions)
    VALUES (S.balance_id, S.inventory_kind, S.balance_date, S.warehouse_id, S.warehouse_code,
        S.product_code, S.product_barcode, S.product_id, S.card_code, S.expiry_date,
        S.serial_number, S.batch_number, S.quantity, S.measure_code, S.input_price,
        S.filial_id, S.filial_code, S.product_conditions);
=======
    inventory_kind  = S.inventory_kind,
    balance_date    = S.balance_date,
    warehouse_id    = S.warehouse_id,
    warehouse_code  = S.warehouse_code,
    product_code    = S.product_code,
    product_barcode = S.product_barcode,
    product_id      = S.product_id,
    card_code       = S.card_code,
    expiry_date     = S.expiry_date,
    serial_number   = S.serial_number,
    batch_number    = S.batch_number,
    quantity        = S.quantity,
    measure_code    = S.measure_code,
    input_price     = S.input_price,
    filial_id       = S.filial_id,
    filial_code     = S.filial_code
WHEN NOT MATCHED THEN
    INSERT (balance_id, inventory_kind, balance_date, warehouse_id, warehouse_code,
            product_code, product_barcode, product_id, card_code, expiry_date,
            serial_number, batch_number, quantity, measure_code, input_price,
            filial_id, filial_code)
    VALUES (S.balance_id, S.inventory_kind, S.balance_date, S.warehouse_id, S.warehouse_code,
            S.product_code, S.product_barcode, S.product_id, S.card_code, S.expiry_date,
            S.serial_number, S.batch_number, S.quantity, S.measure_code, S.input_price,
            S.filial_id, S.filial_code);
>>>>>>> ecf2bb909876df5c5d1018c65d0b3098ceb3a4cb
""")

    # 7) MERGE: Group upsert (PRIMARY KEY = balance_id + group_code)
    cursor.execute(f"""
MERGE {GROUP_TABLE} AS T
USING (
    -- null group_code lar bo‘lishi mumkin; PK uchun NULL yo‘q, shu sabab null bo‘lsa ham bitta 'NULL' sifatida saqlamoqchi bo‘lsak ISNULL ishlatamiz.
    SELECT DISTINCT balance_id,
           ISNULL(group_code, N'__NULL__') AS group_code,
           type_code
    FROM #TmpGroup
) AS S
ON (T.balance_id = S.balance_id AND T.group_code = S.group_code)
WHEN MATCHED THEN UPDATE SET
    type_code = S.type_code
WHEN NOT MATCHED THEN
    INSERT (balance_id, group_code, type_code)
    VALUES (S.balance_id, S.group_code, S.type_code);
""")

    # 8) Tozalash va commit
    cursor.execute("DROP TABLE #TmpFact; DROP TABLE #TmpGroup;")
    conn.commit()
    cursor.close()
    conn.close()

    print(f"💾 Yuklash yakunlandi | Fact yozuvlar: {len(fact_rows)} | Group yozuvlar: {len(group_rows)}")


if __name__ == "__main__":
    main()
