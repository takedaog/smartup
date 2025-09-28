# -*- coding: utf-8 -*-
import hashlib
import json
import platform
import re
import sys
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pyodbc
import requests

print(sys.getdefaultencoding())

# ====== KONFIG ======
URL = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
USERNAME = "powerbi@epco"
PASSWORD = "said_2021"
DATE_FORMAT = "%d.%m.%Y"

SQL_SERVER = "localhost"
SQL_DATABASE = "DealDB"
SQL_TRUSTED = "Yes"  # Windows auth

# Har kuni 01.01.2025 dan bugungi kunga (Asia/Samarkand) qadar ishlaydi (END dinamik)
BEGIN_DATE_STR = "01.01.2025"
FILIAL_WAREHOUSE_JSON = "filial_warehouse.json"
PRODUCT_CONDITION_JSON = "product_condition.json"

# --- 3 ta jadval ---
FACT_TABLE = "dbo.FactBalance"
GROUP_TABLE = "dbo.BalanceGroup"
CONDITION_TABLE = "dbo.BalanceCondition"
COLLATION = "Cyrillic_General_CI_AS"

# ====== INCREMENTAL SETTINGS ======
INCREMENTAL_BUFFER_DAYS = 3

# ====== UTIL ======
_WS_CHARS = "\u00A0\u202F\u2007"  # NBSP, thin space, figure space
_WS_TABLE = str.maketrans({c: " " for c in _WS_CHARS})


def today_samarkand_date():
    # Asia/Samarkand = UTC+5 (DST yo‘q)
    return (datetime.utcnow() + timedelta(hours=5)).date()


def daterange(start_date: datetime, end_date: datetime, step_days: int = 30):
    current = start_date
    while current <= end_date:
        next_date = min(current + timedelta(days=step_days - 1), end_date)
        yield current, next_date
        current = next_date + timedelta(days=1)


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


def _clean_str(s: str) -> str:
    return s.translate(_WS_TABLE).strip()


def to_date(val):
    """Matn/iso datetime -> date (yaroqsiz bo'lsa None)."""
    if val is None:
        return None
    s = _clean_str(str(val))
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
    """Har xil formatdagi sonlarni (NBSP, vergul, NaN, —, va h.k.) DECIMAL(18,4) ga mos float qiladi."""
    if val is None:
        return None
    s = _clean_str(str(val))
    if s == "" or s.lower() in {"null", "nan"} or s in {"-", "—"}:
        return None
    s = s.replace(" ", "")  # ming ajratkichlarni olib tashlash
    s = s.replace(",", ".")  # vergul -> nuqta
    s = re.sub(r"[^0-9\.\-]", "", s)  # faqat raqam, nuqta, minus
    if s in {"", "-", ".", "-.", ".-"}:
        return None
    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def safe_int(val):
    if val is None:
        return None
    s = _clean_str(str(val))
    if s == "" or s.lower() in {"null", "nan"} or s in {"-", "—"}:
        return None
    s = s.replace(" ", "")
    s = re.sub(r"[^0-9\-]", "", s)
    if s in {"", "-"}:
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
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


# ====== AUTO DTYPE INFERENCE (add-only) ======

def _round_nvarchar_len(n: int) -> int:
    """Matn uzunligini oqilona pog‘onaga yaxlitlaydi."""
    if n <= 50: return 50
    if n <= 100: return 100
    if n <= 200: return 200
    if n <= 400: return 400
    if n <= 800: return 800
    if n <= 1000: return 1000
    if n <= 2000: return 2000
    return -1  # NVARCHAR(MAX)


def _is_all_none(values):
    return all(v is None or (isinstance(v, str) and _clean_str(v) == "") for v in values)


def _looks_int(s):
    try:
        return safe_int(s) is not None
    except Exception:
        return False


def _looks_float(s):
    try:
        return to_float(s) is not None
    except Exception:
        return False


def _looks_date(s):
    try:
        return to_date(s) is not None
    except Exception:
        return False


def _has_fraction(values):
    """Agar sonlarda kasr qismi uchrasa True."""
    for v in values:
        f = to_float(v)
        if f is not None and abs(f - int(f)) > 1e-12:
            return True
    return False


def infer_sql_type_for_column(values) -> str:
    """
    Ustun qiymatlariga qarab oqilona SQL tipini qaytaradi:
      - Hammasi bo‘sh/None → NVARCHAR(100)
      - Barchasi sana → DATE
      - Barchasi raqam: kasr bo‘lsa DECIMAL(18,4); aks holda INT/BIGINT/DECIMAL(38,0)
      - Boshqa holat → NVARCHAR(rounded) COLLATE {COLLATION}
    """
    vals = list(values)

    if _is_all_none(vals):
        return f"NVARCHAR(100) COLLATE {COLLATION}"

    # Date?
    non_null = [v for v in vals if v is not None and not (isinstance(v, str) and _clean_str(v) == "")]
    if non_null and all(_looks_date(v) for v in non_null):
        return "DATE"

    # Numeric?
    if non_null and all(_looks_int(v) or _looks_float(v) for v in non_null):
        if _has_fraction(non_null) or any(_looks_float(v) and not _looks_int(v) for v in non_null):
            return "DECIMAL(18,4)"
        try:
            max_abs = max(abs(int(to_float(v))) for v in non_null if to_float(v) is not None)
            if max_abs <= 2_147_483_647:
                return "INT"
            if max_abs <= 9_223_372_036_854_775_807:
                return "BIGINT"
            return "DECIMAL(38,0)"
        except Exception:
            return "DECIMAL(18,4)"

    # Text — maksimal uzunlikka qarab
    max_len = 0
    for v in non_null:
        s = _clean_str(str(v))
        if len(s) > max_len:
            max_len = len(s)
    rounded = _round_nvarchar_len(max_len)
    if rounded == -1:
        return f"NVARCHAR(MAX) COLLATE {COLLATION}"
    return f"NVARCHAR({rounded}) COLLATE {COLLATION}"


def infer_sql_schema_from_rows(rows, column_names):
    """
    rows: list[tuple] — masalan #TmpFact/#TmpGroup/#TmpCond ga insert qilinadigan tuplar
    column_names: list[str] — ustun nomlari tartibda
    Natija: dict {col_name: sql_type_str}
    """
    if not rows:
        # Hech narsa bo‘lmasa, default NVARCHAR(100) qaytaramiz
        return {c: f"NVARCHAR(100) COLLATE {COLLATION}" for c in column_names}

    col_buckets = {i: [] for i in range(len(column_names))}
    for r in rows:
        for i, val in enumerate(r):
            col_buckets[i].append(val)

    schema = {}
    for i, col in enumerate(column_names):
        schema[col] = infer_sql_type_for_column(col_buckets[i])
    return schema


def generate_create_table_from_rows(table_name: str, rows, column_names):
    """
    rows va column_names bo‘yicha CREATE TABLE skriptini generatsiya qiladi (faqat skript, DBga yozmaydi).
    """
    sch = infer_sql_schema_from_rows(rows, column_names)
    cols_sql = [f"    [{col}] {sch[col]}" for col in column_names]
    return "CREATE TABLE " + table_name + " (\n" + ",\n".join(cols_sql) + "\n);"


# ====== DB Objects ======
def ensure_tables(cursor):
    """3 ta jadvalni yaratadi (agar bo'lmasa). PK/FK/indekslarni ham borligini tekshiradi."""
    # FactBalance
    cursor.execute(f"""
IF OBJECT_ID('{FACT_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {FACT_TABLE} (
        balance_id      CHAR(64)     NOT NULL PRIMARY KEY,
        inventory_kind  VARCHAR(50)   NULL,
        balance_date    DATE          NULL,
        warehouse_id    INT           NULL,
        warehouse_code  NVARCHAR(200) COLLATE {COLLATION} NULL,
        product_code    NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_barcode NVARCHAR(100) COLLATE {COLLATION} NULL,
        product_id      NVARCHAR(50)  COLLATE {COLLATION} NULL,
        card_code       NVARCHAR(100) COLLATE {COLLATION} NULL,
        expiry_date     DATE          NULL,
        serial_number   NVARCHAR(100) COLLATE {COLLATION} NULL,
        batch_number    NVARCHAR(100) COLLATE {COLLATION} NULL,
        quantity        INT           NULL,
        measure_code    NVARCHAR(50)  COLLATE {COLLATION} NULL,
        input_price     DECIMAL(18,4) NULL,
        filial_id       INT           NULL,
        filial_code     NVARCHAR(100) COLLATE {COLLATION} NULL
    );
END
""")

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

    # BalanceCondition
    cursor.execute(f"""
IF OBJECT_ID('{CONDITION_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {CONDITION_TABLE} (
        balance_id        CHAR(64)      NOT NULL,
        product_condition NVARCHAR(50)  COLLATE {COLLATION} NOT NULL,
        CONSTRAINT PK_BalanceCondition PRIMARY KEY (balance_id, product_condition),
        CONSTRAINT FK_BalanceCondition_FactBalance
            FOREIGN KEY (balance_id) REFERENCES {FACT_TABLE}(balance_id)
    );
END
""")
    cursor.execute(f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'IX_BalanceCondition_Cond' AND object_id = OBJECT_ID('{CONDITION_TABLE}')
)
    CREATE INDEX IX_BalanceCondition_Cond ON {CONDITION_TABLE}(product_condition);
""")


def ensure_loadstate_table(cursor):
    cursor.execute("""
IF OBJECT_ID('dbo.LoadState_Balance','U') IS NULL
BEGIN
  CREATE TABLE dbo.LoadState_Balance
  (
      scope_key         nvarchar(200) NOT NULL PRIMARY KEY, -- masalan: "filial=1|warehouse=65478|cond=T"
      last_balance_date date          NULL,
      last_run_utc      datetime2     NULL,
      last_rowcount     int           NULL
  );
END
""")


def make_scope_key(filial_id, warehouse_id, condition=None) -> str:
    if condition:
        return f"filial={filial_id}|warehouse={warehouse_id}|cond={condition}"
    return f"filial={filial_id}|warehouse={warehouse_id}"


def get_scope_state(cursor, scope_key: str):
    cursor.execute("SELECT last_balance_date FROM dbo.LoadState_Balance WHERE scope_key = ?", scope_key)
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_scope_state(cursor, scope_key: str, last_balance_date, rowcount: int):
    cursor.execute("""
    MERGE dbo.LoadState_Balance AS T
    USING (SELECT ? AS scope_key) AS S
       ON T.scope_key = S.scope_key
    WHEN MATCHED THEN UPDATE SET
        last_balance_date = CASE WHEN (? IS NULL OR ? > ISNULL(T.last_balance_date, '1900-01-01')) THEN ? ELSE T.last_balance_date END,
        last_run_utc      = SYSUTCDATETIME(),
        last_rowcount     = ?
    WHEN NOT MATCHED THEN
       INSERT (scope_key, last_balance_date, last_run_utc, last_rowcount)
       VALUES (S.scope_key, ?, SYSUTCDATETIME(), ?);
    """, scope_key, last_balance_date, last_balance_date, last_balance_date, rowcount,
                   last_balance_date, rowcount)


# ====== API → ROWS (INCREMENTAL, with product_condition) ======
def fetch_balance_chunks(cursor, filial_warehouse_list, product_conditions, user_begin_date: datetime,
                         user_end_date: datetime):
    """
    Har bir (filial_id, warehouse_id, condition) scope bo‘yicha LoadState’ni o‘qiydi:
      effective_begin = max(user_begin_date, (state_date - buffer))
      effective_end   = user_end_date
    Qaytadi: fact_rows, group_rows, condition_rows
    """
    session = requests.Session()
    fact_rows = []  # tuples like in original code
    group_rows = []
    condition_rows = []  # (balance_id, product_condition)

    seen_balance_ids = set()
    seen_group_pairs = set()
    seen_cond_pairs = set()

    total_items = 0

    for entry in filial_warehouse_list:
        filial_id = entry.get("filial_id")
        filial_code = entry.get("filial_code")
        warehouse_id = entry.get("warehouse_id")
        warehouse_code = entry.get("warehouse_code")

        for cond in product_conditions:
            # cond can be "T" or "B" or "F"
            scope_key = make_scope_key(filial_id, warehouse_id, cond)
            state_last = get_scope_state(cursor, scope_key)  # DATE or None

            effective_begin = user_begin_date
            if state_last:
                eff = state_last - timedelta(days=INCREMENTAL_BUFFER_DAYS)
                if eff > effective_begin:
                    effective_begin = eff

            effective_end = user_end_date
            if effective_begin > effective_end:
                print(f"↪️  Skip scope {scope_key}: effective_begin>{effective_end}")
                continue

            scope_max_balance_date = None
            scope_added_f, scope_added_g, scope_added_c = 0, 0, 0

            for start, finish in daterange(effective_begin, effective_end, step_days=30):
                params = {"filial_id": filial_id}
                payload = {
                    "warehouse_codes": [{"warehouse_code": warehouse_code}],
                    "filial_code": filial_code,
                    "begin_date": start.strftime(DATE_FORMAT),
                    "end_date": finish.strftime(DATE_FORMAT),
                    # API specific: include product_conditions filter if supported by API
                    "product_conditions": [cond]
                }

                try:
                    resp = session.post(
                        URL,
                        params=params,
                        auth=(USERNAME, PASSWORD),
                        headers={"Content-Type": "application/json; charset=utf-8"},
                        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                        timeout=120,
                    )
                    resp.encoding = "utf-8"
                    resp.raise_for_status()
                    data = resp.json()
                    balance = data.get("balance", [])
                    total_items += len(balance)

                    added_f, added_g, added_c = 0, 0, 0
                    for item in balance:
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

                        balance_id = make_balance_id(warehouse_id, prod_id, batch_num, bal_date)

                        # Fact rows (only once per balance_id)
                        if balance_id not in seen_balance_ids:
                            fact_rows.append((
                                balance_id, inv_kind, bal_date,
                                safe_int(warehouse_id),
                                warehouse_code, prod_code, prod_barcode, prod_id, card_code, expiry_date,
                                serial_num, batch_num,
                                qty, measure_code, input_price,
                                safe_int(filial_id), filial_code
                            ))
                            seen_balance_ids.add(balance_id)
                            added_f += 1
                            if bal_date and (scope_max_balance_date is None or bal_date > scope_max_balance_date):
                                scope_max_balance_date = bal_date

                        # Groups (may be multiple)
                        groups = item.get("groups") or [{"group_code": None, "type_code": None}]
                        for g in groups:
                            gc = g.get("group_code")
                            tc = g.get("type_code")
                            key = (balance_id, gc)
                            if key not in seen_group_pairs:
                                group_rows.append((balance_id, gc, tc))
                                seen_group_pairs.add(key)
                                added_g += 1

                        # Condition mapping (balance_id, cond)
                        cond_key = (balance_id, cond)
                        if cond_key not in seen_cond_pairs:
                            condition_rows.append((balance_id, cond))
                            seen_cond_pairs.add(cond_key)
                            added_c += 1

                    print(f"✅ {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | "
                          f"{scope_key} | cond:{cond} | items:{len(balance)} → +F:{added_f}, +G:{added_g}, +C:{added_c}")

                    scope_added_f += added_f
                    scope_added_g += added_g
                    scope_added_c += added_c

                except Exception as e:
                    print(
                        f"⚠️ API xatosi | {scope_key} | cond:{cond} | {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | {e}")

            # Update load state per (filial|warehouse|cond)
            if scope_max_balance_date:
                upsert_scope_state(cursor, scope_key, scope_max_balance_date, scope_added_f)

    print(
        f"Σ API items: {total_items} | fact_rows:{len(fact_rows)} | group_rows:{len(group_rows)} | condition_rows:{len(condition_rows)}")
    return fact_rows, group_rows, condition_rows


# ====== MAIN ======
def main():
    # 1) JSON ni UTF-8 da o‘qiymiz
    with open(FILIAL_WAREHOUSE_JSON, "r", encoding="utf-8") as f:
        filial_warehouse_list = json.load(f)

    # read product conditions as simple list of strings: ["T","B","F"]
    with open(PRODUCT_CONDITION_JSON, "r", encoding="utf-8") as f:
        cond_json = json.load(f)
    # cond_json expected like: [ {"product_conditions":["T"]}, {"product_conditions":["B"]} ... ]
    product_conditions = []
    for entry in cond_json:
        pcs = entry.get("product_conditions") or []
        for p in pcs:
            if p and p not in product_conditions:
                product_conditions.append(p)

    # 2) Sana oynasi: 01.01.2025 → bugun (Asia/Samarkand)
    begin_date = datetime.strptime(BEGIN_DATE_STR, DATE_FORMAT)
    end_date = datetime.strptime(today_samarkand_date().strftime(DATE_FORMAT), DATE_FORMAT)

    # 3) SQL ga ulanib, jadvallarni tekshiramiz
    conn = connect_sql()
    cursor = conn.cursor()
    print("✅ SQL Serverga ulandik")

    ensure_tables(cursor)
    ensure_loadstate_table(cursor)
    conn.commit()

    # 4) API dan ma’lumotlarni yig‘amiz (INCREMENTAL, per-scope per-condition)
    fact_rows, group_rows, condition_rows = fetch_balance_chunks(cursor, filial_warehouse_list, product_conditions,
                                                                 begin_date, end_date)
    if not fact_rows and not group_rows and not condition_rows:
        print("ℹ️ Yangi yozuvlar topilmadi.")
        cursor.close()
        conn.close()
        return

    # 5) Temp jadvallarni yaratish (shu joyni original koddagi temp strukturasiga mos qildim)
    def create_temp_tables():
        cursor.execute(f"""
IF OBJECT_ID('tempdb..#TmpFact') IS NOT NULL DROP TABLE #TmpFact;
CREATE TABLE #TmpFact (
    balance_id      CHAR(64)     NOT NULL,
    inventory_kind  VARCHAR(50)  NULL,
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
IF OBJECT_ID('tempdb..#TmpGroup') IS NOT NULL DROP TABLE #TmpGroup;
CREATE TABLE #TmpGroup (
    balance_id  CHAR(64)      NOT NULL,
    group_code  NVARCHAR(100) COLLATE {COLLATION} NULL,
    type_code   NVARCHAR(200) COLLATE {COLLATION} NULL
);
IF OBJECT_ID('tempdb..#TmpCond') IS NOT NULL DROP TABLE #TmpCond;
CREATE TABLE #TmpCond (
    balance_id        CHAR(64)     NOT NULL,
    product_condition NVARCHAR(50) COLLATE {COLLATION} NOT NULL
);
""")

    create_temp_tables()

    # 6) Bulk insert (Unicode safe) — xatoda fallback + temp jadvallarni qayta yaratish
    try:
        cursor.fast_executemany = True
        if fact_rows:
            cursor.executemany("""
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, fact_rows)
        if group_rows:
            cursor.executemany("""
                INSERT INTO #TmpGroup VALUES (?,?,?)
            """, group_rows)
        if condition_rows:
            cursor.executemany("""
                INSERT INTO #TmpCond VALUES (?,?)
            """, condition_rows)
    except pyodbc.Error as e:
        print(f"⚠️ fast_executemany muammo: {e}. Fallback bilan davom etamiz.")
        create_temp_tables()
        cursor.fast_executemany = False
        if fact_rows:
            cursor.executemany("""
                INSERT INTO #TmpFact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, fact_rows)
        if group_rows:
            cursor.executemany("""
                INSERT INTO #TmpGroup VALUES (?,?,?)
            """, group_rows)
        if condition_rows:
            cursor.executemany("""
                INSERT INTO #TmpCond VALUES (?,?)
            """, condition_rows)

    # 7) MERGE: Fact upsert (PRIMARY KEY = balance_id)
    cursor.execute(f"""
MERGE {FACT_TABLE} AS T
USING (
    SELECT DISTINCT *
    FROM #TmpFact
) AS S
ON (T.balance_id = S.balance_id)
WHEN MATCHED THEN UPDATE SET
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
""")

    # 8) MERGE: Group upsert (PRIMARY KEY = balance_id + group_code)
    cursor.execute(f"""
MERGE {GROUP_TABLE} AS T
USING (
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

    # 9) MERGE: BalanceCondition upsert (PRIMARY KEY = balance_id + product_condition)
    cursor.execute(f"""
MERGE {CONDITION_TABLE} AS T
USING (
    SELECT DISTINCT balance_id, product_condition
    FROM #TmpCond
) AS S
ON (T.balance_id = S.balance_id AND T.product_condition = S.product_condition)
WHEN NOT MATCHED THEN
    INSERT (balance_id, product_condition)
    VALUES (S.balance_id, S.product_condition);
""")

    # 10) Tozalash va commit
    cursor.execute("DROP TABLE #TmpFact; DROP TABLE #TmpGroup; DROP TABLE #TmpCond;")
    conn.commit()
    cursor.close()
    conn.close()

    print(
        f"💾 Yuklash yakunlandi | Fact yozuvlar: {len(fact_rows)} | Group yozuvlar: {len(group_rows)} | Condition yozuvlar: {len(condition_rows)}")


if __name__ == "__main__":
    main()