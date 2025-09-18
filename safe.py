import pyodbc
import json

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=SmartUpDB;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()
print("✅ Успешное подключение к SQL Server")

# Читаем JSON
with open("final_all.json", "r", encoding="utf-8") as f:
    data = json.load(f)

if isinstance(data, dict) and "balance" in data:
    data = data["balance"]

rows = []
for item in data:
    groups = item.get("groups", [])
    if not groups:
        groups = [{"group_code": None, "type_code": None}]
    for g in groups:
        rows.append((
            item.get("inventory_kind"),
            item.get("date"),
            int(item["warehouse_id"]) if item.get("warehouse_id") else None,
            item.get("warehouse_code"),
            item.get("product_code"),
            item.get("product_barcode"),
            item.get("product_id"),
            item.get("card_code"),
            item.get("expiry_date"),
            item.get("serial_number"),
            item.get("batch_number"),
            float(item["quantity"]) if item.get("quantity") else None,
            item.get("measure_code"),
            float(item["input_price"]) if item.get("input_price") else None,
            int(item["filial_id"]) if item.get("filial_id") else None,
            item.get("filial_code"),
            g.get("group_code"),
            g.get("type_code")
        ))

# Создаём временную таблицу
cursor.execute("""
CREATE TABLE #TempBalanceData (
    inventory_kind NVARCHAR(MAX),
    [date] DATE,
    warehouse_id INT,
    warehouse_code NVARCHAR(MAX),
    product_code NVARCHAR(MAX),
    product_barcode NVARCHAR(MAX),
    product_id NVARCHAR(MAX),
    card_code NVARCHAR(MAX),
    expiry_date DATE,
    serial_number NVARCHAR(MAX),
    batch_number NVARCHAR(MAX),
    quantity FLOAT,
    measure_code NVARCHAR(MAX),
    input_price FLOAT,
    filial_id INT,
    filial_code NVARCHAR(MAX),
    group_code NVARCHAR(MAX),
    type_code NVARCHAR(MAX)
)
""")

# Вставляем все данные во временную таблицу
cursor.fast_executemany = True
cursor.executemany("""
INSERT INTO #TempBalanceData VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", rows)

# MERGE во главную таблицу, вставляем только новые записи
cursor.execute("""
MERGE BalanceData AS target
USING #TempBalanceData AS source
ON target.warehouse_id = source.warehouse_id
   AND target.product_code = source.product_code
   AND target.[date] = source.[date]
   AND target.batch_number = source.batch_number
WHEN NOT MATCHED BY TARGET THEN
INSERT (
    inventory_kind, [date], warehouse_id, warehouse_code,
    product_code, product_barcode, product_id, card_code,
    expiry_date, serial_number, batch_number, quantity,
    measure_code, input_price, filial_id, filial_code,
    group_code, type_code
)
VALUES (
    source.inventory_kind, source.[date], source.warehouse_id, source.warehouse_code,
    source.product_code, source.product_barcode, source.product_id, source.card_code,
    source.expiry_date, source.serial_number, source.batch_number, source.quantity,
    source.measure_code, source.input_price, source.filial_id, source.filial_code,
    source.group_code, source.type_code
);
""")  # ← точка с запятой в конце


# Удаляем временную таблицу
cursor.execute("DROP TABLE #TempBalanceData")

conn.commit()
cursor.close()
conn.close()

print("✅ Данные успешно загружены в SQL Server (только новые записи, быстро!)")

