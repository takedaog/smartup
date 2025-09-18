import gspread
import pandas as pd
import urllib
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials


# ============ 1. Подключение к Google Sheets ============
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1H85Jz7VR9tIGhNyKB2inyZ0V1tc-LjVQcfJWG-OuHn8"
SHEET_NAME = "F222"   # замени на актуальное имя листа
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

data = sheet.get_all_values()
df = pd.DataFrame(data)


# ============ 2. Подготовка ============
header_row = 6  # строка с заголовками в Google Sheets (нумерация с нуля → это 7-я строка)
headers = df.iloc[header_row].tolist()

# подставляем свои заголовки для первых колонок
headers[:4] = ["#", "Name", "Surname", "Contract"]

# заменяем пустые имена колонок
headers = [h if h.strip() != "" else f"col_{i}" for i, h in enumerate(headers)]

# делаем имена уникальными (если Amount повторяется → Amount_1, Amount_2 и т.д.)
def make_unique(seq):
    seen = {}
    result = []
    for x in seq:
        if x not in seen:
            seen[x] = 0
            result.append(x)
        else:
            seen[x] += 1
            result.append(f"{x}_{seen[x]}")
    return result

headers = make_unique(headers)

df.columns = headers
df = df.iloc[header_row+1:].reset_index(drop=True)

# убираем пустые строки
df = df[df["#"] != ""]
df = df[~df["#"].str.contains("TOTAL", na=False)]

print("✅ Размер таблицы после загрузки:", df.shape)
print("✅ Колонки:", df.columns.tolist())
print(df.head(10).to_string())


# ============ 3. Подключение к SQL Server ============
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Epco;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# ============ 4. Загрузка в SQL ============
df.to_sql("PaymentsRaw", engine, if_exists="replace", index=False)
print("✅ Данные успешно сохранены в таблицу PaymentsRaw")
