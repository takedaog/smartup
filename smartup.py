import requests
import json
from datetime import datetime, timedelta

url = "https://smartup.online/b/anor/mxsx/mkw/balance$export"
username = "powerbi@epco"
password = "said_2021"

DATE_FORMAT = "%d.%m.%Y"

# Разбивка на интервалы по 30 дней (но итог будет один JSON)
def daterange(start_date, end_date, step_days=30):
    current = start_date
    while current <= end_date:
        next_date = min(current + timedelta(days=step_days - 1), end_date)
        yield current, next_date
        current = next_date + timedelta(days=1)

# Загружаем связку филиал ↔ склад
with open("filial_warehouse.json", "r", encoding="utf-8") as f:
    filial_warehouse_list = json.load(f)

# Итоговый словарь
final_data = {"balance": []}
seen = set()  # сюда складываем уникальные записи

# Укажи диапазон дат сам
begin_date = datetime.strptime("15.02.2025", DATE_FORMAT)
end_date   = datetime.strptime("15.04.2025", DATE_FORMAT)

for entry in filial_warehouse_list:
    filial_id = entry["filial_id"]
    filial_code = entry["filial_code"]
    warehouse_id = entry["warehouse_id"]
    warehouse_code = entry["warehouse_code"]

    for start, finish in daterange(begin_date, end_date, step_days=30):
        params = {"filial_id": filial_id}
        payload = {
            "warehouse_codes": [{"warehouse_code": warehouse_code}],
            "filial_code": filial_code,
            "begin_date": start.strftime(DATE_FORMAT),
            "end_date": finish.strftime(DATE_FORMAT)
        }

        try:
            response = requests.post(
                url,
                params=params,
                auth=(username, password),
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=60
            )
            response.raise_for_status()

            data = response.json()
            balance_data = data.get("balance", [])

            added_count = 0
            for item in balance_data:
                item["filial_id"] = filial_id
                item["filial_code"] = filial_code
                item["warehouse_id"] = warehouse_id
                item["warehouse_code"] = warehouse_code

                # создаём ключ для уникальности
                key = json.dumps(item, sort_keys=True, ensure_ascii=False)
                if key not in seen:
                    seen.add(key)
                    final_data["balance"].append(item)
                    added_count += 1

            print(f"✅ {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | filial={filial_code} | warehouse={warehouse_code} | {len(balance_data)} items ({added_count} new)")

        except Exception as e:
            print(f"⚠️ Ошибка | filial={filial_code} | warehouse={warehouse_code} | {start.strftime(DATE_FORMAT)} - {finish.strftime(DATE_FORMAT)} | {e}")

# Сохраняем итог в один файл
with open("final_all12.json", "w", encoding="utf-8") as f:
    json.dump(final_data, f, ensure_ascii=False, indent=4)

print(f"💾 All data saved to final_all12.json | Total unique records: {len(final_data['balance'])}")
