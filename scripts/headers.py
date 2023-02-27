import csv
from __init__ import *

# Открыть файл для записи
with open('headers.csv', mode='w', newline='') as file:
    writer: csv.writer = csv.writer(file, delimiter=',')

    header_row: list = list(HEADERS_ENG.values())
    writer.writerow(header_row)

    # Записать данные
    rows: list = []
    for russian_headers in HEADERS_ENG.keys():
        row: list = list(russian_headers)
        rows.append(row)

    # Найти максимальную длину строки
    max_row_len: int = max(len(row) for row in rows)

    # Добавить пустые значения в конец строк, чтобы все строки имели одинаковую длину
    for i, row in enumerate(rows):
        while len(rows[i]) < max_row_len:
            rows[i].append('')

    # Транспонировать таблицу
    rows: zip = zip(*rows)

    # Записать данные
    for row in rows:
        writer.writerow(row)
