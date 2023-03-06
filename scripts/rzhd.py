import os
import sys
import json
import math
import itertools
import contextlib
import numpy as np
from __init__ import *
from typing import Generator
from datetime import datetime, timedelta
from pandas import DataFrame, read_csv, read_excel


class RZHD(object):
    def __init__(self, filename: str, folder: str):
        self.filename: str = filename
        self.folder: str = folder

    @staticmethod
    def divide_chunks(list_data: list, chunk: int) -> Generator:
        """
        Divide by chunks of a list.
        """
        for i in range(0, len(list_data), chunk):
            yield list_data[i:i + chunk]

    @staticmethod
    def convert_to_int(int_value: str) -> int:
        """
        Convert a value to integer.
        """
        with contextlib.suppress(ValueError):
            return int(int_value)

    @staticmethod
    def split_month_and_year(data: dict, key: str, value: str) -> None:
        """
        Split month and year.
        """
        try:
            year, month = value.split("/")
            data[key] = int(month)
            data[key.replace("month", "year")] = int(year)
        except (AttributeError, ValueError):
            data[key] = value

    @staticmethod
    def rename_columns(df: DataFrame) -> None:
        """
        Rename of a columns.
        """
        dict_columns_eng: dict = {}
        for column, columns in itertools.product(df.columns, HEADERS_ENG):
            for column_eng in columns:
                column_strip: str = column.strip()
                if column_strip == column_eng.strip():
                    dict_columns_eng[column] = HEADERS_ENG[columns]
        df.rename(columns=dict_columns_eng, inplace=True)

    @staticmethod
    def convert_xlsx_datetime_to_date(xlsx_datetime: float) -> str:
        """
        Convert date to %Y-%m-%d from xlsx value.
        """
        days: float
        portion: float
        temp_date: datetime = datetime(1899, 12, 30)
        (days, portion) = math.modf(xlsx_datetime)
        delta_days: timedelta = timedelta(days=days)
        secs: int = int(24 * 60 * 60 * portion)
        delta_seconds: timedelta = timedelta(seconds=secs)
        time: datetime = (temp_date + delta_days + delta_seconds)
        return time.strftime("%Y-%m-%d")

    def convert_format_date(self, date: str) -> str:
        """
        Convert to a date type.
        """
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        if date.isdigit():
            return self.convert_xlsx_datetime_to_date(float(date))
        return date

    def convert_csv_to_dict(self, xlsb) -> list:
        """
        Csv data representation in json.
        """
        if xlsb:
            df: DataFrame = read_excel(self.filename, sheet_name=0, engine=xlsb, dtype=str)
        else:
            df: DataFrame = read_csv(self.filename, low_memory=False, dtype=str)
        df.replace({np.NAN: None}, inplace=True)
        df = df.dropna(axis=0, how='all')
        df = df.dropna(axis=1, how='all')
        for column in LIST_SPLIT_MONTH:
            df[column.replace("month", "year")] = None
        self.rename_columns(df)
        return df.to_dict('records')

    def change_type(self, data: dict) -> None:
        """
        Change a type of data.
        """
        for key, value in data.items():
            with contextlib.suppress(Exception):
                if key in LIST_OF_FLOAT_TYPE:
                    data[key] = float(value.replace(',', '.'))
                elif key in LIST_OF_DATE_TYPE:
                    data[key] = self.convert_format_date(value)
                elif key in LIST_OF_INT_TYPE:
                    data[key] = self.convert_to_int(value)
                elif key in LIST_SPLIT_MONTH:
                    self.split_month_and_year(data, key, value)

    def save_data_to_file(self, i: int, chunk_data: list) -> None:
        """
        Save a data to a file.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.folder, f'{basename}_{i}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, ensure_ascii=False, indent=4)

    def main(self, xlsb=None) -> None:
        parsed_data: list = self.convert_csv_to_dict(xlsb)
        original_file_index: int = 0
        divided_parsed_data: list = list(self.divide_chunks(parsed_data, 50000))
        for index, chunk_parsed_data in enumerate(divided_parsed_data):
            for dict_data in chunk_parsed_data:
                self.change_type(dict_data)
                dict_data['original_file_name'] = os.path.basename(self.filename)
                dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                dict_data['original_file_index'] = original_file_index
                original_file_index += 1
            self.save_data_to_file(index, chunk_parsed_data)


if __name__ == "__main__":
    rzhd: RZHD = RZHD(os.path.abspath(sys.argv[1]), sys.argv[2])
    rzhd.main()
