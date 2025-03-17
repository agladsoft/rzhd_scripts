import re
import sys
import json
import math
import itertools
import contextlib
from src.scripts.app_logger import logger
import numpy as np
from src.scripts.__init__ import *
from typing import Generator, Union
from datetime import datetime, timedelta
from pandas import DataFrame, read_excel, ExcelFile


class Rzhd(object):
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
    def convert_to_float(value: str) -> Union[float, None]:
        """
        Convert a value to float.
        """
        if value == "#" or value is None:
            return None
        try:
            return float(re.sub(" +", "", value).replace(',', '.'))
        except ValueError as e:
            raise AssertionError(f"Value is not float. Value is {value}") from e

    @staticmethod
    def convert_to_int(value: str) -> Union[int, None]:
        """
        Convert a value to integer.
        """
        if value == "#" or value is None:
            return None
        try:
            return int(value)
        except ValueError as e:
            raise AssertionError(f"Value is not integer. Value is {value}") from e

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

    def convert_format_date(self, date: str) -> Union[str, datetime.date, None]:
        """
        Convert to a date type.
        """
        if not re.findall(r"\d", date):
            raise AssertionError(f"Date format is not valid. Date is {date}")
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return datetime.strptime(date, date_format).date()
        if date.isdigit() and len(date) >= 4:
            return self.convert_xlsx_datetime_to_date(float(date))
        return None

    def convert_csv_to_dict(self, sheet: str, references: tuple) -> list:
        """
        Csv data representation in json.
        """
        for format_file, engine in DICT_FORMAT_AND_ENGINE.items():
            if format_file in self.filename:
                df: DataFrame = read_excel(self.filename, sheet_name=sheet, engine=engine, dtype=str)
                self.rename_columns(df)
                df.replace({np.NAN: None}, inplace=True)
                df = df.dropna(axis=0, how='all')
                df = df.dropna(axis=1, how='all')
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                for column in LIST_SPLIT_MONTH:
                    df[column.replace("month", "year")] = None
                return df.to_dict('records')

    def change_type(self, data: dict, index: int) -> None:
        """
        Change a type of data.
        """
        for key, value in data.items():
            with contextlib.suppress(ValueError, TypeError, IndexError):
                if key in LIST_OF_FLOAT_TYPE:
                    data[key] = self.convert_to_float(value)
                elif key in LIST_OF_DATE_TYPE and value:
                    data[key] = str(self.convert_format_date(value))
                elif key in LIST_OF_INT_TYPE:
                    data[key] = self.convert_to_int(value)
                elif key in LIST_SPLIT_MONTH:
                    self.split_month_and_year(data, key, value)

    def save_data_to_file(self, i: int, chunk_data: list, sheet: str) -> None:
        """
        Save a data to a file.
        """
        basename: str = os.path.basename(self.filename)
        output_file_path: str = os.path.join(self.folder, f'{basename}_{sheet}_{i}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        Parse data from Excel file. And split it by chunks.
        """
        xls: ExcelFile = ExcelFile(self.filename)
        for sheet in xls.sheet_names:
            parsed_data: list = self.convert_csv_to_dict(sheet, ())
            original_file_index: int = 1
            divided_parsed_data: list = list(self.divide_chunks(parsed_data, 50000))
            for chunk_parsed_data in divided_parsed_data:
                for dict_data in chunk_parsed_data:
                    self.change_type(dict_data, original_file_index)
                    dict_data['original_file_name'] = os.path.basename(self.filename)
                    dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    dict_data['original_file_index'] = original_file_index
                    original_file_index += 1
            for index, chunk_parsed_data in enumerate(divided_parsed_data):
                self.save_data_to_file(index, chunk_parsed_data, sheet)


if __name__ == "__main__":
    logger.info(f"{os.path.basename(sys.argv[1])} has started processing")
    try:
        rzhd: Rzhd = Rzhd(os.path.abspath(sys.argv[1]), sys.argv[2])
        rzhd.main()
    except Exception as ex:
        logger.error(f"Unknown error. Exception is {ex}")
        telegram(f'Ошибка при обработке файла {sys.argv[1]}. Ошибка : {ex}')
        print("unknown", file=sys.stderr)

        sys.exit(1)
    logger.info(f"{os.path.basename(sys.argv[1])} has finished processing")
