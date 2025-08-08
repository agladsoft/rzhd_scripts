import re
import sys
import json
import math
import itertools
import contextlib
import app_logger
import numpy as np
import pandas as pd
from __init__ import *
from typing import Generator, Union
from datetime import datetime, timedelta
from pandas import DataFrame, read_excel, ExcelFile
from clickhouse_connect import get_client

logger: app_logger = app_logger.get_logger(str(os.path.basename(__file__).replace(".py", "")))


class Rzhd(object):
    def __init__(self, filename: str, folder: str):
        self.filename: str = filename
        self.folder: str = folder
        self._client = None  # Кеш для подключения к БД

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

    @staticmethod
    def query_to_dataframe(client, query):
        """
        Конвертация результата запроса ClickHouse в DataFrame
        """
        result = client.query(query)
        data = result.result_rows
        columns = result.column_names
        return pd.DataFrame(data, columns=columns)

    def connect_to_clickhouse(self):
        """
        Единое подключение к ClickHouse с кешированием соединения
        """
        if self._client is not None:
            return self._client
            
        try:
            self._client = get_client(
                host=get_my_env_var('HOST'), 
                database=get_my_env_var('DATABASE'),
                username=get_my_env_var('USERNAME_DB'), 
                password=get_my_env_var('PASSWORD')
            )
            logger.info("Successfully connected to ClickHouse")
            return self._client
        except Exception as ex:
            logger.error(f"Error connecting to ClickHouse: {ex}")
            telegram(f'Ошибка подключения к БД. Файл: {self.filename}. Ошибка: {ex}')
            return None

    def get_stations_reference(self):
        """
        Загрузка справочника станций РФ и СНГ
        """
        client = self.connect_to_clickhouse()
        if client is None:
            logger.warning("No database connection, stations enrichment will be skipped")
            return None
        
        try:
            reference_stations_query = "SELECT * FROM reference_departure_stations_rf"
            reference_stations = self.query_to_dataframe(client, reference_stations_query)
            
            if not reference_stations.empty:
                reference_stations.set_index('departure_station_of_the_rf', inplace=True)
                logger.info(f"Loaded {len(reference_stations)} stations from reference")
                return reference_stations
            else:
                logger.warning("Stations reference table is empty")
                return None
        except Exception as ex:
            logger.error(f"Error loading stations reference: {ex}")
            return None

    def enrich_with_stations(self, data_list: list, stations_df=None):
        """
        Обогащение данных информацией о всех типах станций (РФ и СНГ, отправления и назначения)
        """
        if stations_df is None:
            stations_df = self.get_stations_reference()
            
        if stations_df is None or stations_df.empty:
            logger.info("Skipping stations enrichment - no reference data")
            return data_list
            
        enriched_count = 0
        for data in data_list:
            # Обогащение для всех типов станций из одного справочника
            stations_to_check = [
                ('departure_station_of_the_rf', 'departure_rf'),
                ('cis_departure_station', 'departure_cis'),
                ('rf_destination_station', 'destination_rf'), 
                ('cis_destination_station', 'destination_cis')
            ]
            
            for station_field, prefix in stations_to_check:
                station_name = data.get(station_field)
                if station_name and station_name in stations_df.index:
                    station_info = stations_df.loc[station_name]
                    data[f'{prefix}_station_type'] = station_info.get('departure_station_of_the_rf_type')
                    data[f'{prefix}_border_crossing_sign'] = station_info.get('sign_of_the_border_crossing_of_the_departure_of_the_rf')
                    data[f'{prefix}_station_sign'] = station_info.get('sign_of_departure_station_of_the_rf')
                    enriched_count += 1
        
        logger.info(f"Enriched {enriched_count} station records with reference data")
        return data_list

    def close_connection(self):
        """
        Закрытие подключения к БД с proper cleanup
        """
        if self._client:
            try:
                self._client.close()
                self._client = None
                logger.info("ClickHouse connection closed")
            except Exception as ex:
                logger.error(f"Error closing ClickHouse connection: {ex}")

    def convert_format_date(self, date: str) -> Union[str, datetime.date, None]:
        """
        Convert to a date type.
        """
        if not re.findall(r"\d", date):
            raise AssertionError(f"Date format is not valid. Date is {date}")
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
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
                df = df.dropna(axis=0, how='all')
                df = df.dropna(axis=1, how='all')
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                for column in LIST_SPLIT_MONTH:
                    df[column.replace("month", "year")] = None
                df.replace({np.nan: None, np.NAN: None, np.NaN: None, "NaT": None}, inplace=True)
                return df.to_dict('records')
        return []

    def change_type(self, data: dict, index: int) -> None:
        """
        Change a type of data.
        """
        for key, value in data.items():
            with contextlib.suppress(ValueError, TypeError, IndexError):
                if key in LIST_OF_FLOAT_TYPE:
                    data[key] = self.convert_to_float(value)
                elif key in LIST_OF_DATE_TYPE and value:
                    data[key] = self.convert_format_date(value)
                elif key in LIST_OF_INT_TYPE:
                    data[key] = self.convert_to_int(value)
                elif key in LIST_SPLIT_MONTH:
                    self.split_month_and_year(data, key, value)
                elif key in LIST_OF_CAPITALIZE_TYPE:
                    data[key] = str(value).strip().capitalize() if value and value != "#" else None

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
        
        # Закрытие соединения с БД
        self.close_connection()


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
