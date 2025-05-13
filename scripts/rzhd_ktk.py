import re
import sys
import app_logger
import numpy as np
import pandas as pd
from rzhd import Rzhd
from __init__ import *
from typing import List
from datetime import datetime
from collections import defaultdict
from clickhouse_connect import get_client
from pandas import ExcelFile, DataFrame, read_excel

logger: app_logger = app_logger.get_logger(str(os.path.basename(__file__).replace(".py", "")))


class RzhdKTK(Rzhd):

    @staticmethod
    def _get_information_default_dict(default_dict: defaultdict, name: str) -> list:
        return [data.get(name) for data in default_dict]

    def find_date_and_name_of_cargo(self, data: dict, deep_data: defaultdict, deep_date: datetime) -> bool:
        departure_date = self._get_information_default_dict(deep_data[data['container_no']], 'departure_date')
        name_of_cargo = self._get_information_default_dict(deep_data[data['container_no']], 'name_of_cargo')
        return (
            deep_date in departure_date
            and data.get('name_of_cargo') in name_of_cargo
        )

    @staticmethod
    def replace_organization_form(payer_of_the_railway_tariff):
        replacements = [
            (r'ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ', 'ИП '),
            (r'ОБОСОБЛЕННОЕ ПОДРАЗДЕЛЕНИЕ', 'ОП '),
            (r'ОБ[ЩШ]ЕСТВ[ОА] С О(Л?)(Р?)Г(Р?)АНИ[ЧЦ](Е?)Н(Н?)(Н?)ОЙ\s*(О?)ТВ(Е?)(Н?)(Т?)(С?)(Т?)(С?)[ВС]ЕНН(Н?)ОСТЬ('
             r'Ю?)', 'ООО '),
            (r'ОБЩЕСТВО С ОГРАНИЧЕННОЙ', 'ООО '),
            (r'ТОВАРИЩЕ(Н?)СТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ', 'ТОО '),
            (r'НЕПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', 'НАО '),
            (r'ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО ЦЕНТР ПО ПЕРЕВОЗКЕ ГРУЗОВ В КОНТЕЙНЕРА', 'ПАО ТРАНСКОНТЕЙНЕР'),
            (r'ПУБЛИЧНО(Е|ГО) АКЦИОНЕРНО(Е|ГО) ОБЩЕСТВ[ОА]', 'ПАО '),
            (r'ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', 'ОАО '),
            (r'ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО', 'ЗАО '),
            (r'АКЦИОНЕРНО(Е|ГО) ОБЩЕСТВ[ОА]', 'АО '),
            (r'АКЦИОНЕРНАЯ КОМПАНИЯ', 'АК '),
            (r'ФЕДЕРАЛЬНОЕ АГЕНТСТВО', 'ФА '),
            (r'ООО', 'ООО '),
            (r'ОАО', 'ОАО '),
        ]

        for pattern, replacement in replacements:
            payer_of_the_railway_tariff = re.sub(pattern, replacement, payer_of_the_railway_tariff)

        return payer_of_the_railway_tariff

    def change_value(self, data: dict):
        if not data.get('name_of_cargo'):
            data['name_of_cargo'] = 'Пустой'
        if data.get('payer_of_the_railway_tariff'):
            payer_of_the_railway_tariff = re.sub(
                r'[",()?«».\']', '', str(data['payer_of_the_railway_tariff']).upper()
            ).strip()
            payer_of_the_railway_tariff = self.replace_organization_form(payer_of_the_railway_tariff)
            payer_of_the_railway_tariff = re.sub(r' +', ' ', payer_of_the_railway_tariff)
            data['payer_of_the_railway_tariff'] = payer_of_the_railway_tariff
        if not data.get('payer_of_the_railway_tariff_unified'):
            data['payer_of_the_railway_tariff_unified'] = data['payer_of_the_railway_tariff']

    @staticmethod
    def get_dict_containers(rzhd_query):
        default_d = defaultdict(list)
        for row in rzhd_query.result_rows:
            default_d[row[0]].append({'departure_date': row[1], 'name_of_cargo': row[2]})
        logger.info("Dictionary is filled with containers")
        return default_d

    @staticmethod
    def query_to_dataframe(client, query):
        result = client.query(query)
        data = result.result_rows  # Получаем данные
        columns = result.column_names  # Получаем названия столбцов
        return pd.DataFrame(data, columns=columns)

    def get_reference(self, client):
        """
        Getting references
        :param client: client Clickhouse
        :return:
        """
        # Загружаем таблицы
        reference_tonnage_query = "SELECT * FROM reference_tonnage"
        reference_container_type_query = "SELECT * FROM reference_container_type"
        reference_replace_company_name_query = "SELECT * FROM reference_replace_company_name"

        reference_tonnage = self.query_to_dataframe(client, reference_tonnage_query)
        reference_container_type = self.query_to_dataframe(client, reference_container_type_query)
        reference_replace_company_name = self.query_to_dataframe(client, reference_replace_company_name_query)

        reference_replace_company_name.rename(
            columns={
                "company_name": "payer_of_the_railway_tariff",
                "company_name_unified": "payer_of_the_railway_tariff_unified"
            },
            inplace=True
        )

        reference_tonnage.set_index('container_tonnage', inplace=True)
        reference_container_type.set_index('type_of_special_container', inplace=True)
        reference_replace_company_name.set_index('payer_of_the_railway_tariff', inplace=True)

        return reference_tonnage, \
            reference_container_type, \
            reference_replace_company_name

    def connect_to_db(self):
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            logger.info("Successfully connect to db")
            rzhd_query = client.query(
                "SELECT container_no, departure_date, name_of_cargo "
                "FROM rzhd_ktk "
                "GROUP BY container_no, departure_date, name_of_cargo"
            )
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(rzhd_query.result_rows[0])
            return self.get_dict_containers(rzhd_query), client
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            print("error_connect_db", file=sys.stderr)
            telegram(f'Нет подключения к базе данных. Файл : {self.filename}')
            sys.exit(1)

    @staticmethod
    def get_last_data_with_dupl(parsed_data: List[dict]) -> list:
        """
        Mark rows as obsolete if there's a duplicate of 'container_no' and 'departure_date'.
        """
        seen = {}
        for i, dict_data in enumerate(parsed_data):
            key = (dict_data["container_no"], dict_data["departure_date"], dict_data["name_of_cargo"])
            if key in seen:
                parsed_data[seen[key]]["is_obsolete"] = True
            else:
                seen[key] = i
            dict_data["is_obsolete"] = False
        return parsed_data

    def convert_csv_to_dict(self, sheet: str, references: tuple) -> list:
        """
        Csv data representation in json.
        """
        for format_file, engine in DICT_FORMAT_AND_ENGINE.items():
            if format_file in self.filename:
                logger.info(f"Starting to read Excel file: {self.filename}, sheet: {sheet}")
                df: DataFrame = read_excel(self.filename, sheet_name=sheet, engine=engine, dtype=str)
                logger.info(f"Ending to read Excel file: {self.filename}, sheet: {sheet}")
                self.rename_columns(df)
                df.set_index(
                    ['container_tonnage', 'type_of_special_container', 'payer_of_the_railway_tariff'],
                    inplace=True
                )
                logger.info(f"Starting to join Excel file: {self.filename}, sheet: {sheet}")
                df = df.join(references[0]).join(references[1]).join(references[2])
                logger.info(f"Ending to join Excel file: {self.filename}, sheet: {sheet}")
                df.replace({np.nan: None, np.NAN: None, np.NaN: None, "NaT": None}, inplace=True)
                df = df.dropna(axis=0, how='all')
                df = df.dropna(axis=1, how='all')
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                for column in LIST_SPLIT_MONTH:
                    df[column.replace("month", "year")] = None
                return df.reset_index().to_dict('records')
            return []
        return []

    def main(self) -> None:
        """
        Parse data from Excel file and split it by chunks.
        """
        date_and_containers, client = self.connect_to_db()
        references = self.get_reference(client)
        xls = ExcelFile(self.filename)
        original_file_name = os.path.basename(self.filename)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for sheet in xls.sheet_names:
            parsed_data = self.get_last_data_with_dupl(self.convert_csv_to_dict(sheet, references))
            divided_parsed_data = list(self.divide_chunks(parsed_data, 50000))
            original_file_index: int = 1
            for index, chunk in enumerate(divided_parsed_data):
                for data in chunk:
                    self.change_type(data, original_file_index)
                    self.change_value(data)
                    dep_date = self.convert_format_date(data["departure_date"])
                    if self.find_date_and_name_of_cargo(data, date_and_containers, dep_date):
                        client.query(f"""
                            ALTER TABLE rzhd.rzhd_ktk
                            UPDATE is_obsolete=true
                            WHERE departure_date = '{data['departure_date']}'
                            AND container_no = '{data['container_no']}'
                            AND name_of_cargo = '{data['name_of_cargo']}'
                        """)
                    data.update({
                        'original_file_name': original_file_name,
                        'original_file_parsed_on': timestamp,
                        'original_file_index': original_file_index
                    })
                    original_file_index += 1

                self.save_data_to_file(index, chunk, sheet)


if __name__ == "__main__":
    logger.info(f"{os.path.basename(sys.argv[1])} has started processing")
    try:
        rzhd: RzhdKTK = RzhdKTK(os.path.abspath(sys.argv[1]), sys.argv[2])
        rzhd.main()
    except Exception as ex:
        logger.error(f"Unknown error. Exception is {ex}")
        telegram(f'Ошибка при обработке файла {sys.argv[1]}. Ошибка : {ex}')
        print("unknown", file=sys.stderr)
        sys.exit(1)
    logger.info(f"{os.path.basename(sys.argv[1])} has finished processing")
