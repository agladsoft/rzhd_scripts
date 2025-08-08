import sys
from datetime import datetime

from pandas import ExcelFile

import app_logger
from rzhd import Rzhd
from __init__ import *
from typing import Union

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", ""))


class MyError(Exception):
    pass


class RzhdWeekly(Rzhd):


    def check_is_null_value(self, key: Union[str, None], value: Union[str, None]) -> None:
        if value is None:
            raise MyError
        elif key in ["departure_day_report", "departure_date"]:
            date = str(self.convert_format_date(value))
            if not date:
                raise MyError

    def change_type(self, data: dict, index: int) -> None:
        """
        Change a type of data.
        """
        for key, value in data.items():
            try:
                if key in LIST_VALUE_NOT_NULL:
                    self.check_is_null_value(key, value)
                if key in LIST_OF_FLOAT_TYPE:
                    data[key] = self.convert_to_float(value)
                if key in LIST_OF_DATE_TYPE and value:
                    data[key] = str(self.convert_format_date(value))
                if key in LIST_OF_INT_TYPE:
                    data[key] = self.convert_to_int(value)
                if key in LIST_SPLIT_MONTH:
                    self.split_month_and_year(data, key, value)
            except MyError:
                print(f"row_{index + 1}", file=sys.stderr)
                telegram(f'Ошибка при обработке строки {index + 1}. Файл {self.filename}')
                sys.exit(1)
            except (IndexError, ValueError, TypeError):
                continue

    def main(self) -> None:
        """
        Parse data from Excel file, enrich with stations data, and split it by chunks.
        """
        # Загрузка справочника станций через базовый метод
        stations_ref = self.get_stations_reference()

        xls = ExcelFile(self.filename)
        original_file_index: int = 1

        for sheet in xls.sheet_names:
            parsed_data: list = self.convert_csv_to_dict(sheet, ())

            # Обогащение данных информацией о станциях (используем базовый метод)
            enriched_data = self.enrich_with_stations(parsed_data, stations_ref)

            divided_parsed_data: list = list(self.divide_chunks(enriched_data, 50000))

            for chunk_parsed_data in divided_parsed_data:
                for dict_data in chunk_parsed_data:
                    self.change_type(dict_data, original_file_index)
                    dict_data['original_file_name'] = os.path.basename(self.filename)
                    dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    dict_data['original_file_index'] = original_file_index
                    original_file_index += 1

            for index, chunk_parsed_data in enumerate(divided_parsed_data):
                self.save_data_to_file(index, chunk_parsed_data, sheet)

        # Закрытие соединения через базовый метод
        self.close_connection()


if __name__ == "__main__":
    logger.info(f"{os.path.basename(sys.argv[1])} has started processing")
    try:
        rzhd: RzhdWeekly = RzhdWeekly(os.path.abspath(sys.argv[1]), sys.argv[2])
        rzhd.main()
    except Exception as ex:
        logger.error(f"Unknown error. Exception is {ex}")
        telegram(f'Ошибка при обработке файла {sys.argv[1]}. Ошибка : {ex}')
        print("unknown", file=sys.stderr)
        sys.exit(1)
    logger.info(f"{os.path.basename(sys.argv[1])} has finished processing")
