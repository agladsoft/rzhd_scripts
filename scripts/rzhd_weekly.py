import os
import sys
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
            date = self.convert_format_date(value)
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
                    data[key] = self.convert_format_date(value)
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
