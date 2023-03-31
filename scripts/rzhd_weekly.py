import os
import sys
from rzhd import Rzhd
from __init__ import *
from typing import Union
from pandas import DataFrame


class MyError(Exception):
    pass


class RzhdWeekly(Rzhd):

    @staticmethod
    def check_is_null_value(value: Union[str, None]) -> None:
        if value is None:
            raise MyError

    def change_type(self, data: dict, index: int) -> None:
        """
        Change a type of data.
        """
        for key, value in data.items():
            try:
                if key in LIST_VALUE_NOT_NULL:
                    self.check_is_null_value(value)
                if key in LIST_OF_FLOAT_TYPE:
                    data[key] = self.convert_to_float(value)
                if key in LIST_OF_DATE_TYPE:
                    data[key] = self.convert_format_date(value)
                if key in LIST_OF_INT_TYPE:
                    data[key] = self.convert_to_int(value)
                if key in LIST_SPLIT_MONTH:
                    self.split_month_and_year(data, key, value)
            except MyError:
                print(f"row_{index + 1}", file=sys.stderr)
                sys.exit(1)
            except (IndexError, ValueError, TypeError):
                continue

    @staticmethod
    def shift_columns(df: DataFrame):
        """
        Shift of a columns.
        """
        for i, r in df.iterrows():
            if df.loc[i]["container_prefix"].isdigit():
                df.loc[i][16:] = df.loc[i][16:].shift(1)


if __name__ == "__main__":
    rzhd: RzhdWeekly = RzhdWeekly(os.path.abspath(sys.argv[1]), sys.argv[2])
    rzhd.main()
