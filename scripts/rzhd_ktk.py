import sys
import app_logger
from rzhd import Rzhd
from __init__ import *
from pandas import ExcelFile
from datetime import datetime
from collections import defaultdict
from clickhouse_connect import get_client

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", ""))


class RzhdKTK(Rzhd):

    @staticmethod
    def _get_information_default_dict(default_dict: defaultdict, name: str) -> list:
        return [data.get(name) for data in default_dict]

    def find_date_and_name_of_cargo(self, data: dict, deep_data: defaultdict, deep_date: datetime) -> bool:
        departure_date = self._get_information_default_dict(deep_data[data['container_no']], 'departure_date')
        name_of_cargo = self._get_information_default_dict(deep_data[data['container_no']], 'name_of_cargo')
        if deep_date in departure_date and data.get('name_of_cargo') in name_of_cargo:
            return True
        return False

    @staticmethod
    def change_name_of_cargo(data: dict):
        if not data.get('name_of_cargo'):
            data['name_of_cargo'] = 'Пустой'

    @staticmethod
    def get_dict_containers(rzhd_query):
        default_d = defaultdict(list)
        for row in rzhd_query.result_rows:
            default_d[row[0]].append({'departure_date': row[1], 'name_of_cargo': row[2]})
        logger.info("Dictionary is filled with containers")
        return default_d

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
                "SELECT container_no, departure_date, name_of_cargo FROM rzhd_ktk GROUP BY container_no, departure_date, name_of_cargo"
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
    def get_last_data_with_dupl(parsed_data: list) -> list:
        """
        Mark rows as obsolete if there's a duplicate of 'container_no' and 'departure_date'.
        """
        seen = {}
        for i, dict_data in enumerate(parsed_data):
            key = (dict_data["container_no"], dict_data["departure_date"])
            if key in seen:
                parsed_data[seen[key]]["is_obsolete"] = True
            else:
                seen[key] = i
            dict_data["is_obsolete"] = False
        return parsed_data

    def main(self) -> None:
        """
        Parse data from Excel file and split it by chunks.
        """
        date_and_containers, client = self.connect_to_db()
        xls = ExcelFile(self.filename)
        original_file_name = os.path.basename(self.filename)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for sheet in xls.sheet_names:
            parsed_data = self.get_last_data_with_dupl(self.convert_csv_to_dict(sheet))
            divided_parsed_data = list(self.divide_chunks(parsed_data, 50000))
            original_file_index: int = 1
            for index, chunk in enumerate(divided_parsed_data):
                for data in chunk:
                    self.change_type(data, original_file_index)
                    self.change_name_of_cargo(data)
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
