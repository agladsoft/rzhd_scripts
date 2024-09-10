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
    def get_dict_containers(rzhd_query):
        default_d = defaultdict(list)
        for row in rzhd_query.result_rows:
            default_d[row[0]].append(row[1])
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
                "SELECT container_no, departure_date FROM rzhd_ktk GROUP BY container_no, departure_date"
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

            for index, chunk in enumerate(divided_parsed_data):
                for i, data in enumerate(chunk, start=1):
                    self.change_type(data, i)
                    dep_date = self.convert_format_date(data["departure_date"])
                    if dep_date in date_and_containers.get(data["container_no"], []):
                        client.query(f"""
                            ALTER TABLE rzhd.rzhd_ktk 
                            UPDATE is_obsolete=true 
                            WHERE departure_date = '{data['departure_date']}' 
                            AND container_no = '{data['container_no']}'
                        """)
                    data.update({
                        'original_file_name': original_file_name,
                        'original_file_parsed_on': timestamp,
                        'original_file_index': i
                    })

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
