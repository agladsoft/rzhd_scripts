import os
import sys
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime
from collections import defaultdict
sys.modules['src.scripts.app_logger'] = MagicMock()
from src.scripts.rzhd import Rzhd
from src.scripts.rzhd_ktk import RzhdKTK



os.environ['XL_IDP_ROOT_RZHD'] = '/tmp'


class TestRzhdKTK:
    @pytest.fixture
    def rzhd_ktk_instance(self):
        """Создание тестового экземпляра класса RzhdKTK."""
        filename = "test_file.xlsx"
        destination = "test_destination"
        with patch.object(RzhdKTK, '__init__', return_value=None):
            instance = RzhdKTK(filename, destination)
            instance.filename = filename
            return instance

    @pytest.fixture
    def mock_client(self):
        """Мок для клиента ClickHouse."""
        mock = MagicMock()
        mock_result = MagicMock()
        mock_result.column_names = ["container_no", "departure_date", "name_of_cargo"]
        mock_result.result_rows = [
            ["CONT123456", "2023-01-01", "Кирпичи"],
            ["CONT123456", "2023-01-02", "Цемент"],
            ["CONT789012", "2023-01-01", "Песок"]
        ]
        mock.query.return_value = mock_result
        return mock

    @pytest.fixture
    def mock_references(self):
        """Мок для справочных данных."""
        reference_tonnage = pd.DataFrame({
            'container_tonnage': ['20', '40'],
            'tonnage_normalized': [20, 40]
        }).set_index('container_tonnage')

        reference_container_type = pd.DataFrame({
            'type_of_special_container': ['REF', 'STD'],
            'container_type_normalized': ['Рефрижератор', 'Стандартный']
        }).set_index('type_of_special_container')

        reference_replace_company_name = pd.DataFrame({
            'payer_of_the_railway_tariff': ['ООО КОМПАНИЯ', 'АО ФИРМА'],
            'payer_of_the_railway_tariff_unified': ['ООО Компания', 'АО Фирма']
        }).set_index('payer_of_the_railway_tariff')

        return (reference_tonnage, reference_container_type, reference_replace_company_name)

    @patch('src.scripts.rzhd_ktk.get_client')
    @patch('src.scripts.rzhd_ktk.get_my_env_var')
    def test_connect_to_db(self, mock_get_env, mock_get_client, rzhd_ktk_instance, mock_client):
        """Тест подключения к базе данных."""
        mock_get_env.side_effect = lambda var: {
            'HOST': 'test_host',
            'DATABASE': 'test_db',
            'USERNAME_DB': 'test_user',
            'PASSWORD': 'test_pass'
        }[var]
        mock_get_client.return_value = mock_client
        result, client = rzhd_ktk_instance.connect_to_db()

        assert isinstance(result, defaultdict)
        assert client == mock_client
        mock_get_client.assert_called_once_with(
            host='test_host',
            database='test_db',
            username='test_user',
            password='test_pass'
        )
        mock_client.query.assert_called_once()

    def test_get_dict_containers(self, rzhd_ktk_instance, mock_client):
        """Тест формирования словаря контейнеров."""
        result = rzhd_ktk_instance.get_dict_containers(mock_client.query.return_value)

        assert 'CONT123456' in result
        assert len(result['CONT123456']) == 2
        assert result['CONT123456'][0]['departure_date'] == "2023-01-01"
        assert result['CONT123456'][0]['name_of_cargo'] == "Кирпичи"
        assert result['CONT789012'][0]['name_of_cargo'] == "Песок"

    def test_replace_organization_form(self, rzhd_ktk_instance):
        """Тест замены формы организации в названии компании."""
        test_cases = [
            ("ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ ТЕСТ", "ООО   ТЕСТ"),
            ("ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ ИВАНОВ", "ИП  ИВАНОВ"),
            ("ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО ГАЗПРОМ", "ПАО  ГАЗПРОМ"),
            ("ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО ТЕХНОЛОГИИ", "ЗАО  ТЕХНОЛОГИИ")
        ]

        for input_name, expected_output in test_cases:
            result = rzhd_ktk_instance.replace_organization_form(input_name)
            assert result == expected_output

    def test_change_value(self, rzhd_ktk_instance):
        """Тест изменения значений в данных."""
        data = {
            'name_of_cargo': None,
            'payer_of_the_railway_tariff': 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ТЕСТ"',
            'payer_of_the_railway_tariff_unified': None
        }

        rzhd_ktk_instance.change_value(data)

        assert data['name_of_cargo'] == 'Пустой'
        assert data['payer_of_the_railway_tariff'] == 'ООО ТЕСТ'
        assert data['payer_of_the_railway_tariff_unified'] == 'ООО ТЕСТ'

    @patch('src.scripts.rzhd_ktk.read_excel')
    def test_convert_csv_to_dict(self, mock_read_excel, rzhd_ktk_instance, mock_references):
        """Тест преобразования данных Excel в словарь."""
        mock_df = pd.DataFrame({
            'container_no': ['CONT123', 'CONT456'],
            'departure_date': ['2023-01-01', '2023-01-02'],
            'name_of_cargo': ['Кирпичи', 'Цемент'],
            'container_tonnage': ['20', '40'],
            'type_of_special_container': ['STD', 'REF'],
            'payer_of_the_railway_tariff': ['ООО КОМПАНИЯ', 'АО ФИРМА']
        })

        with patch.object(Rzhd, 'rename_columns', return_value=None) as mock_rename:

            mock_read_excel.return_value = mock_df
            rzhd_ktk_instance.filename = 'test_file.xlsx'

            with patch('src.scripts.__init__.DICT_FORMAT_AND_ENGINE', {'xlsx': 'openpyxl'}), \
                    patch('src.scripts.__init__.LIST_SPLIT_MONTH', ['month1', 'month2']):
                result = rzhd_ktk_instance.convert_csv_to_dict('Sheet1', mock_references)

                assert isinstance(result, list)
                assert len(result) == 2
                assert result[0]['container_no'] == 'CONT123'
                assert result[1]['container_no'] == 'CONT456'

                mock_rename.assert_called_once()
                mock_read_excel.assert_called_once_with('test_file.xlsx', sheet_name='Sheet1', engine='openpyxl',
                                                        dtype=str)

    def test_find_date_and_name_of_cargo(self, rzhd_ktk_instance):
        """Тест поиска даты и имени груза."""
        data = {'container_no': 'CONT123', 'name_of_cargo': 'Кирпичи'}
        deep_data = defaultdict(list)
        deep_data['CONT123'].append({'departure_date': datetime(2023, 1, 1), 'name_of_cargo': 'Кирпичи'})
        deep_data['CONT123'].append({'departure_date': datetime(2023, 1, 2), 'name_of_cargo': 'Цемент'})

        result = rzhd_ktk_instance.find_date_and_name_of_cargo(data, deep_data, datetime(2023, 1, 1))
        assert result is True

        result = rzhd_ktk_instance.find_date_and_name_of_cargo(data, deep_data, datetime(2023, 1, 3))
        assert result is False

        data['name_of_cargo'] = 'Песок'
        result = rzhd_ktk_instance.find_date_and_name_of_cargo(data, deep_data, datetime(2023, 1, 1))
        assert result is False

    def test_get_last_data_with_dupl(self, rzhd_ktk_instance):
        """Тест обработки дубликатов в данных."""
        parsed_data = [
            {"container_no": "CONT123", "departure_date": "2023-01-01", "name_of_cargo": "Кирпичи"},
            {"container_no": "CONT456", "departure_date": "2023-01-02", "name_of_cargo": "Цемент"},
            {"container_no": "CONT123", "departure_date": "2023-01-01", "name_of_cargo": "Кирпичи"}  # Дубликат
        ]

        result = rzhd_ktk_instance.get_last_data_with_dupl(parsed_data)

        assert result[0]["is_obsolete"] is True
        assert result[1]["is_obsolete"] is False
        assert result[2]["is_obsolete"] is False

    @patch.object(RzhdKTK, 'connect_to_db')
    @patch.object(RzhdKTK, 'get_reference')
    @patch('src.scripts.rzhd_ktk.ExcelFile')
    @patch.object(RzhdKTK, 'get_last_data_with_dupl')
    @patch.object(RzhdKTK, 'convert_csv_to_dict')
    @patch.object(RzhdKTK, 'divide_chunks')
    @patch.object(RzhdKTK, 'change_type')
    @patch.object(RzhdKTK, 'change_value')
    @patch.object(RzhdKTK, 'convert_format_date')
    @patch.object(RzhdKTK, 'find_date_and_name_of_cargo')
    @patch.object(RzhdKTK, 'save_data_to_file')
    def test_main(self, mock_save, mock_find, mock_convert_date, mock_change_value,
                  mock_change_type, mock_divide, mock_convert_csv, mock_get_last,
                  mock_excel, mock_get_ref, mock_connect, rzhd_ktk_instance, mock_client):
        """Тест основного метода main."""
        mock_connect.return_value = (defaultdict(list), mock_client)
        mock_get_ref.return_value = ("ref1", "ref2", "ref3")

        mock_excel_instance = MagicMock()
        mock_excel_instance.sheet_names = ["Sheet1"]
        mock_excel.return_value = mock_excel_instance

        mock_get_last.return_value = [
            {"container_no": "CONT123", "departure_date": "2023-01-01", "name_of_cargo": "Кирпичи"}]
        mock_convert_csv.return_value = [
            {"container_no": "CONT123", "departure_date": "2023-01-01", "name_of_cargo": "Кирпичи"}]

        mock_divide.return_value = [
            [{"container_no": "CONT123", "departure_date": "2023-01-01", "name_of_cargo": "Кирпичи"}]]
        mock_convert_date.return_value = datetime(2023, 1, 1)
        mock_find.return_value = True

        rzhd_ktk_instance.main()
        mock_connect.assert_called_once()
        mock_get_ref.assert_called_once()
        mock_excel.assert_called_once()
        mock_get_last.assert_called_once()
        mock_convert_csv.assert_called_once()
        mock_divide.assert_called_once()
        mock_change_type.assert_called_once()
        mock_change_value.assert_called_once()
        mock_convert_date.assert_called_once()
        mock_find.assert_called_once()
        mock_client.query.assert_called()
        mock_save.assert_called_once()
