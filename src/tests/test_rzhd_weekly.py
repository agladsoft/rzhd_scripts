import os
import sys
from unittest.mock import MagicMock, patch
import pytest
from datetime import datetime, timedelta
sys.modules['src.scripts.app_logger'] = MagicMock()
from src.scripts.rzhd_weekly import RzhdWeekly, MyError

sys.modules['src.scripts.app_logger'] = MagicMock()
os.environ['XL_IDP_ROOT_RZHD'] = '/tmp'

LIST_VALUE_NOT_NULL = ["departure_day_report", "departure_date", "arrival_station"]
LIST_OF_FLOAT_TYPE = ["cost", "weight"]
LIST_OF_DATE_TYPE = ["departure_date", "arrival_date"]
LIST_OF_INT_TYPE = ["passengers", "wagons"]
LIST_SPLIT_MONTH = ["departure_month_year"]


@pytest.fixture(autouse=True)
def override_globals(monkeypatch):
    """Фикстура для переопределения глобальных констант в модуле"""
    monkeypatch.setattr("src.scripts.rzhd_weekly.LIST_VALUE_NOT_NULL", LIST_VALUE_NOT_NULL)
    monkeypatch.setattr("src.scripts.rzhd_weekly.LIST_OF_FLOAT_TYPE", LIST_OF_FLOAT_TYPE)
    monkeypatch.setattr("src.scripts.rzhd_weekly.LIST_OF_DATE_TYPE", LIST_OF_DATE_TYPE)
    monkeypatch.setattr("src.scripts.rzhd_weekly.LIST_OF_INT_TYPE", LIST_OF_INT_TYPE)
    monkeypatch.setattr("src.scripts.rzhd_weekly.LIST_SPLIT_MONTH", LIST_SPLIT_MONTH)


@pytest.fixture
def rzhd_weekly():
    """Фикстура для создания экземпляра RzhdWeekly для тестов"""
    rzhd = RzhdWeekly('test_file.xlsx', 'test_sheet')
    rzhd.convert_format_date = MagicMock(return_value=datetime(2023, 1, 1))
    rzhd.convert_to_float = MagicMock(return_value=100.5)
    rzhd.convert_to_int = MagicMock(return_value=10)
    rzhd.split_month_and_year = MagicMock()
    return rzhd


def test_check_is_null_value_none(rzhd_weekly):
    """Тест проверки на None значение"""
    with pytest.raises(MyError):
        rzhd_weekly.check_is_null_value("arrival_station", None)


def test_check_is_null_value_valid(rzhd_weekly):
    """Тест проверки на валидное значение"""
    rzhd_weekly.check_is_null_value("arrival_station", "Москва")


def test_check_is_null_value_date(rzhd_weekly):
    """Тест проверки даты отправления"""
    rzhd_weekly.check_is_null_value("departure_date", "01.01.2023")

    rzhd_weekly.convert_format_date.return_value = ""
    with pytest.raises(MyError):
        rzhd_weekly.check_is_null_value("departure_date", "invalid-date")


def test_change_type_valid_data(rzhd_weekly):
    """Тест преобразования типов данных с валидными значениями"""
    data = {
        "departure_date": "01.01.2023",
        "arrival_station": "Санкт-Петербург",
        "cost": "1500.50",
        "passengers": "150",
        "departure_month_year": "01.2023"
    }

    rzhd_weekly.change_type(data, 0)

    rzhd_weekly.convert_format_date.assert_called_with("01.01.2023")
    rzhd_weekly.convert_to_float.assert_called_with("1500.50")
    rzhd_weekly.convert_to_int.assert_called_with("150")
    rzhd_weekly.split_month_and_year.assert_called_with(data, "departure_month_year", "01.2023")

    assert data["departure_date"] == str(rzhd_weekly.convert_format_date.return_value)
    assert data["cost"] == rzhd_weekly.convert_to_float.return_value
    assert data["passengers"] == rzhd_weekly.convert_to_int.return_value


@patch('sys.exit')
@patch('sys.stderr')
@patch('src.scripts.rzhd_weekly.telegram')
def test_change_type_null_value(mock_telegram, mock_stderr, mock_exit, rzhd_weekly):
    """Тест обработки null значений с выходом из программы"""
    data = {
        "departure_date": None,
        "arrival_station": "Санкт-Петербург"
    }

    rzhd_weekly.change_type(data, 2)
    mock_telegram.assert_called_once()
    mock_stderr.write.assert_called()
    mock_exit.assert_called_once_with(1)


def test_change_type_exception_handling(rzhd_weekly):
    """Тест обработки исключений в методе change_type"""
    rzhd_weekly.convert_to_float.side_effect = ValueError("Test error")

    data = {
        "departure_date": "01.01.2023",
        "cost": "invalid"
    }
    rzhd_weekly.change_type(data, 0)
