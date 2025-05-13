import os
import sys
import json
import pytest
import pandas as pd
from unittest.mock import MagicMock
sys.modules['src.scripts.app_logger'] = MagicMock()
from src.scripts.rzhd import Rzhd
from datetime import datetime, timedelta

os.environ['XL_IDP_ROOT_RZHD'] = '/tmp'

HEADERS_ENG = {("Колонка1", "КЛ1"): "column1_eng"}

DATE_FORMATS = ["%d.%m.%Y", "%Y-%m-%d"]
DICT_FORMAT_AND_ENGINE = {"xlsx": "openpyxl"}
LIST_SPLIT_MONTH = ["split_month"]
LIST_OF_FLOAT_TYPE = ["float_key"]
LIST_OF_DATE_TYPE = ["date_key"]
LIST_OF_INT_TYPE = ["int_key"]


@pytest.fixture(autouse=True)
def override_globals(monkeypatch):
    monkeypatch.setattr("src.scripts.rzhd.HEADERS_ENG", HEADERS_ENG)
    monkeypatch.setattr("src.scripts.rzhd.DATE_FORMATS", DATE_FORMATS)
    monkeypatch.setattr("src.scripts.rzhd.DICT_FORMAT_AND_ENGINE", DICT_FORMAT_AND_ENGINE)
    monkeypatch.setattr("src.scripts.rzhd.LIST_SPLIT_MONTH", LIST_SPLIT_MONTH)
    monkeypatch.setattr("src.scripts.rzhd.LIST_OF_FLOAT_TYPE", LIST_OF_FLOAT_TYPE)
    monkeypatch.setattr("src.scripts.rzhd.LIST_OF_DATE_TYPE", LIST_OF_DATE_TYPE)
    monkeypatch.setattr("src.scripts.rzhd.LIST_OF_INT_TYPE", LIST_OF_INT_TYPE)


def test_convert_to_float():
    assert Rzhd.convert_to_float("123,45") == 123.45
    assert Rzhd.convert_to_float("#") is None
    assert Rzhd.convert_to_float(None) is None
    with pytest.raises(AssertionError):
        Rzhd.convert_to_float("abc")


def test_convert_to_int():
    assert Rzhd.convert_to_int("123") == 123
    assert Rzhd.convert_to_int("#") is None
    assert Rzhd.convert_to_int(None) is None
    with pytest.raises(AssertionError):
        Rzhd.convert_to_int("12.3")


def test_divide_chunks():
    data = [1, 2, 3, 4, 5]
    chunks = list(Rzhd.divide_chunks(data, 2))
    assert chunks == [[1, 2], [3, 4], [5]]


def test_split_month_and_year():
    data = {}
    Rzhd.split_month_and_year(data, "test_month", "2020/12")
    assert data["test_month"] == 12
    assert data["test_year"] == 2020


def test_convert_xlsx_datetime_to_date():
    result = Rzhd.convert_xlsx_datetime_to_date(1.0)
    expected_date = (datetime(1899, 12, 30) + timedelta(days=1)).strftime("%Y-%m-%d")
    assert result == expected_date


def test_convert_format_date():
    rzhd = Rzhd("dummy.xlsx", "dummy_folder")
    result = rzhd.convert_format_date("15.03.2020")
    assert result.strftime("%Y-%m-%d") == "2020-03-15"
    result2 = rzhd.convert_format_date("2020-03-15")
    assert result2.strftime("%Y-%m-%d") == "2020-03-15"
    with pytest.raises(AssertionError):
        rzhd.convert_format_date("invalid-date")


def test_save_data_to_file(tmp_path, monkeypatch):
    rzhd = Rzhd("dummy.xlsx", str(tmp_path))
    chunk_data = [{"key": "value"}]
    sheet = "Sheet1"
    captured = {}

    def fake_open(file, mode, encoding):
        class FakeFile:
            def __init__(self):
                self.data = ""

            def write(self, data):
                self.data += data

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                captured['data'] = self.data

        return FakeFile()

    monkeypatch.setattr("builtins.open", fake_open)
    rzhd.save_data_to_file(0, chunk_data, sheet)
    assert 'data' in captured
    data_written = json.loads(captured['data'])
    assert data_written == chunk_data


def test_main(monkeypatch):
    dummy_sheet_names = ["Sheet1", "Sheet2"]

    class DummyExcelFile:
        def __init__(self, filename):
            self.sheet_names = dummy_sheet_names

    monkeypatch.setattr("src.scripts.rzhd.ExcelFile", DummyExcelFile)

    dummy_df = pd.DataFrame({
        "Колонка1": ["value1", "value2"],
        "Колонка2": ["value3", "value4"]
    })

    def fake_read_excel(filename, sheet_name, engine, dtype):
        return dummy_df.copy()

    monkeypatch.setattr("src.scripts.rzhd.read_excel", fake_read_excel)
    calls = []

    def fake_save_data_to_file(self, i, chunk_data, sheet):
        calls.append((i, chunk_data, sheet))

    monkeypatch.setattr(Rzhd, "save_data_to_file", fake_save_data_to_file)
    rzhd = Rzhd("dummy.xlsx", "dummy_folder")
    rzhd.main()
    assert len(calls) >= len(dummy_sheet_names)
