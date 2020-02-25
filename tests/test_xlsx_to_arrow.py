import datetime
from pathlib import Path
import subprocess
import tempfile
from typing import Tuple, Union
import openpyxl as xl
import pyarrow
from .util import assert_table_equals


def do_convert(
    xlsx_path: Path,
    *,
    max_rows: int = 99999,
    max_columns: int = 99998,
    max_bytes_per_value: int = 99997,
    max_bytes_total: int = 999999999,
    max_bytes_per_column_name: int = 99,
    header_rows: str = "0-1",
    include_stdout: bool = False
) -> Union[pyarrow.Table, Tuple[pyarrow.Table, bytes]]:
    with tempfile.NamedTemporaryFile(suffix=".arrow") as arrow_file:
        args = [
            "/usr/bin/xlsx-to-arrow",
            "--max-rows",
            str(max_rows),
            "--max-columns",
            str(max_columns),
            "--max-bytes-per-value",
            str(max_bytes_per_value),
            "--max-bytes-total",
            str(max_bytes_total),
            "--max-bytes-per-column-name",
            str(max_bytes_per_column_name),
            "--header-rows",
            header_rows,
            xlsx_path.as_posix(),
            Path(arrow_file.name).as_posix(),
        ]
        try:
            result = subprocess.run(
                args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
            )
        except subprocess.CalledProcessError as err:
            # Rewrite error so it's easy to read in test-result stack trace
            raise RuntimeError(
                "Process failed with code %d: %s"
                % (
                    err.returncode,
                    (
                        err.stdout.decode("utf-8", errors="replace")
                        + err.stderr.decode("utf-8", errors="replace")
                    ),
                )
            ) from None

        assert result.stderr == b""
        result_reader = pyarrow.ipc.open_file(arrow_file.name)
        table = result_reader.read_all()
        if include_stdout:
            return table, result.stdout
        else:
            assert result.stdout == b""
            return table


def do_convert_data(
    workbook: xl.Workbook, **kwargs
) -> Union[pyarrow.Table, Union[pyarrow.Table, str]]:
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as xlsx_file:
        workbook.save(filename=xlsx_file.name)
        return do_convert(Path(xlsx_file.name), **kwargs)


# This is hard to test, since it's really an invalid Excel file
# def test_no_sheets_is_error():
#     # https://openpyxl.readthedocs.io/en/stable/optimized.html#write-only-mode
#     # ... to create a workbook with no worksheets
#     workbook = xl.Workbook()
#     workbook.remove(workbook.active)
#     workbook.get_active_sheet = lambda: None
#     result, stdout = do_convert_data(workbook, include_stdout=True)
#     assert_table_equals(result, pyarrow.table({}))
#     assert stdout == b"Excel file has no worksheets\n"


def test_empty_sheet():
    workbook = xl.Workbook()
    assert_table_equals(do_convert_data(workbook, header_rows=""), pyarrow.table({}))


def test_empty_sheet_no_header_row():
    workbook = xl.Workbook()
    assert_table_equals(do_convert_data(workbook, header_rows="0-1"), pyarrow.table({}))


def test_number_columns():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append([1, 1.1])
    sheet.append([2, 2.2])
    sheet.append([3, 3.3])
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": [1.0, 2.0, 3.0], "B": [1.1, 2.2, 3.3]}),
    )


# openpyxl doesn't write shared strings
# def test_shared_string_column():
#     workbook = xl.Workbook()
#     sheet = workbook.active
#     sheet.append(["a"])
#     sheet.append(["b"])
#     assert_table_equals(
#         do_convert_data(workbook, header_rows=""),
#         pyarrow.table({"A": ["a", "b"]})
#     )


def test_inline_str_column():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append(["a"])
    sheet.append(["b"])
    assert_table_equals(
        do_convert_data(workbook, header_rows=""), pyarrow.table({"A": ["a", "b"]})
    )


def test_date_and_datetime_columns():
    workbook = xl.Workbook()
    sheet = workbook.active
    # These dates are chosen specially -- double precision can't represent
    # every arbitrary number of microseconds accurately (let alone
    # nanoseconds), but the math happens to work for these datetimes.
    #
    # (We aren't testing rounding here.)
    sheet.append(
        [datetime.date(2020, 1, 25), datetime.datetime(2020, 1, 25, 17, 25, 30, 128000)]
    )
    sheet.append(
        [datetime.date(2020, 1, 26), datetime.datetime(2020, 1, 25, 17, 26, 27, 256)]
    )
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table(
            {
                "A": pyarrow.array(
                    [datetime.datetime(2020, 1, 25), datetime.datetime(2020, 1, 26)],
                    pyarrow.timestamp("ns"),
                ),
                "B": pyarrow.array(
                    [
                        datetime.datetime(2020, 1, 25, 17, 25, 30, 128000),
                        datetime.datetime(2020, 1, 25, 17, 26, 27, 256),
                    ],
                    pyarrow.timestamp("ns"),
                ),
            }
        ),
    )


def test_datetetime_overflow():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append([datetime.date(1100, 1, 1), datetime.date(1901, 1, 1)])
    sheet.append([datetime.date(1901, 1, 1), datetime.date(3000, 1, 1)])
    result, stdout = do_convert_data(workbook, include_stdout=True, header_rows="")
    assert_table_equals(
        result,
        pyarrow.table(
            {
                "A": pyarrow.array(
                    [None, datetime.datetime(1901, 1, 1)], pyarrow.timestamp("ns")
                ),
                "B": pyarrow.array(
                    [datetime.datetime(1901, 1, 1), None], pyarrow.timestamp("ns")
                ),
            }
        ),
    )
    assert (
        stdout
        == b"replaced out-of-range with null for 2 Timestamps; see row 0 column A\n"
    )


def test_skip_null_values():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet["A2"] = 3.0
    sheet["A4"] = 4.0
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": [None, 3.0, None, 4.0]}),
    )


def test_skip_null_columns():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet["A1"] = 3.0
    sheet["A2"] = 3.0
    sheet["D1"] = 4.0
    sheet["D2"] = 4.0
    result, stdout = do_convert_data(workbook, header_rows="", include_stdout=True)
    assert_table_equals(
        result,
        pyarrow.table(
            {
                "A": [3.0, 3.0],
                "B": pyarrow.array([None, None], pyarrow.utf8()),
                "C": pyarrow.array([None, None], pyarrow.utf8()),
                "D": [4.0, 4.0],
            }
        ),
    )
    assert stdout == b"chose string type for null column B and more\n"


def test_backfill_column_at_end():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet["A1"] = 3.0
    sheet["B1"] = 4.0
    sheet["B2"] = 4.0
    sheet["B3"] = 4.0
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": [3.0, None, None], "B": [4.0, 4.0, 4.0]}),
    )


def test_bool_becomes_str():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append([True])
    sheet.append([False])
    result, stdout = do_convert_data(workbook, header_rows="", include_stdout=True)
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": ["TRUE", "FALSE"]}),
    )


def test_invalid_zipfile():
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tf:
        path = Path(tf.name)
        path.write_bytes(b"12345")
        result, stdout = do_convert(path, include_stdout=True)
    assert_table_equals(result, pyarrow.table({}))
    assert stdout == b"Invalid XLSX file: xlnt::exception : failed to find zip header\n"


def test_skip_rows():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet["A1"] = "Hi"
    sheet["A2"] = 1.0
    sheet["A4"] = 1.0
    result, stdout = do_convert_data(
        workbook, max_rows=1, header_rows="0-1", include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"Hi": [1.0]}))
    assert stdout == b"skipped 2 rows (after row limit of 1)\n"


def test_skip_columns():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet["A1"] = "a"
    sheet["B1"] = "b"
    sheet["C1"] = "c"
    result, stdout = do_convert_data(
        workbook, max_columns=1, header_rows="", include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"A": ["a"]}))
    assert stdout == b"skipped column B and more (after column limit of 1)\n"


def test_column_name_truncated():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append(["xy1", "xy2"])
    sheet.append(["a", "b"])
    result, stdout = do_convert_data(
        workbook, max_bytes_per_column_name=2, header_rows="0-1", include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"xy": ["a"]}))
    assert stdout == b"".join(
        [
            b"truncated 2 column names; example xy\n"
            b"ignored duplicate column xy starting at row 0\n"
        ]
    )


def test_column_name_truncated_only_first_row():
    # v0.0.8: accidentally truncated all values, not just colnames
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append(["a", "b"])
    sheet.append(["xy1", "xy2"])
    assert_table_equals(
        do_convert_data(workbook, max_bytes_per_column_name=2, header_rows="0-1"),
        pyarrow.table({"a": ["xy1"], "b": ["xy2"]}),
    )


def test_column_name_invalid():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append(["A", "B\tC"])
    sheet.append(["a", "b"])
    result, stdout = do_convert_data(workbook, header_rows="0-1", include_stdout=True)
    assert_table_equals(result, pyarrow.table({"A": ["a"]}))
    assert stdout == b'ignored invalid column "B\\tC"\n'


def test_column_name_duplicated():
    workbook = xl.Workbook()
    sheet = workbook.active
    sheet.append(["A", "A", "A", "B", "B"])
    sheet.append(["x", "y", "z", "a", "b"])
    result, stdout = do_convert_data(workbook, header_rows="0-1", include_stdout=True)
    assert_table_equals(result, pyarrow.table({"A": ["x"], "B": ["a"]}))
    assert stdout == b"ignored duplicate column A and more starting at row 0\n"


def test_values_truncated():
    workbook = xl.Workbook()
    workbook.active.append(["abcde", "fghijklmn", "opq"])
    result, stdout = do_convert_data(
        workbook, max_bytes_per_value=3, header_rows="", include_stdout=True,
    )
    assert_table_equals(
        result, pyarrow.table({"A": ["abc"], "B": ["fgh"], "C": ["opq"]})
    )
    assert stdout == b"truncated 2 values (value byte limit is 3; see row 0 column A)\n"


def test_truncate_do_not_cause_invalid_utf8():
    workbook = xl.Workbook()
    for s in [
        # Examples from https://en.wikipedia.org/wiki/UTF-8
        "AAAA",
        "AA\u00A2",  # ¢ (2 bytes) -- keep
        "AAA\u00A2",  # ¢ (2 bytes) -- drop both bytes
        "A\u0939",  # ह (3 bytes) -- keep
        "AA\u0939",  # ह (3 bytes) -- drop all three bytes
        "AAA\u0939",  # ह (3 bytes) -- drop all three bytes
        "\U00010348",  # 𐍈 (4 bytes) -- keep
        "A\U00010348",  # 𐍈 (4 bytes) -- drop all four bytes
        "AA\U00010348",  # 𐍈 (4 bytes) -- drop all four bytes
        "AAA\U00010348",  # 𐍈 (4 bytes) -- drop all four bytes
    ]:
        workbook.active.append([s])

    result, stdout = do_convert_data(
        workbook, max_bytes_per_value=4, header_rows="", include_stdout=True,
    )
    expected = pyarrow.table(
        {
            "A": [
                "AAAA",
                "AA\u00A2",
                "AAA",
                "A\u0939",
                "AA",
                "AAA",
                "\U00010348",
                "A",
                "AA",
                "AAA",
            ]
        }
    )
    assert_table_equals(result, expected)
    assert stdout == b"truncated 6 values (value byte limit is 4; see row 2 column A)\n"


def test_convert_float_to_string_and_report():
    workbook = xl.Workbook()
    workbook.active["A1"] = 3.4
    workbook.active["A2"] = "s"
    workbook.active["A3"] = -2.2
    result, stdout = do_convert_data(workbook, header_rows="", include_stdout=True,)
    assert_table_equals(result, pyarrow.table({"A": ["3.4", "s", "-2.2"]}))
    assert stdout == b"interpreted 2 Numbers as String; see row 0 column A\n"


def test_stop_after_byte_total_limit():
    workbook = xl.Workbook()
    workbook.active.append(["abcd", "efgh"])
    workbook.active.append(["ijkl", "mnop"])
    result, stdout = do_convert_data(
        workbook, max_bytes_total=8, header_rows="", include_stdout=True,
    )
    assert_table_equals(result, pyarrow.table({"A": ["abcd"], "B": ["efgh"]}))
    assert stdout == b"stopped at limit of 8 bytes of data\n"
