import datetime
from pathlib import Path
import subprocess
import tempfile
from typing import Tuple, Union
import xlwt as xl
import pyarrow
from .util import assert_table_equals


def do_convert(
    xls_path: Path,
    *,
    max_rows: int = 99999,
    max_columns: int = 99998,
    max_bytes_per_value: int = 99997,
    max_bytes_total: int = 999999999,
    header_rows: str = "0-1",
    header_rows_file: str = "",
    include_stdout: bool = False
) -> Union[pyarrow.Table, Tuple[pyarrow.Table, bytes]]:
    with tempfile.NamedTemporaryFile(suffix=".arrow") as arrow_file:
        args = [
            "/usr/bin/xls-to-arrow",
            "--max-rows",
            str(max_rows),
            "--max-columns",
            str(max_columns),
            "--max-bytes-per-value",
            str(max_bytes_per_value),
            "--max-bytes-total",
            str(max_bytes_total),
            "--header-rows",
            header_rows,
            "--header-rows-file",
            header_rows_file,
            xls_path.as_posix(),
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
    with tempfile.NamedTemporaryFile(suffix=".xls") as xls_file:
        workbook.save(xls_file.name)
        return do_convert(Path(xls_file.name), **kwargs)


# This is hard to test, because xlwt won't write a workbook without a sheet
# def test_no_sheets_is_error():
#     # https://openpyxl.readthedocs.io/en/stable/optimized.html#write-only-mode
#     # ... to create a workbook with no worksheets
#     workbook = xl.Workbook()
#     result, stdout = do_convert_data(workbook, include_stdout=True)
#     assert_table_equals(result, pyarrow.table({}))
#     assert stdout == b"Excel file has no worksheets\n"


def test_empty_sheet():
    workbook = xl.Workbook()
    workbook.add_sheet('X')
    assert_table_equals(do_convert_data(workbook, header_rows=""), pyarrow.table({}))


def test_empty_sheet_no_header_row():
    workbook = xl.Workbook()
    workbook.add_sheet('X')
    assert_table_equals(do_convert_data(workbook, header_rows="0-1"), pyarrow.table({}))


def test_number_columns():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('A')
    sheet.write(0, 0, 1)
    sheet.write(0, 1, 1.1)
    sheet.write(1, 0, 2)
    sheet.write(1, 1, 2.2)
    sheet.write(2, 0, 3)
    sheet.write(2, 1, 3.3)
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
    sheet = workbook.add_sheet('A')
    sheet.write(0, 0, "a")
    sheet.write(1, 0, "b")
    assert_table_equals(
        do_convert_data(workbook, header_rows=""), pyarrow.table({"A": ["a", "b"]})
    )


def test_date_and_datetime_columns():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('A')
    # These dates are chosen specially -- double precision can't represent
    # every arbitrary number of microseconds accurately (let alone
    # nanoseconds), but the math happens to work for these datetimes.
    #
    # (We aren't testing rounding here.)
    fmt = xl.easyxf(num_format_str="dd-mmm-yyyy")
    sheet.write(0, 0, datetime.date(2020, 1, 25), fmt)
    sheet.write(0, 1, datetime.datetime(2020, 3, 4, 1, 25, 18), fmt)
    sheet.write(1, 0, datetime.date(2020, 1, 26), fmt)
    sheet.write(1, 1, 23123.2505327, fmt)  # tested in LibreOffice -- at least, to milliseconds
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
                        datetime.datetime(2020, 3, 4, 1, 25, 18),
                        datetime.datetime(1963, 4, 22, 6, 0, 46, 25280),
                    ],
                    pyarrow.timestamp("ns"),
                ),
            }
        ),
    )


def test_datetime_overflow():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    fmt = xl.easyxf(num_format_str="dd-mmm-yyyy")
    sheet.write(0, 0, datetime.date(1100, 1, 1), fmt)
    sheet.write(0, 1, datetime.date(1901, 1, 1), fmt)
    sheet.write(1, 0, datetime.date(1901, 1, 1), fmt)
    sheet.write(1, 1, datetime.date(3000, 1, 1), fmt)
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
    sheet = workbook.add_sheet('X')
    sheet.write(1, 0, 3.0)
    sheet.write(3, 0, 4.0)
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": [None, 3.0, None, 4.0]}),
    )


def test_skip_null_columns():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 3.0)
    sheet.write(0, 3, 4.0)
    sheet.write(1, 0, 3.0)
    sheet.write(1, 3, 4.0)
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
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 3.0)
    sheet.write(0, 1, 4.0)
    sheet.write(1, 1, 4.0)
    sheet.write(2, 1, 4.0)
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": [3.0, None, None], "B": [4.0, 4.0, 4.0]}),
    )


def test_bool_becomes_str():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, True)
    sheet.write(1, 0, False)
    result, stdout = do_convert_data(workbook, header_rows="", include_stdout=True)
    assert_table_equals(
        do_convert_data(workbook, header_rows=""),
        pyarrow.table({"A": ["TRUE", "FALSE"]}),
    )


def test_invalid_xls_file():
    with tempfile.NamedTemporaryFile(suffix=".xls") as tf:
        path = Path(tf.name)
        path.write_bytes(b"12345")
        result, stdout = do_convert(path, include_stdout=True)
    assert_table_equals(result, pyarrow.table({}))
    assert stdout == b"Invalid XLS file: error opening file\n"


def test_skip_rows():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'Hi')
    sheet.write(1, 0, 1.0)
    sheet.write(3, 0, 1.0)
    result, stdout = do_convert_data(
        workbook, max_rows=1, header_rows="0-1", include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"A": [1.0]}))
    assert stdout == b"skipped 2 rows (after row limit of 1)\n"


def test_skip_columns():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'a')
    sheet.write(0, 1, 'b')
    sheet.write(0, 2, 'c')
    result, stdout = do_convert_data(
        workbook, max_columns=1, header_rows="", include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"A": ["a"]}))
    assert stdout == b"skipped column B and more (after column limit of 1)\n"


def test_header_rows():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'ColA')
    sheet.write(0, 1, 'ColB')
    sheet.write(1, 0, 'a')
    sheet.write(1, 1, 'b')
    with tempfile.NamedTemporaryFile(suffix="-headers.arrow") as header_file:
        result = do_convert_data(
            workbook, header_rows="0-1", header_rows_file=header_file.name
        )
        with pyarrow.ipc.open_file(header_file.name) as header_reader:
            header_table = header_reader.read_all()
    assert_table_equals(result, pyarrow.table({"A": ["a"], "B": ["b"]}))
    assert_table_equals(header_table, pyarrow.table({"A": ["ColA"], "B": ["ColB"]}))


def test_header_rows_convert_to_str():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, datetime.date(2020, 1, 25), xl.easyxf(num_format_str="dd-mmm-yyyy"))
    sheet.write(0, 1, 123.4213)
    sheet.write(0, 2, 123.4213, xl.easyxf(num_format_str="#.00"))
    # Leave D1 blank
    # It'd be nice to set E1="", but xlwt treats "" as blank
    sheet.write(1, 0, 'a')
    sheet.write(1, 1, 'b')
    sheet.write(1, 2, 'c')
    sheet.write(1, 3, 'd')
    with tempfile.NamedTemporaryFile(suffix="-headers.arrow") as header_file:
        # ignore result
        do_convert_data(
            workbook, header_rows="0-1", header_rows_file=header_file.name
        )
        with pyarrow.ipc.open_file(header_file.name) as header_reader:
            header_table = header_reader.read_all()
    assert_table_equals(header_table, pyarrow.table({"A": ["25-Jan-2020"], "B": ["123.4213"], "C": ["123.42"], "D": pyarrow.array([None], pyarrow.utf8())}))


def test_header_truncated():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'xy1')
    sheet.write(0, 1, 'xy2')
    sheet.write(1, 0, 'a')
    sheet.write(1, 1, 'b')
    with tempfile.NamedTemporaryFile(suffix="-headers.arrow") as header_file:
        result, stdout = do_convert_data(
            workbook, max_bytes_per_value=2, header_rows="0-1", header_rows_file=header_file.name, include_stdout=True
        )
        with pyarrow.ipc.open_file(header_file.name) as header_reader:
            header_table = header_reader.read_all()
    assert_table_equals(result, pyarrow.table({"A": ["a"], "B": ["b"]}))
    assert_table_equals(header_table, pyarrow.table({"A": ["xy"], "B": ["xy"]}))
    assert stdout == b"".join(
        [
            b"truncated 2 values (value byte limit is 2; see row 0 column A)\n"
        ]
    )


def test_values_truncated():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'abcde')
    sheet.write(0, 1, 'fghijklmn')
    sheet.write(0, 2, 'opq')
    result, stdout = do_convert_data(
        workbook, max_bytes_per_value=3, header_rows="", include_stdout=True,
    )
    assert_table_equals(
        result, pyarrow.table({"A": ["abc"], "B": ["fgh"], "C": ["opq"]})
    )
    assert stdout == b"truncated 2 values (value byte limit is 3; see row 0 column A)\n"


def test_truncate_do_not_cause_invalid_utf8():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    for i, s in enumerate([
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
    ]):
        sheet.write(i, 0, s)

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
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 3.4)
    sheet.write(1, 0, 's')
    sheet.write(2, 0, -2.2)
    result, stdout = do_convert_data(workbook, header_rows="", include_stdout=True,)
    assert_table_equals(result, pyarrow.table({"A": ["3.4", "s", "-2.2"]}))
    assert stdout == b"interpreted 2 Numbers as String; see row 0 column A\n"


def test_stop_after_byte_total_limit():
    workbook = xl.Workbook()
    sheet = workbook.add_sheet('X')
    sheet.write(0, 0, 'abcd')
    sheet.write(0, 1, 'efgh')
    sheet.write(1, 0, 'ijkl')
    sheet.write(1, 1, 'mnop')
    result, stdout = do_convert_data(
        workbook, max_bytes_total=8, header_rows="", include_stdout=True,
    )
    assert_table_equals(result, pyarrow.table({"A": ["abcd"], "B": ["efgh"]}))
    assert stdout == b"stopped at limit of 8 bytes of data\n"
