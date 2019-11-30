import json
from pathlib import Path
import subprocess
import tempfile
from typing import Tuple, Union
import pyarrow
from .util import assert_table_equals


def do_convert(
    json_path: Path,
    *,
    max_rows: int = 99999,
    max_columns: int = 99998,
    max_bytes_per_value: int = 99997,
    max_bytes_total: int = 999999999,
    max_bytes_per_error_value: int = 100,
    max_bytes_per_column_name: int = 99,
    include_stdout: bool = False
) -> Union[pyarrow.Table, Tuple[pyarrow.Table, bytes]]:
    with tempfile.NamedTemporaryFile(suffix=".arrow") as arrow_file:
        args = [
            "/usr/bin/json-to-arrow",
            "--max-rows",
            str(max_rows),
            "--max-columns",
            str(max_columns),
            "--max-bytes-per-value",
            str(max_bytes_per_value),
            "--max-bytes-total",
            str(max_bytes_total),
            "--max-bytes-per-error-value",
            str(max_bytes_per_error_value),
            "--max-bytes-per-column-name",
            str(max_bytes_per_column_name),
            json_path.as_posix(),
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
    s: Union[str, bytes], **kwargs
) -> Union[pyarrow.Table, Union[pyarrow.Table, str]]:
    if isinstance(s, str):
        s = s.encode("utf-8")
    with tempfile.NamedTemporaryFile(suffix=".json") as json_file:
        json_path = Path(json_file.name)
        json_path.write_bytes(s)
        return do_convert(json_path, **kwargs)


def test_root_is_record_array():
    assert_table_equals(
        do_convert_data('[{"x": "a", "y": "b"}, {"x": "c", "y": "d"}]'),
        pyarrow.table({"x": ["a", "c"], "y": ["b", "d"]}),
    )


def test_int8():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": -2}, {"x": 3}]'),
        pyarrow.table({"x": pyarrow.array([1, -2, 3], pyarrow.int8())}),
    )


def test_int16():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": -200}, {"x": 3}]'),
        pyarrow.table({"x": pyarrow.array([1, -200, 3], pyarrow.int16())}),
    )


def test_int32():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": -2000000}, {"x": 3}]'),
        pyarrow.table({"x": pyarrow.array([1, -2000000, 3], pyarrow.int32())}),
    )


def test_int64():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": -2000000}, {"x": 35184372088832}]'),
        pyarrow.table(
            {"x": pyarrow.array([1, -2000000, 35184372088832], pyarrow.int64())}
        ),
    )


def test_float():
    assert_table_equals(
        do_convert_data('[{"x": 1.1}, {"x": -2.2}, {"x": 3.3}]'),
        pyarrow.table({"x": pyarrow.array([1.1, -2.2, 3.3], pyarrow.float64())}),
    )


def test_float_overflow():
    # rapidjson 1.1.0 is broken in this manner. Fixed on master.
    # https://github.com/Tencent/rapidjson/issues/1368
    result, stdout = do_convert_data(
        '[{"x": 1.1}, {"x": 1.1e500}]', include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"x": [1.1]}))
    assert (
        stdout
        == b"JSON parse error at byte 19: Number too big to be stored in double.\n"
    )
    # should be:
    # assert_table_equals(result, pyarrow.table({"x": [1.1, None]}))
    # assert stdout == b"replaced infinity with null for 1 Numbers; see row 1 column x\n"


def test_convert_int_to_float():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": -2.2}]'),
        pyarrow.table({"x": pyarrow.array([1.0, -2.2], pyarrow.float64())}),
    )


def test_int_column_with_null():
    assert_table_equals(
        do_convert_data('[{"x": 1}, {"x": null}, {"x": 2}]'),
        pyarrow.table({"x": pyarrow.array([1, None, 2], pyarrow.int8())}),
    )


def test_float_column_with_null():
    assert_table_equals(
        do_convert_data('[{"x": 1.1}, {"x": null}, {"x": 2.2}]'),
        pyarrow.table({"x": pyarrow.array([1.1, None, 2.2], pyarrow.float64())}),
    )


def test_string_column_with_null():
    assert_table_equals(
        do_convert_data('[{"x": "a"}, {"x": null}, {"x": "c"}]'),
        pyarrow.table({"x": ["a", None, "c"]}),
    )


def test_null_column_to_int():
    assert_table_equals(
        do_convert_data('[{"x": null}, {"x": 1}, {"x": 2}]'),
        pyarrow.table({"x": pyarrow.array([None, 1, 2], pyarrow.int8())}),
    )


def test_backfill_str_column_at_start():
    assert_table_equals(
        do_convert_data('[{"x": "a"}, {"x": "b", "y": "c"}]'),
        pyarrow.table({"x": ["a", "b"], "y": [None, "c"]}),
    )


def test_backfill_int_column_at_start():
    assert_table_equals(
        do_convert_data('[{"x": "a"}, {"x": "b", "y": 1}]'),
        pyarrow.table({"x": ["a", "b"], "y": pyarrow.array([None, 1], pyarrow.int8())}),
    )


def test_backfill_float_column_at_start():
    assert_table_equals(
        do_convert_data('[{"x": "a"}, {"x": "b", "y": 1.1}]'),
        pyarrow.table({"x": ["a", "b"], "y": [None, 1.1]}),
    )


def test_fill_str_column_at_end():
    assert_table_equals(
        do_convert_data('[{"x": "a", "y": "b"}, {"x": "c"}]'),
        pyarrow.table({"x": ["a", "c"], "y": ["b", None]}),
    )


def test_fill_int_column_at_end():
    assert_table_equals(
        do_convert_data('[{"x": "a", "y": 1}, {"x": "c"}]'),
        pyarrow.table({"x": ["a", "c"], "y": pyarrow.array([1, None], pyarrow.int8())}),
    )


def test_fill_float_column_at_end():
    assert_table_equals(
        do_convert_data('[{"x": "a", "y": 1.1}, {"x": "c"}]'),
        pyarrow.table({"x": ["a", "c"], "y": [1.1, None]}),
    )


def test_bool_becomes_str():
    assert_table_equals(
        do_convert_data('[{"x": true}, {"x": false}]'),
        pyarrow.table({"x": ["true", "false"]}),
    )


def test_object_becomes_json_str():
    assert_table_equals(
        do_convert_data('[{"x": {"foo": "bar"}}]'),
        pyarrow.table({"x": ['{"foo":"bar"}']}),
    )


def test_json_serialize_special_characters():
    assert_table_equals(
        do_convert_data(r'[{"x": {"s": "\u0000\n\r\t\" \u001f"}}]'),
        pyarrow.table({"x": [r'{"s":"\u0000\n\r\t\" \u001F"}']}),
    )


def test_json_serialize_commas():
    assert_table_equals(
        do_convert_data(
            json.dumps(
                [
                    {
                        "x": [
                            [True, False],
                            [None, None],
                            [1.1, 2.2],
                            ["x", "y"],
                            [{"x": [], "y": []}, {}, []],
                        ]
                    }
                ]
            )
        ),
        pyarrow.table(
            {
                "x": [
                    '[[true,false],[null,null],[1.1,2.2],["x","y"],[{"x":[],"y":[]},{},[]]]'
                ]
            }
        ),
    )


def test_root_is_null():
    result, stdout = do_convert_data("null", include_stdout=True)
    assert_table_equals(result, pyarrow.table({}))
    assert stdout == b"JSON is not an Array or Object containing an Array; got: null\n"


def test_bool_is_null():
    result, stdout = do_convert_data("true", include_stdout=True)
    assert_table_equals(result, pyarrow.table({}))
    assert stdout == b"JSON is not an Array or Object containing an Array; got: true\n"


def test_find_record_array_in_object():
    assert_table_equals(
        do_convert_data(
            '{"metadata": {"date":"now", "garbage": [{"x": "no!"}]}, "records": [{"x": "a"}]}'
        ),
        pyarrow.table({"x": ["a"]}),
    )


def test_json_parse_error():
    result, stdout = do_convert_data('[{"x": "y"}, "no good]', include_stdout=True)
    assert_table_equals(result, pyarrow.table({"x": ["y"]}))
    assert (
        stdout
        == b"JSON parse error at byte 22: Missing a closing quotation mark in string.\n"
    )


def test_bad_surrogate_is_json_parse_error():
    result, stdout = do_convert_data(r'[{"x": "\udc00\ud800"}]', include_stdout=True)
    assert_table_equals(result, pyarrow.table({"x": pyarrow.array([], pyarrow.utf8())}))
    assert stdout == b"".join(
        [
            b"JSON parse error at byte 14: The surrogate pair in string is invalid.\n",
            b"chose string type for null column x\n",
        ]
    )


def test_truncate_error_str():
    result, stdout = do_convert_data(
        '["1234567890"]', max_bytes_per_error_value=4, include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({}))
    assert stdout == b'skipped 1 non-Object records; example Array item 0: "123\n'


def test_skip_rows():
    result, stdout = do_convert_data(
        '[{"x": "a"}, {"x": "b"}, {"x": "c"}]', max_rows=1, include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"x": "a"}))
    assert stdout == b"skipped 2 rows (after row limit of 1)\n"


def test_row_invalid_record_is_array():
    result, stdout = do_convert_data('[[1, {"x": ["y"]}, 4]]', include_stdout=True)
    assert_table_equals(result, pyarrow.table({}))
    assert (
        stdout
        == b'skipped 1 non-Object records; example Array item 0: [1{"x":["y"]},4]\n'
    )


def test_skip_columns():
    result, stdout = do_convert_data(
        '[{"x":"a", "y": "b", "z": "c", "Z": "C"},{"Z": "D", "z": "d", "x": "e", "y": "f"}]',
        max_columns=2,
        include_stdout=True,
    )
    assert_table_equals(result, pyarrow.table({"x": ["a", "e"], "y": ["b", "f"]}))
    assert stdout == b"skipped column z and more (after column limit of 2)\n"


def test_null_column_warn_and_choose_str():
    result, stdout = do_convert_data('[{"x": null}]', include_stdout=True)
    assert_table_equals(
        result, pyarrow.table({"x": pyarrow.array([None], pyarrow.utf8())})
    )
    assert stdout == b"chose string type for null column x\n"


def test_column_name_truncated():
    result, stdout = do_convert_data(
        '[{"xy1": "x", "xy2": "y"}]', max_bytes_per_column_name=2, include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"xy": ["x"]}))
    assert stdout == b"".join(
        [
            b"truncated 1 column names; example xy\n"
            b"ignored duplicate column xy starting at row 0\n"
        ]
    )


def test_column_name_invalid():
    result, stdout = do_convert_data(
        '[{"A": "x", "\\n":"y", "": "z"}]', include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"A": ["x"]}))
    assert stdout == b'ignored invalid column "\\n" and more\n'


def test_column_name_duplicated():
    result, stdout = do_convert_data(
        '[{"A": "x", "A": "y", "A": "z", "B": "a", "B": "b"}]', include_stdout=True
    )
    assert_table_equals(result, pyarrow.table({"A": ["x"], "B": ["a"]}))
    assert stdout == b"ignored duplicate column A and more starting at row 0\n"


def test_values_truncated():
    result, stdout = do_convert_data(
        '[{"A": "abcde", "B": "fghijklmn", "C": "opq"}]',
        max_bytes_per_value=3,
        include_stdout=True,
    )
    assert_table_equals(
        result, pyarrow.table({"A": ["abc"], "B": ["fgh"], "C": ["opq"]})
    )
    assert stdout == b"truncated 2 values (value byte limit is 3; see row 0 column A)\n"


def test_truncate_do_not_cause_invalid_utf8():
    result, stdout = do_convert_data(
        json.dumps(
            [
                {"A": s}
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
                ]
            ]
        ),
        max_bytes_per_value=4,
        include_stdout=True,
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


def test_convert_int64_to_float_and_report_lossy():
    result, stdout = do_convert_data(
        '[{"x": 1152921504606846977}, {"x": -2.2}, {"x": 1152921504606846978}]',
        include_stdout=True,
    )
    assert_table_equals(
        result,
        pyarrow.table(
            {
                "x": pyarrow.array(
                    [float(1152921504606846976), -2.2, float(1152921504606846976)],
                    pyarrow.float64(),
                )
            }
        ),
    )
    assert (
        stdout
        == b"lost precision converting 2 int64 Numbers to float64; see row 0 column x\n"
    )


def test_convert_int64_to_string_and_report():
    result, stdout = do_convert_data(
        '[{"x": 1152921504606846977}, {"x": "s"}, {"x": -2.2}]', include_stdout=True
    )
    assert_table_equals(
        result, pyarrow.table({"x": ["1152921504606846977", "s", "-2.2"]})
    )
    assert stdout == b"interpreted 2 Numbers as String; see row 0 column x\n"


def test_convert_float_to_string_and_report():
    result, stdout = do_convert_data(
        '[{"x": 11529215046061312413846977.123}, {"x": "s"}, {"x": -2.2}]',
        include_stdout=True,
    )
    assert_table_equals(
        result, pyarrow.table({"x": ["11529215046061312413846977.123", "s", "-2.2"]})
    )
    assert stdout == b"interpreted 2 Numbers as String; see row 0 column x\n"


def test_stop_after_byte_total_limit():
    result, stdout = do_convert_data(
        '[{"x": "abcd", "y": "efgh"}, {"x": "ijkl", "y": "mnop"}]',
        max_bytes_total=8,
        include_stdout=True,
    )
    assert_table_equals(result, pyarrow.table({"x": ["abcd"], "y": ["efgh"]}))
    assert stdout == b"stopped at limit of 8 bytes of data\n"
