import math
from pathlib import Path
import struct
import subprocess
from typing import Dict, Optional, Tuple
import numpy as np
import pyarrow as pa
from .util import arrow_file


ALL_CHECKS = {
    "utf8": True,
    "offsets-dont-overflow": True,
    "floats-all-finite": True,
    "dictionary-values-all-used": True,
    "dictionary-values-not-null": True,
    "dictionary-values-unique": True,
    "column-name-control-characters": True,
    "column-name-max-bytes": 100,
}


def validate(
    arrow_path: Path, checks: Dict[str, bool] = {}
) -> Optional[Tuple[str, str]]:
    """
    arrow-validate with `checks`; return None on exit=0, (stdout, stderr) otherwise.
    """
    args = ["/usr/bin/arrow-validate", arrow_path]
    for check, value in checks.items():
        if isinstance(value, bool):
            if value:
                args.append(f"--check-{check}")
            else:
                args.append(f"--nocheck-{check}")
        else:
            args.append(f"--check-{check}")
            args.append(str(value))
    result = subprocess.run(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
    )
    if result.returncode == 0:
        assert result.stdout == ""
        assert result.stderr == ""
        return None
    else:
        assert result.returncode == 1
        return (result.stdout, result.stderr)


def test_check_offsets_dont_overflow_string_array():
    array = pa.StringArray.from_buffers(
        2,
        # value_offsets: last byte of data is 9.
        pa.py_buffer(struct.pack("III", 0, 1, 9)),
        # data: 8 bytes of data. (Flatbuffers always align to 8-byte
        # boundaries.)
        pa.py_buffer(b"abcdefgh"),
    )
    table = pa.table({"A": array})
    with arrow_file(table) as path:
        assert validate(path, {"offsets-dont-overflow": True}) == (
            "--check-offsets-dont-overflow failed on column A\n",
            "",
        )


def test_check_utf8_valid_string_array():
    table = pa.table({"A": ["Montréal", None, ""]})
    with arrow_file(table) as path:
        assert validate(path, {"utf8": True}) is None


def test_check_utf8_invalid_string_array():
    array = pa.StringArray.from_buffers(
        1,
        # value_offsets: first item spans buffer offsets 0 to 1
        pa.py_buffer(struct.pack("II", 0, 1)),
        # data: a not-UTF8-safe character
        pa.py_buffer(b"\xc9"),
    )
    table = pa.table({"A": array})
    with arrow_file(table) as path:
        assert validate(path, {"utf8": True}) == (
            "--check-utf8 failed on column A\n",
            "",
        )


def test_check_utf8_valid_dict_values():
    table = pa.table({"A": pa.array(["Montréal", None, ""]).dictionary_encode()})
    with arrow_file(table) as path:
        assert validate(path, {"utf8": True}) is None


def test_check_utf8_invalid_dict_values():
    array = pa.StringArray.from_buffers(
        1,
        # value_offsets: first item spans buffer offsets 0 to 1
        pa.py_buffer(struct.pack("II", 0, 1)),
        # data: a not-UTF8-safe character
        pa.py_buffer(b"\xc9"),
    )
    table = pa.table({"A": array.dictionary_encode()})
    with arrow_file(table) as path:
        assert validate(path, {"utf8": True}) == (
            "--check-utf8 failed on column A\n",
            "",
        )


def test_check_utf8_invalid_column_name():
    table = pa.table({b"b\xc9ad": [1, 2, 3]})
    with arrow_file(table) as path:
        assert validate(path, {"utf8": True}) == (
            "--check-utf8 failed on a column name\n",
            "",
        )


def test_check_column_name_control_characters_valid():
    table = pa.table({"ééé": [1, 2]})
    with arrow_file(table) as path:
        assert validate(path, {"column-name-control-characters": True}) is None


def test_check_column_name_control_characters_invalid():
    table = pa.table({"a\nb": [1, 2]})
    with arrow_file(table) as path:
        assert validate(path, {"column-name-control-characters": True}) == (
            "--check-column-name-control-characters failed on a column name\n",
            "",
        )


def test_check_column_name_max_length():
    table = pa.table({"ABCDEFGHIJKLMNOP": [1, 2]})
    with arrow_file(table) as path:
        assert validate(path, {"column-name-max-bytes": "10"}) == (
            "--check-column-name-max-bytes=10 failed on column ABCDEFGHIJKLMNOP\n",
            "",
        )


def test_dictionary_values_all_used_valid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 2, 1], pa.int32()), pa.array(["A", "B", "C"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_dictionary_values_all_used_invalid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 2, 2], pa.int32()), pa.array(["A", "B", "C"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, {"dictionary-values-all-used": True}) == (
            "--check-dictionary-values-all-used failed on column A\n",
            "",
        )


def test_dictionary_values_all_used_all_null_indices_valid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([None, None, None], pa.int32()), pa.array([], pa.utf8())
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_dictionary_values_all_used_all_null_indices_invalid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([None, None, None], pa.int32()), pa.array(["A"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, {"dictionary-values-all-used": True}) == (
            "--check-dictionary-values-all-used failed on column A\n",
            "",
        )


def test_dictionary_values_not_null_valid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 1], pa.int32()), pa.array(["A", "B"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_dictionary_values_not_null_invalid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 1], pa.int32()), pa.array(["A", "B", None])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, {"dictionary-values-not-null": True}) == (
            "--check-dictionary-values-not-null failed on column A\n",
            "",
        )


def test_dictionary_values_unique_valid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, 2], pa.int32()), pa.array(["A", "B", "C"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_dictionary_values_unique_invalid():
    table = pa.table(
        {
            "A": pa.DictionaryArray.from_arrays(
                pa.array([0, 1, 2], pa.int32()), pa.array(["A", "B", "A"])
            )
        }
    )
    with arrow_file(table) as path:
        assert validate(path, {"dictionary-values-unique": True}) == (
            "--check-dictionary-values-unique failed on column A\n",
            "",
        )


def test_check_empty_data_string_array():
    with arrow_file(pa.table({"A": ["", "", ""]})) as path:
        assert validate(path, ALL_CHECKS) is None


def test_check_null_data_string_array():
    with arrow_file(pa.table({"A": pa.array([None, None], pa.utf8())})) as path:
        assert validate(path, ALL_CHECKS) is None


def test_check_zero_length_string_array():
    with arrow_file(pa.table({"A": pa.array([], pa.utf8())})) as path:
        assert validate(path, ALL_CHECKS) is None


def test_ints():
    table = pa.table(
        {
            "int8": pa.array([1, 2, 3], pa.int8()),
            "int16": pa.array([1, 2, 3], pa.int16()),
            "int32": pa.array([1, 2, 3], pa.int32()),
            "int64": pa.array([1, 2, 3], pa.int64()),
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_floats():
    table = pa.table(
        {
            "float16": pa.array(
                [np.float16(1), np.float16(2), np.float16(3)], pa.float16()
            ),
            "float32": pa.array([1, 2, 3], pa.float32()),
            "float64": pa.array([1, 2, 3], pa.float64()),
        }
    )
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_floats_all_finite_null_is_valid():
    table = pa.table({"A": pa.array([1.0, -2.1, None])})
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None


def test_floats_all_finite_nan_is_invalid():
    table = pa.table({"A": pa.array([1.0, -2.1, math.nan])})
    with arrow_file(table) as path:
        assert validate(path, {"floats-all-finite": True}) == (
            "--check-floats-all-finite failed on column A\n",
            "",
        )


def test_floats_all_finite_infinity_is_invalid():
    table = pa.table({"A": pa.array([1.0, -math.inf])})
    with arrow_file(table) as path:
        assert validate(path, {"floats-all-finite": True}) == (
            "--check-floats-all-finite failed on column A\n",
            "",
        )


def test_timestamp():
    table = pa.table({"date64": pa.array([1231241234, 235234234], pa.timestamp("s"))})
    with arrow_file(table) as path:
        assert validate(path, ALL_CHECKS) is None
