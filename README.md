arrow-tools
===========

Statically-linked Linux binaries for dealing with
[Arrow](https://arrow.apache.org) files.

Usage
=====

Use this in your Docker images. For instance:

```
# (look to Git tags to find latest VERSION)
FROM workbenchdata/arrow-tools:VERSION AS arrow-tools

FROM debian:buster AS my-normal-build
# ... normal Dockerfile stuff...
# ... normal Dockerfile stuff...
COPY --from=arrow-tools /usr/bin/csv-to-arrow /usr/bin/csv-to-arrow
# ... normal Dockerfile stuff...
```

... and now your programs running in that Docker container have access
to these binaries.

Binaries
========

arrow-validate
--------------

*Purpose*: check in O(n) time that an Arrow-format file meets assumptions.

*Usage*:

```
arrow-validate input.arrow \
    --check-utf8 \
    --check-offsets-dont-overflow \
    --check-floats-all-finite \
    --check-dictionary-values-all-used \
    --check-dictionary-values-not-null \
    --check-dictionary-values-unique \
    --check-column-name-control-characters \
    --check-column-name-max-bytes=100
```

`arrow-validate` implements checks that should arguably be included in Arrow
itself. It returns exit code 0 if validation passes, or 1 if validation fails.

The two most-obvious checks are UTF-8 validation and buffer-overflow detection.
These code paths are highly optimized, and you should use them liberally.

The other checks are opinionated. They help callers make assumptions about
the Arrow file.

*Features*:

* `--check-utf8`: validate UTF-8 in column names, utf8 columns and dictionaries.
* `--check-offsets-dont-overflow`: ensure offsets don't cause buffer overflow.
* `--check-floats-all-finite`: disallow NaN, -Infinity and Infinity.
* `--check-dictionary-values-all-used`: disallow unused dictionary values.
* `--check-dictionary-values-not-null`: disallow nulls in dictionaries.
* `--check-dictionary-values-unique`: disallow duplicate dictionary values.
* `--check-column-name-control-characters`: disallow ASCII characters
  `\x00` to `\x1f` in column names.
* `--check-column-name-max-bytes=[int]`: disallow too-long column names.
* _Warn on stdout_ as soon as any check fails. The error is on one line,
  newline-terminated. It looks like:

```
--check-dictionary-values-unique failed on column My Column
```

Two potential errors follow a slightly different pattern: they omit invalid
column names, so output is valid UTF-8:

```
--check-column-name-control-characters failed on a column name
--check-utf8 failed on a column name
```

Error-message patterns are guaranteed not to change between major versions.

csv-to-arrow
------------

*Purpose*: convert a CSV file to Arrow format, permissively and RAM-safely.

*Usage*:

```
csv-to-arrow input.csv output.arrow \
    --delimiter=, \
    --max-columns=1000 \
    --max-rows=1000000 \
    --max-bytes-per-value=32768
```

`csv-to-arrow` holds the entire Arrow table in RAM. To restrict RAM usage,
truncate the input file beforehand.

Also, `csv-to-arrow` assumes valid UTF-8. To prevent errors, validate the input
file beforehand. (If you're truncating, ensure you don't truncate a UTF-8
continuation byte.)

If you plan to load the output into Pandas, be aware that a Python `str` costs
about 50 bytes of overhead. As a heuristic, loading the output Arrow file as a
`pandas.DataFrame` costs:

* The size of all the bytes of text -- roughly the size of the Arrow file
* 8 bytes per cell for a pointer -- `8 * table.num_columns * table.num_rows`
* 50 bytes per string --
  `50 * (table.num_columns * table.num_rows - sum([col.null_count for col in table.columns]))`

Memory savings can be found when reading with `pyarrow.Table.to_pandas()`:
`deduplicate_objects` can remove any duplicated string; and
`strings_to_categorical` and `categories` can halve the cost of pointers if
most values are duplicated. Dictionary-encoding and Python memory costs are
scope of this tool.

*Features*:

* _Limit table size_: truncate the result if it's too long or wide.
* _Limit value size_: truncate values if they consume too many bytes.
* _No character-set conversion_: assume UTF-8 input.
* _Variable row lengths_: allow any new row to add new columns (back-filling
  `null` for prior rows).
* _No types_: every value is a string.
* _No column names_: columns are named `"0"`, `"1"`, etc.
* _Skip empty rows_: newline-only rows are ignored.
* _Universal newlines_: any sequence of `\r` and/or `\n` starts a new record.
* _Permit any characters_: control characters and `\0` are not a problem. Also,
  `"` is allowed in an unquoted value (except, of course, as the first
  character).
* _Repair invalid values_: data after a close-quote is appended to a value.
* _Repair unclosed quote_: close a quote if the file ends on an unclosed quote.
* _Garbage in, garbage out_: any valid UTF-8 file will produce valid output.
  Most invalid UTF-8 files will produce invalid output.
* _Warn on stdout_: stdout can produce lines of text matching these patterns:

```
skipped 102312 rows (after row limit of 1000000)
skipped 1 columns (after column limit of 1000)
truncated 123 values (value byte limit is 32768; see row 2 column 1)
repaired 321 values (misplaced quotation marks; see row 3 column 5)
repaired last value (missing quotation mark)
```

Note `skipped 1 columns` is plural. The intent is for callers to parse using
regular expressions, so the `s` is not optional. Also, messages formats won't
change without a major-version bump.


json-to-arrow
-------------

*Purpose*: convert a JSON Array-of-records file to Arrow format, predictably
and RAM-safely.

*Usage*:

```
json-to-arrow input.json output.arrow \
    --max-columns=1000 \
    --max-rows=1000000 \
    --max-bytes-per-value=32768 \
    --max-bytes-total=1073741824 \
    --max-bytes-per-error-value=100 \
    --max-bytes-per-column-name=100
```

`json-to-arrow` assumes valid UTF-8. To prevent errors, validate the input
file beforehand. (If you're truncating it, ensure you don't truncate a UTF-8
continuation byte.)

*Features*:

* _Limit table size_: truncate the result if it's too long or wide.
* _Limit value size_: truncate values if they consume too many bytes.
* _No character-set conversion_: assume UTF-8 input.
* _Variable row lengths_: allow any new row to add new columns (back-filling
  `null` for prior rows).
* _Automatic types_: each column starts null. It will grow to int8, int16,
  int32, int64, float64 when it encounters numbers. It will switch to String
  when we see a String. (When parsing numbers, we store the raw JSON text so
  the String conversion is lossless.)
* _JSON-serialize Object/Array values_. No nested lists or structs, thanks.
* _Warn on lossy Numbers_: when we encounter an int64 in a float column, warn
  if converting int64 to float64 is lossy.
* _Sensible column names_: column names must be non-empty and cannot contain
  ASCII control characters `0x00-0x1f`.
* _Auto-find Array of records_. If the file is an Array, great! Otherwise, if
  the file is an Object, scan its values until we find an Array. This is fast
  and predictable; it should work for most API servers.
* _Skip non-records_: Continue (and warn) if a record is not an Object.
* _Garbage in, garbage out_: any valid UTF-8 file will produce valid output.
  Most invalid UTF-8 files will produce invalid output.
* _Warn on stdout_: stdout can produce lines of text matching these patterns:

```
JSON parse error at byte %d: %s [see rapidjson/error/en.h for all strings]
JSON is not an Array or Object containing an Array; got: %s [JSON-encoded document]
skipped %d rows (after row limit of %d) [--max-rows]
stopped at limit of %d bytes of data [--max-bytes-total]
skipped %d non-Object records; example Array item %d: %s [JSON-encoded value]
skipped column %s%s (after column limit of %d) [--max-columns; second %s is either "and more" or ""]
chose string type for null column %s%s [second %s is either "and more" or ""]
truncated %d column names; example %s [--max-bytes-per-column-name]
ignored invalid column %s%s [second %s is either "and more" or ""]
ignored duplicate column %s%s starting at row %d [second %s is either "and more" or ""]
truncated %d values (value byte limit is %d; see row %d column %s) [--max-bytes-per-value]
lost precision converting %d int64 Numbers to float64; see row %d column %s
replaced infinity with null for %d Numbers; see row %d column %s
interpreted %d Numbers as String; see row %d column %s
```

The intent is for callers to parse using regular expressions. Messages won't
change without a major-version bump. Neither JSON-encoded values nor column
names can contain newlines, so each message is guaranteed to fit one line.

(Why use `and more` instead of counting? Because to count columns we'd need
to store column names we aren't using -- disrespecting the RAM limit implied
by `--max-columns`.)

*Memory considerations*: while parsing, all data is stored in RAM. To limit RAM
usage, there are lots of dials to tune! But the principles are simple.

Basically, RAM usage boils down to: *cost per table cell*, *cost per value*
and *overhead*.

* Each table cell costs *8 to 16 bytes*. We store all values (even nulls) in a
  String array (costing 8 bytes). If it's a Number, we also store it in a
  Number array (costing 1-8 bytes for int8-int64, or 8 bytes for float64). So
  in the worst case, allocate `--max-rows` times `--max-columns` times `16`
  bytes.
* Each non-null value costs *its UTF-8 String representation*. For instance,
  `true` costs 4 bytes; `1231.4231` costs 9 bytes; `"abc"` costs 3 bytes. This
  costs `--max-bytes-total` at most.
* When we append to a full buffer, we double its capacity. (This is standard
  practice.) So in the absolute worst case, *overhead may double all costs*.
* Finally, remember to tune little odds and ends:
    * `--max-columns` and `--max-bytes-per-column-name` aren't included in
      the above calculations. Keep both small so you can ignore them.
    * `--max-bytes-per-column-name`, `--max-bytes-per-error-value` and
      `--max-bytes-per-column-name` are fixed-size buffers, allocated when
      the program starts. Keep them small.
    * (*BUG*: presently, rapidjson may allocate more RAM than this, because it
      doesn't use fixed-size buffers. For example, a single-string JSON file
      can cost the entire file size in RAM just to parse the string. Sorry.)

You should be able to tune these parameters to handle any real-world JSON file.
You probably can't avoid degenerate cases (such as a malicious attacker); so
plan for out-of-memory when you invoke this program.


xlsx-to-arrow, xls-to-arrow
---------------------------

*Purpose*: convert an Excel file to Arrow format, predictably and RAM-safely.

*Usage*:

```
xlsx-to-arrow input.xlsx output.arrow \
    --max-columns=1000 \
    --max-rows=1000000 \
    --header-rows=0-1 \
    --max-bytes-per-value=32768 \
    --max-bytes-total=1073741824 \
    --max-bytes-per-column-name=100
```

Use `xlsx-to-arrow` for `.xlsx` files (Excel 2007+). Use `xls-to-arrow` for
older `.xls` files.

*Features*:

* _Limit table size_: truncate the result if it's too long or wide.
* _Limit value size_: truncate values if they consume too many bytes.
* _Automatic types_: each column starts null. It will grow to float64 when it
  encounters numbers, timestamp when it encounters dates, and String when it
  encounters anything else (or a mix of types). Conversions always warn.
* _Sensible column names_: default column names are "A", "B", etc. Column names
  cannot contain ASCII control characters `0x00-0x1f`, and they cannot be
  duplicated. (Conflicting columns will be nixed with a warning.)
* _Warn on stdout_: stdout can produce lines of text matching these patterns:

```
Invalid XLSX file: %s [xlnt::exception for now; may change in a minor version]
Invalid XLS file: %s
skipped %d rows (after row limit of %d) [--max-rows]
stopped at limit of %d bytes of data [--max-bytes-total]
skipped column %s%s (after column limit of %d) [--max-columns; second %s is either "and more" or ""]
chose string type for null column %s%s [second %s is either "and more" or ""]
truncated %d column names; example %s [--max-bytes-per-column-name]
ignored invalid column %s%s [second %s is either "and more" or ""]
ignored duplicate column %s%s starting at row %d [second %s is either "and more" or ""]
truncated %d values (value byte limit is %d; see row %d column %s) [--max-bytes-per-value]
replaced infinity with null for %d Numbers; see row %d column %s
replaced out-of-range with null for %d Timestamps; see row %d column %s
interpreted %d Numbers as String; see row %d column %s
interpreted %d Timestamps as String; see row %d column %s
```

The intent is for callers to parse using regular expressions. Messages won't
change without a major-version bump. Neither JSON-encoded values nor column
names can contain newlines, so each message is guaranteed to fit one line.

(Why use `and more` instead of counting? To mimic `json-to-arrow`.)

*Memory considerations*: see `json-to-arrow`.


Developing
==========

`docker build .` will compile and unit-test everything.

To make a faster development loop when you need it most, use `docker` to
jump in with an intermediate image. You can see all `IMAGE_ID`s in the
`docker build .` output.

* `docker run -it --rm --volume "$(pwd):/app" IMAGE_ID bash` -- start a
  Bash shell, and overwrite the image's source code with our own. The mounted
  volume means you can `make` or `pytest` immediately after you edit source
  code. (That's not normal.)


Deploying
=========

1. Write to `CHANGELOG.md`
2. `git commit`
3. `git tag VERSION` (use semver -- e.g., `v1.2.3`)
4. `git push --tags && git push`
5. Wait; Docker Hub will publish the new image **SEE NOTE**

**NOTE** Currently, Docker Hub automatic builds don't use AVX2 instructions,
so the builds fail. For now, the workaround is:

1. `docker build . -t workbenchdata/arrow-tools:VERSION`
2. `docker push workbenchdata/arrow-tools:VERSION`

Longer-term, several paths would work: A) use AVX, not AVX2, and suffer a
not-yet-measured slowdown; B) switch away from Docker Hub; C) wait for
Docker Hub to upgrade its machines; D) use runtime CPU feature detection, so
Docker Hub runs different code than production.


License
=======

MIT
