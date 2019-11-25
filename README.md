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
* _Warn on stdout_: stdout can produce lines of text matching these patterns:
* _Garbage in, garbage out_: any valid UTF-8 file will produce valid output.
  Most invalid UTF-8 files will produce invalid output.

```
skipped 102312 rows (after row limit of 1000000)
skipped 1 columns (after column limit of 1000)
truncated 123 values (value byte limit is 32768; see row 2 column 1)
repaired 321 values (misplaced quotation marks; see row 3 column 5)
repaired last value (missing quotation mark)
```

(Note `skipped 1 columns` is plural. The intent is for callers to parse using
regular expressions, so the `s` is not optional. Also, messages formats won't
change without a major-version bump.


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
5. Wait; Docker Hub will publish the new image
