v1.1.0 - 2021-03-04
-------------------

* Upgrade underlying Arrow to 3.0.0.
* Upgrade underlying libxls to 1.6.2.

v1.0.0 - 2020-09-22
-------------------

Major version bump -- this is not backwards-compatible.

* Upgrade underlying Arrow to 1.0.1.
* arrow-validate: nix `--check-utf8` and `--check-offsets-dont-overflow` and
  present `--check-safe` instead. Arrow v1 checks all this for us.

v0.0.14 - 2020-09-17
--------------------

* xlsx-to-arrow, xls-to-arrow, json-to-arrow: do not convert a number
  column to String if the only String values are purely whitespace.
* xlsx-to-arrow: catch and report XML errors.

v0.0.13 - 2020-07-28
--------------------

* xlsx-to-arrow: Fix reading of empty rows: now, CPI xlsx file should
  be readable.

v0.0.12 - 2020-07-23
--------------------

* xlsx-to-arrow: Bump xlnt to v1.5.0. Fix some issues with Bureau of Labor
  Statistics CPI xlsx file, and author a couple of xlnt patches that make it hum.

v0.0.11 - 2020-03-03
--------------------

* xls(x)-to-arrow: column names are _always_ "A", "B", "C", etc.
* xls(x)-to-arrow: nix `--max-bytes-per-column-name`
* xls(x)-to-arrow: add `--header-rows-file=PATH` option, to write header rows
  as strings to a separate Arrow output file.
* xls(x)-to-arrow: use cell format for nice string values
* xls(x)-to-arrow: use correct (formatted) string length for max-bytes calculations
* xlsx-to-arrow: don't copy previous-cell format+value onto next cells

v0.0.10 - 2020-03-02
--------------------

* `xlsx-to-arrow`: similar to `xlsx-to-arrow` but for (old-style) XLS files.

v0.0.9 - 2020-02-25
-------------------

* `xlsx-to-arrow`: fix value truncation size.

v0.0.8 - 2020-02-25
-------------------

* `xlsx-to-arrow`: similar to `json-to-arrow` but for XLSX files.

v0.0.7 - 2020-02-21
-------------------

* Upgrade to pyarrow 0.16.0

v0.0.6 - 2019-12-09
-------------------

* `json-to-arrow`: avoid segfault in some --max-bytes-total cases.

v0.0.5 - 2019-12-06
-------------------

* `arrow-validate --check-column-name-control-characters`: allow non-ASCII

v0.0.4 - 2019-12-05
-------------------

* `arrow-validate`: emit a parseable description when an Arrow file does not
  meet expectations.

v0.0.3 - 2019-12-02
-------------------

* `csv-to-arrow`: emit empty string, not null, when last value of a CSV is
  empty and there is no newline after it.

v0.0.2 - 2019-11-30
-------------------

* New, experimental `json-to-arrow`

v0.0.1 - 2019-11-25
-------------------

* Initial release, with `csv-to-arrow`
