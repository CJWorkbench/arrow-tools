#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <string_view>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_set>
#include <utility>

#include <arrow/api.h>
#include <boost/numeric/conversion/cast.hpp>
#include <gflags/gflags.h>
#include <xlnt/xlnt.hpp>

DEFINE_uint64(max_rows, 1048576, "Skip rows after parsing this many");
DEFINE_uint32(max_columns, 16384, "Skip columns after parsing this many");
DEFINE_uint32(max_bytes_per_value, 32767 * 4, "Truncate each value to at most this size");
DEFINE_uint64(max_bytes_per_column_name, 1024, "Truncate each column header to at most this size");
DEFINE_uint64(max_bytes_total, std::numeric_limits<uint64_t>::max(), "Truncate file if it surpasses this many bytes of useful data");
DEFINE_string(header_rows, "0-1", "Treat rows (comma-separated hyphenated [start, end) pairs) as column headers, not values. '' means no headers");

#include "common.h"
#include "json-table-builder.h"
#include "json-warnings.h"


#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)


struct XlsxHandler {
    int64_t maxRowSeen; // max row index there was a cell for (may have been ignored)
    int64_t maxRowHandled; // max row index of the output table
    uint64_t nBytesTotal;
    Warnings warnings;
    std::vector<std::unique_ptr<ColumnBuilder>> columns;
    StringBuffer colnameTruncator;
    StringBuffer valueTruncator;

    typedef enum _NextAction {
        CONTINUE,
        STOP, // STOP means "ignore the rest of the file"
    } NextAction;

    XlsxHandler()
        : maxRowSeen(-1)
        , maxRowHandled(-1)
        , nBytesTotal(0)
        , columns()
        , colnameTruncator(FLAGS_max_bytes_per_column_name)
        , valueTruncator(FLAGS_max_bytes_per_value)
    {
    }

    NextAction
    addCell(const xlnt::cell& cell)
    {
        size_t col(cell.column_index() - 1);
        ColumnBuilder* cb(this->column(col));
        if (!cb) return CONTINUE;

        int64_t row(cell.row() - 1);
        std::string strValue(cell.to_string());

        if (FLAGS_header_rows.size()) {
            // Assume it's "0-1" -- we don't handle anything else yet.
            if (row == 0) {
                if (strValue.size() > FLAGS_max_bytes_per_column_name) {
                    this->colnameTruncator.append(strValue);
                    strValue = this->colnameTruncator.toUtf8StringView();
                    this->colnameTruncator.reset();
                    this->warnings.warnColumnNameTruncated(strValue);
                }

                cb->setName(strValue);
                return CONTINUE;
            }
            // This isn't the header row; second row of xlsx file should be
            // first row of output table. (First row of xlsx file is headers.)
            row -= 1;
        }

        this->maxRowSeen = row;

        if (static_cast<uint64_t>(row) >= FLAGS_max_rows) {
            // Ignore cell ... but keep reading, so we can report the correct
            // number of skipped rows. (Assume row numbers never decrease.)
            return CONTINUE;
        }

        if (strValue.size() > FLAGS_max_bytes_per_value) {
            this->valueTruncator.append(strValue);
            strValue = this->valueTruncator.toUtf8StringView();
            this->valueTruncator.reset();
            this->warnings.warnValueTruncated(row, cb->name);
        }

        uint64_t nBytesTotalNext = this->nBytesTotal + strValue.size();

        if (nBytesTotalNext > FLAGS_max_bytes_total) {
            this->warnings.warnStoppedOutOfMemory();
            return STOP;
        }

        switch (cell.data_type()) {
            case xlnt::cell::type::empty:
                // Empty cell means null. Don't store anything at all.
                // (Not-storing anything means null.)
                break;
            case xlnt::cell::type::date:
                this->addDatetime(*cb, row, cell.value<double>(), cell.base_date(), strValue);
                break;
            case xlnt::cell::type::number:
                if (cell.is_date()) {
                    this->addDatetime(*cb, row, cell.value<double>(), cell.base_date(), strValue);
                } else {
                    this->addNumber(*cb, row, cell.value<double>(), strValue);
                }
                break;
            default:
                this->addString(*cb, row, strValue);
                break;
        }

        this->nBytesTotal = nBytesTotalNext;
        this->maxRowHandled = row;
        return CONTINUE;
    }

    ColumnBuilder*
    column(size_t i)
    {
        if (i >= FLAGS_max_columns) {
            this->warnings.warnColumnSkipped(xlnt::column_t::column_string_from_index(i + 1));
            return nullptr;
        }

        // Create missing columns. They'll be all-null until we write a value.
        while (this->columns.size() <= i) {
            this->columns.emplace_back(std::make_unique<ColumnBuilder>(xlnt::column_t::column_string_from_index(this->columns.size() + 1)));
        }

        return this->columns[i].get();
    }

    std::shared_ptr<arrow::Table>
    finish()
    {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(this->columns.size());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(this->columns.size());
        auto nRows = this->maxRowHandled + 1;

        std::unordered_set<std::string> usedColumnNames;

        for (auto& column : this->columns) {
            bool isInserted;
            std::tie(std::ignore, isInserted) = usedColumnNames.emplace(column->name);
            if (!isInserted) {
                // Do not create two columns with the same name. Ignore.
                StringBuffer buf(column->name.size());
                buf.append(column->name);
                this->warnings.warnColumnNameDuplicated(0, buf);
                continue;
            }
            if (ColumnBuilder::isColumnNameInvalid(column->name)) {
                // Do not create invalid-named columns. Ignore.
                this->warnings.warnColumnNameInvalid(0, column->name);
                continue;
            }

            column->growToLength(nRows);
            column->warnOnErrors(this->warnings);
            if (column->dtype == ColumnBuilder::UNTYPED) {
                this->warnings.warnColumnNull(column->name);
            }
            std::shared_ptr<arrow::Array> array(column->finish(nRows));
            std::shared_ptr<arrow::Field> field = arrow::field(column->name, array->type());
            arrays.push_back(array);
            fields.push_back(field);
        }

        this->columns.clear();

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, arrays, nRows);
    }

  private:
    void
    addNumber(ColumnBuilder& cb, int64_t row, double value, std::string& strValue) const
    {
        cb.writeParsedNumber(row, value, strValue);
    }

    /**
     * Add `value` as a datetime.
     *
     * This API does not use xlnt::datetime, because that would drop the
     * nanoseconds. Instead, we read the cell value as a double (which is how
     * Excel stores it -- whole part days, fractional part fraction-of-the-day)
     * and convert it to nsSinceEpoch.
     */
    void
    addDatetime(ColumnBuilder& cb, int64_t row, double value, xlnt::calendar baseDate, std::string& strValue) const
    {
        double epochDays;
        switch (baseDate) {
            // To find these constants:
            //
            // 1. Open a new Excel (or LibreOffice) sheet
            // 2. Enter `=DATE(1970, 1, 1)`
            // 3. Format as General
            // 4. Convert date system in document properties
            //
            // The cell will contain the number of days to add to Excel's date
            // to arrive at an epoch-centered date.
            case xlnt::calendar::mac_1904:
                epochDays = 24107;
                break;
            case xlnt::calendar::windows_1900:
            default: // prevent "epochDays uninitialized" compiler error
                epochDays = 25569;
                // TODO handle times before false leap year 1900-02-29
                // (Excel bug that is now part of Excel file standard)
                break;
        }
        double nsSinceEpochDouble = (value - epochDays) * 86400 * 1000000000;
        int64_t nsSinceEpoch;
        bool isOverflow;
        try {
            nsSinceEpoch = boost::numeric_cast<int64_t>(nsSinceEpochDouble);
            isOverflow = false;
        } catch (boost::numeric::bad_numeric_cast& e) {
            nsSinceEpoch = 0;
            isOverflow = true;
        }

        cb.writeParsedTimestamp(row, nsSinceEpoch, isOverflow, strValue);
    }

    void
    addString(ColumnBuilder& cb, int64_t row, std::string& strValue) const
    {
        cb.writeString(row, strValue);
    }
};

struct ReadXlsxResult {
    Warnings warnings;
    std::shared_ptr<arrow::Table> table;
};

static ReadXlsxResult readXlsx(const char* xlsxFilename) {
    XlsxHandler handler;

    try {
        xlnt::streaming_workbook_reader wbr;
        wbr.open(xlsxFilename);
        {
            // Open the first worksheet
            std::vector<std::string> titles(wbr.sheet_titles());
            if (titles.size() == 0) {
                printf("Excel file has no worksheets\n");
            } else {
                wbr.begin_worksheet(titles[0]);
            }
        }

        while (LIKELY(wbr.has_cell())) {
            const xlnt::cell& cell(wbr.read_cell());
            if (UNLIKELY(handler.addCell(cell) == XlsxHandler::STOP)) {
                break;
            }
        }
    } catch (xlnt::exception& err) {
        handler.warnings.warnXlsxParseError(err.what());
    }

    size_t nRows = handler.maxRowSeen + 1;
    if (nRows > FLAGS_max_rows) {
        handler.warnings.warnRowsSkipped(nRows - FLAGS_max_rows);
        nRows = FLAGS_max_rows;
    }

    std::shared_ptr<arrow::Table> table(handler.finish());
    return ReadXlsxResult { handler.warnings, table };
}

int main(int argc, char** argv) {
    std::string usage = std::string("Usage: ") + argv[0] + " <XLSX_FILENAME> <ARROW_FILENAME>";
    gflags::SetUsageMessage(usage);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 3) {
        gflags::ShowUsageWithFlags(argv[0]);
        std::_Exit(1);
    }

    const char* xlsxFilename(argv[1]);
    const std::string arrowFilename(argv[2]);

    ReadXlsxResult result = readXlsx(xlsxFilename);
    printWarnings(result.warnings);
    writeArrowTable(*result.table, arrowFilename);
    return 0;
}
