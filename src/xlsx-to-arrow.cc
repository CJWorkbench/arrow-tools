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
#include <tuple>
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
DEFINE_uint64(max_bytes_total, std::numeric_limits<uint64_t>::max(), "Truncate file if it surpasses this many bytes of useful data");
DEFINE_string(header_rows, "", "Treat rows (comma-separated hyphenated [start, end) pairs) as column headers, not values. '' means no headers; only '0-1' behaves correctly right now");
DEFINE_string(header_rows_file, "", "Path to write header-row data");

#include "common.h"
#include "excel-table-builder.h"
#include "json-warnings.h"


#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)


struct XlsxTableBuilder : ExcelTableBuilder {
    NextAction
    addCell(const xlnt::cell& cell)
    {
        size_t col(cell.column_index() - 1);
        auto* builders(this->column(col));
        if (!builders) return CONTINUE;

        ColumnBuilder& cb(builders->first);

        int64_t row(cell.row() - 1);
        std::string strValue(cell.to_string());

        if (strValue.size() > FLAGS_max_bytes_per_value) {
            this->valueTruncator.append(strValue);
            strValue = this->valueTruncator.toUtf8StringView();
            this->valueTruncator.reset();
            this->warnings.warnValueTruncated(row, cb.name);
        }

        if (FLAGS_header_rows.size()) {
            StringColumnBuilder& headerCb(builders->second);

            // Assume it's "0-1" -- we don't handle anything else yet.
            if (row == 0) {
                headerCb.writeValue(row, strValue);
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
                this->addDatetime(cb, row, cell.value<double>(), cell.base_date(), strValue);
                break;
            case xlnt::cell::type::number:
                if (cell.is_date()) {
                    this->addDatetime(cb, row, cell.value<double>(), cell.base_date(), strValue);
                } else {
                    this->addNumber(cb, row, cell.value<double>(), strValue);
                }
                break;
            default:
                this->addString(cb, row, strValue);
                break;
        }

        this->nBytesTotal = nBytesTotalNext;
        this->maxRowHandled = row;
        return CONTINUE;
    }
};

struct ReadXlsxResult {
    Warnings warnings;
    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Table> headerTable;
};

static ReadXlsxResult readXlsx(const char* xlsxFilename) {
    XlsxTableBuilder builder;

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
            if (UNLIKELY(builder.addCell(cell) == ExcelTableBuilder::STOP)) {
                break;
            }
        }
    } catch (xlnt::exception& err) {
        builder.warnings.warnXlsxParseError(err.what());
    }

    size_t nRows = builder.maxRowSeen + 1;
    if (nRows > FLAGS_max_rows) {
        builder.warnings.warnRowsSkipped(nRows - FLAGS_max_rows);
        nRows = FLAGS_max_rows;
    }

    ReadXlsxResult result;
    std::tie(result.table, result.headerTable) = builder.finish();
    result.warnings = builder.warnings;
    return result;
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
    if (!FLAGS_header_rows_file.empty()) {
        writeArrowTable(*result.headerTable, std::string(FLAGS_header_rows_file));
    }
    return 0;
}
