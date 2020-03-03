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
#include <sstream>
#include <string_view>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_set>
#include <utility>

#include <arrow/api.h>
#include <boost/numeric/conversion/cast.hpp>
#include <gflags/gflags.h>
#include <xls.h>
#include <xlnt/styles/number_format.hpp>
#include <xlnt/utils/calendar.hpp>

DEFINE_uint64(max_rows, 1048576, "Skip rows after parsing this many");
DEFINE_uint32(max_columns, 16384, "Skip columns after parsing this many");
DEFINE_uint32(max_bytes_per_value, 32767 * 4, "Truncate each value to at most this size");
DEFINE_uint64(max_bytes_total, std::numeric_limits<uint64_t>::max(), "Truncate file if it surpasses this many bytes of useful data");
DEFINE_string(header_rows, "", "Treat rows (comma-separated hyphenated [start, end) pairs) as column headers, not values. '' means no headers; only '0-1' behaves correctly right now");
DEFINE_string(header_rows_file, "", "Path to write header-row data");

#include "common.h"
#include "excel-table-builder.h"
#include "json-warnings.h"


static const xlnt::number_format GENERAL_NUMBER_FORMAT = xlnt::number_format::general();


#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

const char*
excelBoolToString(uint8_t d) {
    return d > 0 ? "TRUE" : "FALSE";
}

const char*
excelErrorToString(uint8_t d) {
    // http://www.openoffice.org/sc/excelfileformat.pdf
    switch (d) {
        case 0x00:
            return "#NULL!";
        case 0x07:
            return "#DIV/0!";
        case 0x0f:
            return "#VALUE!";
        case 0x17:
            return "#REF!";
        case 0x1d:
            return "#NAME?";
        case 0x24:
            return "#NUM!";
        case 0x2a:
            return "#N/A";
        default:
            return "#BAD_READ!";
    }
}

const char*
excelBoolerrToString(uint8_t d, bool isErr)
{
    // https://interoperability.blob.core.windows.net/files/MS-XLS/%5bMS-XLS%5d.pdf
    // section 2.5.10 "Bes"
    if (isErr) {
        return excelErrorToString(d);
    } else {
        return excelBoolToString(d);
    }
}


std::unordered_map<uint16_t, xlnt::number_format>
parseNumberFormats(const xls::st_format& formats, Warnings& warnings)
{
    std::unordered_map<uint16_t, xlnt::number_format> ret;

    // Add builtin formats (same for every Excel file)
    for (uint16_t i = 0; i < 0x00a4; i++) {
        if (xlnt::number_format::is_builtin_format(i)) {
            ret[i] = xlnt::number_format::from_builtin_id(i);
        }
    }

    // Add custom formats (from workbook)
    for (uint16_t i = 0; i < formats.count; i++) {
        const auto& format(formats.format[i]);
        xlnt::number_format nf(format.value, format.index);
        try {
            nf.format(0, xlnt::calendar::windows_1900); // throw xlnt::exception
            ret[format.index] = xlnt::number_format(format.value, format.index);
        } catch (xlnt::exception& err) {
            auto buf = std::stringstream();
            buf << "Ignoring invalid number format '" << format.value << ": " << err.what();
            warnings.warnXlsParseError(buf.str());
            ret[format.index] = xlnt::number_format::general();
        }
    }

    return ret;
}


std::unordered_map<uint16_t, xlnt::number_format>
loadXfIdToNumberFormat(const xls::st_xf& xfs, std::unordered_map<uint16_t, xlnt::number_format>& formats, Warnings& warnings)
{
    std::unordered_map<uint16_t, xlnt::number_format> ret;

    for (uint16_t xfId = 0; xfId < xfs.count; xfId++) {
        uint16_t formatId(xfs.xf[xfId].format);
        const auto formatIter(formats.find(formatId));
        if (formatIter == formats.end()) {
            auto buf = std::stringstream();
            buf << "Ignoring invalid format ID " << formatId << " in XF " << xfId;
            warnings.warnXlsParseError(buf.str());
            ret[xfId] = xlnt::number_format::general();
        } else {
            ret[xfId] = formatIter->second;
        }
    }

    return ret;
}


struct XlsTableBuilder : ExcelTableBuilder {
    xlnt::calendar calendar;
    xlnt::number_format numberFormatGeneral;
    std::unordered_map<uint16_t, xlnt::number_format> xfIdToNumberFormat;

    XlsTableBuilder() : ExcelTableBuilder(), calendar(xlnt::calendar::windows_1900), numberFormatGeneral(0) {}

    void initWorkbook(const xls::xlsWorkBook& workbook)
    {
        this->calendar = workbook.is1904 == 1 ? xlnt::calendar::mac_1904 : xlnt::calendar::windows_1900;
        auto formats = parseNumberFormats(workbook.formats, this->warnings);
        this->xfIdToNumberFormat = loadXfIdToNumberFormat(workbook.xfs, formats, this->warnings);
    }

    /**
     * Find a format for a cell.
     *
     * Return GENERAL_NUMBER_FORMAT if the cell points to an invalid or
     * missing format.
     */
    const xlnt::number_format&
    getCellNumberFormat(xls::xlsCell& cell)
    {
        const auto formatIter(this->xfIdToNumberFormat.find(cell.xf));
        if (formatIter == this->xfIdToNumberFormat.end()) {
            auto buf = std::stringstream();
            buf << "Invalid XF ID " << cell.xf;
            this->warnings.warnXlsParseError(buf.str());
            return this->numberFormatGeneral;
        } else {
            return formatIter->second;
        }
    }

    std::string
    cellValueString(xls::xlsCell& cell)
    {
        switch (cell.id) {
            case XLS_RECORD_NUMBER:
            case XLS_RECORD_RK:
                // libxls changes XLS_RECORD_FORMULA to XLS_RECORD_RK when the
                // (precomputed, saved) result is a number.
                {
                    const xlnt::number_format& format(this->getCellNumberFormat(cell));
                    return format.format(cell.d, this->calendar);
                }
            case XLS_RECORD_BOOLERR:
                {
                    bool isError = cell.str && std::string_view(cell.str) == "error";
                    return excelBoolerrToString(static_cast<uint8_t>(cell.d), isError);
                }

            case XLS_RECORD_FORMULA:
            case XLS_RECORD_FORMULA_ALT:
                // libxls changes type to XLS_RECORD_NUMBER if it detects a
                // numeric value. But it doesn't change any of the others. If
                // the result is boolean it writes str="bool" and if the result
                // is an error it writes str="error". So it cannot represent
                // the string results "bool" or "error" because they now mean
                // something else.
                if (cell.str != nullptr) {
                    if (std::string_view(cell.str) == "error") {
                        return excelErrorToString(static_cast<uint8_t>(cell.d));
                    } else if (std::string_view(cell.str) == "bool") {
                        return excelBoolToString(static_cast<uint8_t>(cell.d));
                    } else {
                        return cell.str;
                    }
                } else {
                    return "";
                }

            case XLS_RECORD_BLANK:
            default:
                if (cell.str != nullptr) {
                    return cell.str;
                } else {
                    return "";
                }
        }
    }

    NextAction
    addCell(int64_t row, int32_t col, xls::xlsCell& cell)
    {
        auto* columnBuilderAndHeaderColumnBuilder(this->column(col));
        if (!columnBuilderAndHeaderColumnBuilder) return CONTINUE;
        auto& columnBuilder = columnBuilderAndHeaderColumnBuilder->first;

        std::string strValue(cellValueString(cell));
        if (strValue.size() > FLAGS_max_bytes_per_value) {
            this->valueTruncator.append(strValue);
            strValue = this->valueTruncator.toUtf8StringView();
            this->valueTruncator.reset();
            this->warnings.warnValueTruncated(row, columnBuilder.name);
        }

        if (FLAGS_header_rows.size()) {
            // Assume it's "0-1" -- we don't handle anything else yet.
            if (row == 0) {
                auto& headerColumnBuilder = columnBuilderAndHeaderColumnBuilder->second;
                if (cell.id != XLS_RECORD_BLANK) {
                    headerColumnBuilder.writeValue(row, strValue);
                }
                return CONTINUE;
            }
            // This isn't the header row; second row of xls file should be
            // first row of output table. (First row of xls file is headers.)
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
            this->warnings.warnValueTruncated(row, columnBuilder.name);
        }

        uint64_t nBytesTotalNext = this->nBytesTotal + strValue.size();

        if (nBytesTotalNext > FLAGS_max_bytes_total) {
            this->warnings.warnStoppedOutOfMemory();
            return STOP;
        }

        switch (cell.id) {
            case XLS_RECORD_BLANK:
                // Empty cell means null. Don't store anything at all.
                // (Not-storing anything means null.)
                break;
            case XLS_RECORD_NUMBER:
            case XLS_RECORD_RK:
                // libxls changes XLS_RECORD_FORMULA to XLS_RECORD_RK when the
                // (precomputed, saved) result is a number.
                {
                    const xlnt::number_format& format(this->getCellNumberFormat(cell));
                    if (format.is_date_format()) {
                        this->addDatetime(columnBuilder, row, cell.d, this->calendar, strValue);
                    } else {
                        this->addNumber(columnBuilder, row, cell.d, strValue);
                    }
                }
                break;
            case XLS_RECORD_BOOLERR:
            case XLS_RECORD_FORMULA:
            case XLS_RECORD_FORMULA_ALT:
            default:
                this->addString(columnBuilder, row, strValue);
                break;
        }

        this->nBytesTotal = nBytesTotalNext;
        this->maxRowHandled = row;
        return CONTINUE;
    }
};


struct ReadXlsResult {
    Warnings warnings;
    std::shared_ptr<arrow::Table> table;
    std::shared_ptr<arrow::Table> headerTable;
};

static ReadXlsResult readXls(const char* xlsFilename) {
    XlsTableBuilder builder;

    xls::xlsWorkBook* workbook;
    xls::xls_error_t xlsError;
    workbook = xls_open_file(xlsFilename, "UTF-8", &xlsError);
    if (!workbook) {
        builder.warnings.warnXlsParseError("error opening file");
    } else {
        builder.initWorkbook(*workbook);
        xls::xlsWorkSheet* sheet;
        sheet = xls_getWorkSheet(workbook, 0);
        if (!sheet) {
            builder.warnings.warnXlsParseError("there are no worksheets");
        } else if (xls::LIBXLS_OK != xls_parseWorkSheet(sheet)) {
            builder.warnings.warnXlsParseError("error parsing worksheet");
            xls::xls_close_WS(sheet);
        } else {
            auto& rows = sheet->rows;
            auto maxRow = rows.lastrow;
            uint64_t nHeaderRows = FLAGS_header_rows.size() ? 1 : 0;
            ExcelTableBuilder::NextAction nextAction = ExcelTableBuilder::CONTINUE;
            if (maxRow >= FLAGS_max_rows + nHeaderRows) {
                builder.warnings.warnRowsSkipped(maxRow - (FLAGS_max_rows + nHeaderRows - 1));
                maxRow = FLAGS_max_rows + nHeaderRows - 1;
            }
            for (int64_t rowIndex = 0; nextAction != ExcelTableBuilder::STOP && rowIndex <= maxRow; rowIndex++) {
                xls::xlsRow* row = xls::xls_row(sheet, rowIndex);
                for (int64_t colIndex = 0; nextAction != ExcelTableBuilder::STOP && colIndex < rows.lastcol; colIndex++) {
                    xls::xlsCell& cell = row->cells.cell[colIndex];
                    nextAction = builder.addCell(rowIndex, colIndex, cell);
                }
            }
            xls::xls_close_WS(sheet);
        }
        xls::xls_close_WB(workbook);
    }

    ReadXlsResult result;
    std::tie(result.table, result.headerTable) = builder.finish();
    result.warnings = builder.warnings;
    return result;
}

int main(int argc, char** argv) {
    std::string usage = std::string("Usage: ") + argv[0] + " <XLS_FILENAME> <ARROW_FILENAME>";
    gflags::SetUsageMessage(usage);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 3) {
        gflags::ShowUsageWithFlags(argv[0]);
        std::_Exit(1);
    }

    const char* xlsFilename(argv[1]);
    const std::string arrowFilename(argv[2]);

    ReadXlsResult result = readXls(xlsFilename);
    printWarnings(result.warnings);
    writeArrowTable(*result.table, arrowFilename);
    if (!FLAGS_header_rows_file.empty()) {
        writeArrowTable(*result.headerTable, std::string(FLAGS_header_rows_file));
    }
    return 0;
}
