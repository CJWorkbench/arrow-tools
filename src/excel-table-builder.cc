#include <boost/numeric/conversion/cast.hpp>
#include <unordered_set>
#include <gflags/gflags.h>

#include "column-builder.h"
#include "excel-table-builder.h"


class Warnings;

DECLARE_uint32(max_bytes_per_value);
DECLARE_uint32(max_bytes_per_column_name);
DECLARE_uint32(max_columns);

ExcelTableBuilder::ExcelTableBuilder()
    : maxRowSeen(-1)
    , maxRowHandled(-1)
    , nBytesTotal(0)
    , columns()
    , colnameTruncator(FLAGS_max_bytes_per_column_name)
    , valueTruncator(FLAGS_max_bytes_per_value)
{
}

std::string
ExcelTableBuilder::buildDefaultColumnName(uint32_t index)
{
    char buf[4] = { 0, 0, 0, 0 };
    int pos = 0;

    if (index >= 26) {
        if (index >= 26 * 26) {
            buf[pos] = 'A' + (index / (26 * 26)) % 26;
            pos++;
        }
        buf[pos] = 'A' + ((index / 26) % 26);
        pos++;
    }
    buf[pos] = 'A' + (index % 26);

    return std::string(buf);
}

ColumnBuilder*
ExcelTableBuilder::column(size_t i)
{
    if (i >= FLAGS_max_columns) {
        this->warnings.warnColumnSkipped(ExcelTableBuilder::buildDefaultColumnName(i));
        return nullptr;
    }

    // Create missing columns. They'll be all-null until we write a value.
    while (this->columns.size() <= i) {
        this->columns.emplace_back(std::make_unique<ColumnBuilder>(ExcelTableBuilder::buildDefaultColumnName(this->columns.size())));
    }

    return this->columns[i].get();
}

std::shared_ptr<arrow::Table>
ExcelTableBuilder::finish()
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

void
ExcelTableBuilder::addNumber(ColumnBuilder& cb, int64_t row, double value, std::string_view strValue) const
{
    cb.writeParsedNumber(row, value, strValue);
}

void
ExcelTableBuilder::addDatetime(ColumnBuilder& cb, int64_t row, double value, xlnt::calendar calendar, std::string_view strValue) const
{
    double epochDays;
    switch (calendar) {
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
ExcelTableBuilder::addString(ColumnBuilder& cb, int64_t row, std::string_view strValue) const
{
    cb.writeString(row, strValue);
}
