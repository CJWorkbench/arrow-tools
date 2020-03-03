#include <boost/numeric/conversion/cast.hpp>
#include <gflags/gflags.h>

#include "column-builder.h"
#include "common.h"
#include "excel-table-builder.h"

DECLARE_uint32(max_bytes_per_value);
DECLARE_uint32(max_columns);
DECLARE_string(header_rows);

ExcelTableBuilder::ExcelTableBuilder()
    : maxRowSeen(-1)
    , maxRowHandled(-1)
    , nBytesTotal(0)
    , columns()
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

std::pair<ColumnBuilder, StringColumnBuilder>*
ExcelTableBuilder::column(size_t i)
{
    if (i >= FLAGS_max_columns) {
        this->warnings.warnColumnSkipped(ExcelTableBuilder::buildDefaultColumnName(i));
        return nullptr;
    }

    // Create missing columns. They'll be all-null until we write a value.
    while (this->columns.size() <= i) {
        auto column = std::make_unique<std::pair<ColumnBuilder, StringColumnBuilder> >();
        column->first.setName(ExcelTableBuilder::buildDefaultColumnName(this->columns.size()));
        this->columns.emplace_back(std::move(column));
    }

    return this->columns[i].get();
}

std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table> >
ExcelTableBuilder::finish()
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> headerArrays;
    std::vector<std::shared_ptr<arrow::Field>> headerFields;
    arrays.reserve(this->columns.size());
    fields.reserve(this->columns.size());
    headerArrays.reserve(this->columns.size());
    headerFields.reserve(this->columns.size());
    auto nRows = this->maxRowHandled + 1;
    auto nHeaderRows = FLAGS_header_rows == "0-1" ? 1 : 0;

    for (auto& column : this->columns) {
        auto& columnBuilder = column->first;

        columnBuilder.growToLength(nRows);
        columnBuilder.warnOnErrors(this->warnings);
        if (columnBuilder.dtype == ColumnBuilder::UNTYPED) {
            this->warnings.warnColumnNull(columnBuilder.name);
        }
        std::shared_ptr<arrow::Array> array(columnBuilder.finish(nRows));
        std::shared_ptr<arrow::Field> field = arrow::field(columnBuilder.name, array->type());
        arrays.push_back(array);
        fields.push_back(field);

        auto& headerColumnBuilder = column->second;
        headerColumnBuilder.growToLength(nHeaderRows); // may be 0 rows
        std::shared_ptr<arrow::Array> headerArray;
        ASSERT_ARROW_OK(headerColumnBuilder.arrayBuilder.Finish(&headerArray), "converting headers to array");
        std::shared_ptr<arrow::Field> headerField = arrow::field(columnBuilder.name, arrow::utf8());
        headerArrays.push_back(headerArray);
        headerFields.push_back(headerField);
    }

    this->columns.clear();

    auto schema = arrow::schema(fields);
    auto table = arrow::Table::Make(schema, arrays, nRows);

    auto headerSchema = arrow::schema(headerFields);
    auto headerTable = arrow::Table::Make(headerSchema, headerArrays, nHeaderRows);

    return std::make_pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table> >(std::move(table), std::move(headerTable));
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
