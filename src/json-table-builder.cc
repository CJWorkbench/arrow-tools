#include <charconv>

#include <gflags/gflags.h>

#include "common.h"
#include "json-table-builder.h"
#include "json-warnings.h"

DECLARE_uint32(max_columns);

static void
storeStringValue(int64_t row, std::string_view str, arrow::StringBuilder& builder)
{
    // Called no matter what the input
    ASSERT_ARROW_OK(builder.Reserve(row + 1 - builder.length()), "reserving space for Strings");
    ASSERT_ARROW_OK(builder.ReserveData(str.size()), "reserving space for String bytes");

    if (row > builder.length()) {
        // Arrow 0.15.1 is missing UnsafeAppendNulls(); but this can't error
        builder.AppendNulls(row - builder.length());
    }
    builder.UnsafeAppend(str.begin(), str.size());
}

// pass builderLength separately: builder.length() is wrong in Arrow
// 0.15.1: https://issues.apache.org/jira/browse/ARROW-7281
static void
storeIntValue(int64_t row, int64_t value, arrow::AdaptiveIntBuilder& builder, int64_t& builderLength)
{
    if (row > builderLength) {
        ASSERT_ARROW_OK(builder.AppendNulls(row - builderLength), "adding null integers");
    }
    ASSERT_ARROW_OK(builder.Append(value), "adding integer");
    builderLength = row + 1;
}

static void
storeFloat64Value(int64_t row, double value, arrow::DoubleBuilder& builder)
{
    ASSERT_ARROW_OK(builder.Reserve(row + 1 - builder.length()), "reserving space for Floats");
    if (row > builder.length()) {
        // Arrow 0.15.1 is missing UnsafeAppendNulls(); but this can't error
        builder.AppendNulls(row - builder.length());
    }
    builder.UnsafeAppend(value);
}


void
ColumnBuilder::writeString(int64_t row, std::string_view str)
{
    storeStringValue(row, str, this->stringBuilder);

    switch (this->dtype) {
        case UNTYPED:
            this->dtype = STRING;
            break;

        case STRING:
            break;

        case INT:
            this->intBuilder.reset(nullptr);
            this->dtype = STRING;
            break;

        case FLOAT:
            this->doubleBuilder.reset(nullptr);
            this->dtype = STRING;
            break;
    }
}


static bool
canParseJsonNumberAsInt64(std::string_view str)
{
    // JSON number format is blissfully restrictive: no leading "+" or 0,
    // no whitespace. We can pick a parser simply by examining str.
    const static std::string minInt64String = std::to_string(std::numeric_limits<int64_t>::min());
    const static std::string maxInt64String = std::to_string(std::numeric_limits<int64_t>::max());
    return (
        // we always parse exponential notation as float.
        // (And of course, decimals are floats.)
        (str.find_first_of(".eE") == std::string_view::npos)
        // magic numbers differ for negative and positive numbers
        && (
            str[0] == '-' ? (
                // few enough digits it's certainly int64
                str.size() < minInt64String.size()
                // same length and lexicographically smaller
                || (str.size() == minInt64String.size() && str <= minInt64String)
            ) : (
                // few enough digits it's certainly int64
                str.size() < maxInt64String.size()
                // same length and lexicographically smaller
                || (str.size() == maxInt64String.size() && str <= maxInt64String)
            )
        )
    );
}


void
ColumnBuilder::writeNumber(int64_t row, std::string_view str)
{
    storeStringValue(row, str, this->stringBuilder);
    this->nNumbers++;

    if (canParseJsonNumberAsInt64(str)) {
        int64_t value = 0; // 0 to avoid uninitialized-value compiler warning
        std::from_chars(str.begin(), str.end(), value); // Guaranteed success
        this->writeInt64(row, value);
    } else {
        // [2019-11-28] GCC 8.3.0 std::from_chars() does not convert doubles
        // use Google double-conversion library instead
        int processedCharCount = 0;
        double value = this->doubleConverter.StringToDouble(str.begin(), str.size(), &processedCharCount);
        // rapidjson guarantees ([unwisely, IMO -- adamhooper, 2019-11-29])
        // the number can be parsed to a double. If it can't, rapidjson
        // aborts the entire parse. So NaN/Infinity won't happen.
        //
        // This is a bug, fixed after v1.1.0:
        // https://github.com/Tencent/rapidjson/issues/1368
        if (std::isfinite(value)) {
            this->writeFloat64(row, value);
        } else {
            // We can only reach here when we upgrade rapidjson.
            // See https://github.com/Tencent/rapidjson/issues/1368
            this->growToLength(row + 1); // append null
            if (this->nOverflowNumbers == 0) {
                this->firstOverflowNumberRow = row;
            }
            this->nOverflowNumbers++;
        }
    }
}


void
ColumnBuilder::writeInt64(int64_t row, int64_t value)
{
    switch (this->dtype) {
        case UNTYPED:
            {
                this->intBuilder = std::make_unique<arrow::AdaptiveIntBuilder>(arrow::default_memory_pool());
                this->firstNumberRow = row;
                this->dtype = INT;
            }
            storeIntValue(row, value, *this->intBuilder.get(), this->intBuilderLength);
            break;

        case INT:
            storeIntValue(row, value, *this->intBuilder.get(), this->intBuilderLength);
            break;

        case FLOAT:
            storeFloat64Value(row, this->convertIntValueToFloatAndMaybeWarn(row, value), *this->doubleBuilder.get());
            break;

        case STRING:
            // we already stored string data; nothing more is needed
            break;
    }
}


void
ColumnBuilder::convertIntToFloat64()
{
    int64_t len = this->intBuilderLength;

    // Strange design here. We _could_ build a separate codepath for
    // int8/int16/int32, knowing they'll never fail to cast to float.
    // Instead, we force this->intBuilder to build an int64 by adding
    // an int64 value to it -- a value we will then ignore.
    this->intBuilder->Append(std::numeric_limits<int64_t>::min());

    // Gather our ints in a format we can iterate.
    std::shared_ptr<arrow::Array> oldInts;
    ASSERT_ARROW_OK(this->intBuilder->Finish(&oldInts), "converting ints to array");
    auto int64Array = static_cast<arrow::NumericArray<arrow::Int64Type>*>(oldInts.get());
    const int64_t* int64s(int64Array->raw_values());

    // Convert existing ints to float -- warning as we go
    auto doubleBuilder = std::make_unique<arrow::DoubleBuilder>(arrow::default_memory_pool());
    ASSERT_ARROW_OK(doubleBuilder->Reserve(len), "allocating space for doubles"); // allow (faster) UnsafeAppend*
    for (int64_t i = 0; i < len; i++) {
        if (int64Array->IsNull(i)) {
            doubleBuilder->UnsafeAppendNull();
        } else {
            double floatValue = this->convertIntValueToFloatAndMaybeWarn(i, int64s[i]);
            doubleBuilder->UnsafeAppend(floatValue);
        }
    }

    this->intBuilder = nullptr;
    this->intBuilderLength = 0;
    this->doubleBuilder.reset(doubleBuilder.release());
    this->dtype = FLOAT;
}


void
ColumnBuilder::writeFloat64(int64_t row, double value)
{
    switch (this->dtype) {
        case UNTYPED:
            {
                this->doubleBuilder = std::make_unique<arrow::DoubleBuilder>(arrow::default_memory_pool());
                this->firstNumberRow = row;
                this->dtype = FLOAT;
            }
            storeFloat64Value(row, value, *this->doubleBuilder.get());
            break;

        case FLOAT:
            storeFloat64Value(row, value, *this->doubleBuilder.get());
            break;

        case INT:
            this->convertIntToFloat64();
            storeFloat64Value(row, value, *this->doubleBuilder.get());
            break;

        case STRING:
            // we already stored string data; nothing more is needed
            break;
    }
}


std::shared_ptr<arrow::Array>
ColumnBuilder::finish(size_t nRows)
{
    this->growToLength(nRows);
    std::shared_ptr<arrow::Array> ret;

    switch (this->dtype) {
        case UNTYPED:
        case STRING:
            ASSERT_ARROW_OK(this->stringBuilder.Finish(&ret), "finishing String array");
            this->dtype = UNTYPED;
            break;
        case INT:
            this->stringBuilder.Reset();
            ASSERT_ARROW_OK(this->intBuilder->Finish(&ret), "finishing Int array");
            this->intBuilder.reset(nullptr);
            this->intBuilderLength = 0;
            this->dtype = UNTYPED;
            break;
        case FLOAT:
            this->stringBuilder.Reset();
            ASSERT_ARROW_OK(this->doubleBuilder->Finish(&ret), "finishing Float array");
            this->doubleBuilder.reset(nullptr);
            this->dtype = UNTYPED;
            break;
    }

    return ret;
}


static bool
isColumnNameInvalid(std::string_view name)
{
    for (auto c : name) {
        return c < 0x20; // disallow control characters
    }
    return name.size() == 0; // disallow empty string
}


ColumnBuilder*
TableBuilder::createColumnOrNull(int64_t row, std::string_view name, Warnings& warnings)
{
    if (isColumnNameInvalid(name)) {
        warnings.warnColumnNameInvalid(row, name);
        return nullptr;
    }
    if (this->columnBuilders.size() == FLAGS_max_columns) {
        warnings.warnColumnSkipped(name);
        return nullptr;
    }

    auto builder = std::make_unique<ColumnBuilder>(std::string(name.begin(), name.end()));
    ColumnBuilder* ret(builder.get());

    this->columnBuilders.emplace_back(std::move(builder));
    this->lookup[name] = ret;
    return ret;
}

std::shared_ptr<arrow::Table>
TableBuilder::finish(size_t nRows, Warnings& warnings)
{
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(this->columnBuilders.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(this->columnBuilders.size());

    for (auto& columnBuilder : this->columnBuilders) {
        columnBuilder->growToLength(nRows);
        columnBuilder->warnOnErrors(warnings);
        if (columnBuilder->dtype == ColumnBuilder::UNTYPED) {
            warnings.warnColumnNull(columnBuilder->name);
        }
        std::shared_ptr<arrow::Array> column(columnBuilder->finish(nRows));
        std::shared_ptr<arrow::Field> field = arrow::field(columnBuilder->name, column->type());
        columns.push_back(column);
        fields.push_back(field);
    }

    this->lookup.clear();
    this->columnBuilders.clear();

    auto schema = arrow::schema(fields);
    return arrow::Table::Make(schema, columns, nRows);
}
