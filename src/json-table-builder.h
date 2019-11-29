#ifndef ARROW_TOOLS_JSON_TABLE_BUILDER_H_
#define ARROW_TOOLS_JSON_TABLE_BUILDER_H_

#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <double-conversion/double-conversion.h>

#include "json-warnings.h"

/*
 * Accumulate values for a JSON column.
 *
 * The rules of accumulation:
 *
 * * When a column is created, it stores undetermined data.
 * * The type of the first datum decides the type of the column. A JSON number
 *   with a decimal point or "e" power is float64; otherwise, int64. A
 *   too-large int64 becomes a float64 _with a warning_ (see next rule).
 * * If we're at type=int64 and we encounter a float64, convert and warn about
 *   lossiness -- for this int (if it's a too-big int64), for all previous ints
 *   that lose data in conversion, and for all future ints that lose data in
 *   conversion.
 * * Even when parsing int64/float64, *also* store input bytes as String. If
 *   we're fed a String value, convert to String by swapping in all the input
 *   bytes. Warn for every int64/float64 value converted to String.
 */
struct ColumnBuilder {
    std::string name;
    arrow::StringBuilder stringBuilder;
    std::unique_ptr<arrow::AdaptiveIntBuilder> intBuilder;
    size_t intBuilderLength; // intBuilder->length() is wrong in Arrow 0.15.1: https://issues.apache.org/jira/browse/ARROW-7281
    std::unique_ptr<arrow::DoubleBuilder> doubleBuilder;
    size_t firstNumberRow;
    size_t nNumbers;
    size_t firstLossyNumberRow;
    size_t nLossyNumbers;
    size_t nOverflowNumbers;
    size_t firstOverflowNumberRow;
    double_conversion::StringToDoubleConverter doubleConverter;

    typedef enum _Dtype {
        UNTYPED,
        INT,
        FLOAT,
        STRING
    } Dtype;
    Dtype dtype;

    ColumnBuilder(const std::string& name_)
        : stringBuilder(arrow::default_memory_pool()),
          name(name_),
          intBuilderLength(0),
          nNumbers(0),
          firstNumberRow(0),
          nLossyNumbers(0),
          firstLossyNumberRow(0),
          dtype(UNTYPED),
          doubleConverter(0, 0, 0, nullptr, nullptr)
    {
    }

    void writeString(size_t row, std::string_view str)
    {
        this->storeStringValue(row, str);

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

    void writeNumber(size_t row, std::string_view str)
    {
        this->storeStringValue(row, str);
        this->nNumbers++;

        // JSON number format is blissfully restrictive: no leading "+" or 0,
        // no whitespace. We can pick a parser simply by examining str.
        const static std::string minInt64String = std::to_string(std::numeric_limits<int64_t>::min());
        const static std::string maxInt64String = std::to_string(std::numeric_limits<int64_t>::max());
        bool parseDouble = (
            // we always parse exponential notation as float.
            // (And of course, decimals are floats.)
            (str.find_first_of(".eE") != std::string_view::npos)
            // magic numbers differ for negative and positive numbers
            || (
                str[0] == '-' ? (
                    // too many digits
                    str.size() > minInt64String.size()
                    // same length and lexicographically too big
                    || (str.size() == minInt64String.size() && str > minInt64String)
                ) : (
                    // too many digits
                    str.size() > maxInt64String.size()
                    // same length and lexicographically too big
                    || (str.size() == maxInt64String.size() && str > maxInt64String)
                )
            )
        );

        if (parseDouble) {
            // [2019-11-28] GCC 8.3.0 std::from_chars() does not do double conversion
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
        } else {
            int64_t value;
            // Guaranteed success -- because our `parseDouble` logic is infallible!
            std::from_chars(str.begin(), str.end(), value);
            this->writeInt64(row, value);
        }
    }

    uint32_t length() const {
        return this->stringBuilder.length();
    }

    void growToLength(size_t nRows) {
        this->stringBuilder.AppendNulls(nRows - this->stringBuilder.length());
        switch (this->dtype) {
            case UNTYPED:
            case STRING:
                break;
            case INT:
                this->intBuilder->AppendNulls(nRows - this->intBuilderLength);
                this->intBuilderLength = nRows;
                break;
            case FLOAT:
                this->doubleBuilder->AppendNulls(nRows - this->doubleBuilder->length());
                break;
        }
    }

    std::shared_ptr<arrow::Array> finish() {
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

    void warnOnErrors(Warnings& warnings)
    {
        if (this->dtype == STRING && this->nNumbers) {
            warnings.warnValuesNumberToText(this->nNumbers, this->firstNumberRow, this->name);
        }
        if (this->dtype == FLOAT && this->nLossyNumbers) {
            warnings.warnValuesLossyIntToFloat(this->nLossyNumbers, this->firstLossyNumberRow, this->name);
        }
        if (this->dtype == FLOAT && this->nOverflowNumbers) {
            warnings.warnValuesLossyIntToFloat(this->nOverflowNumbers, this->firstOverflowNumberRow, this->name);
        }
    }

private:

    void storeStringValue(size_t row, std::string_view str)
    {
        // Called no matter what the input
        ASSERT_ARROW_OK(this->stringBuilder.Reserve(row + 1 - this->stringBuilder.length()), "reserving space for Strings");
        ASSERT_ARROW_OK(this->stringBuilder.ReserveData(str.size()), "reserving space for String bytes");

        if (row > this->stringBuilder.length()) {
            // Arrow 0.15.1 is missing UnsafeAppendNulls(); but this can't error
            this->stringBuilder.AppendNulls(row - this->stringBuilder.length());
        }
        this->stringBuilder.UnsafeAppend(str.begin(), str.size());
    }

    void storeIntValue(size_t row, int64_t value)
    {
        if (row > this->intBuilderLength) {
            ASSERT_ARROW_OK(this->intBuilder->AppendNulls(row - this->intBuilderLength), "adding null integers");
        }
        ASSERT_ARROW_OK(this->intBuilder->Append(value), "adding integer");
        this->intBuilderLength = row + 1;
    }

    void storeFloat64Value(size_t row, double value)
    {
        ASSERT_ARROW_OK(this->doubleBuilder->Reserve(row + 1 - this->doubleBuilder->length()), "reserving space for Floats");
        if (row > this->doubleBuilder->length()) {
            // Arrow 0.15.1 is missing UnsafeAppendNulls(); but this can't error
            this->doubleBuilder->AppendNulls(row - this->doubleBuilder->length());
        }
        this->doubleBuilder->UnsafeAppend(value);
    }

    void writeInt64(size_t row, int64_t value)
    {
        switch (this->dtype) {
            case UNTYPED:
                {
                    this->intBuilder = std::make_unique<arrow::AdaptiveIntBuilder>(arrow::default_memory_pool());
                    this->firstNumberRow = row;
                    this->dtype = INT;
                }
                this->storeIntValue(row, value);
                break;

            case INT:
                this->storeIntValue(row, value);
                break;

            case FLOAT:
                this->storeFloat64Value(row, this->convertIntValueToFloatAndMaybeWarn(row, value));
                break;

            case STRING:
                // we already stored string data; nothing more is needed
                break;
        }
    }

    void writeFloat64(size_t row, double value)
    {
        switch (this->dtype) {
            case UNTYPED:
                {
                    this->doubleBuilder = std::make_unique<arrow::DoubleBuilder>(arrow::default_memory_pool());
                    this->firstNumberRow = row;
                    this->dtype = FLOAT;
                }
                this->storeFloat64Value(row, value);
                break;

            case FLOAT:
                this->storeFloat64Value(row, value);
                break;

            case INT:
                this->convertIntToFloat64();
                this->storeFloat64Value(row, value);
                break;

            case STRING:
                // we already stored string data; nothing more is needed
                break;
        }
    }

    double convertIntValueToFloatAndMaybeWarn(size_t row, int64_t intValue)
    {
        double floatValue = static_cast<double>(intValue);
        if (static_cast<int64_t>(floatValue) != intValue) {
            if (this->nLossyNumbers == 0) {
                this->firstLossyNumberRow = row;
            }
            this->nLossyNumbers++;
        }
        return floatValue;
    }

    void convertIntToFloat64()
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
};


struct TableBuilder {
    // Chose an unordered_map because in the worst case we have a few hundred
    // keys.... and unordered_map should be quickest in that worst case. See
    // https://playfulprogramming.blogspot.com/2017/08/performance-of-flat-maps.html
    std::vector<std::unique_ptr<ColumnBuilder>> columnBuilders;
    std::unordered_map<std::string_view, ColumnBuilder*> lookup;

    struct FoundColumnOrNull {
        ColumnBuilder* columnOrNull;
        bool isNew;
    };

    FoundColumnOrNull findOrCreateColumnOrNull(size_t row, std::string_view name, Warnings& warnings) {
        auto iter = this->lookup.find(name);
        if (iter == this->lookup.end()) {
            // not found
            ColumnBuilder* column = this->createColumnOrNull(row, name, warnings);
            return { column, column ? true : false };
        } else {
            return { iter->second, false };
        }
    }

    ColumnBuilder* createColumnOrNull(size_t row, std::string_view name, Warnings& warnings) {
        // TODO decide upon legal column-name pattern
        if (!name.size()) {
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

    /**
     * Destructively build an arrow::Table
     *
     * This resets the TableBuilder to its initial state (and frees RAM).
     */
    std::shared_ptr<arrow::Table> finish(Warnings& warnings) {
        // Find nRows, so we can make all columns the same length
        uint32_t nRows = 0;
        for (const auto& columnBuilder : this->columnBuilders) {
            nRows = std::max(nRows, columnBuilder->length());
            columnBuilder->warnOnErrors(warnings);
        }

        // Build columns+fields
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(this->columnBuilders.size());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(this->columnBuilders.size());

        for (auto& columnBuilder : this->columnBuilders) {
            columnBuilder->growToLength(nRows);

            if (columnBuilder->dtype == ColumnBuilder::UNTYPED) {
                warnings.warnColumnNull(columnBuilder->name);
            }
            std::shared_ptr<arrow::Array> column(columnBuilder->finish());
            std::shared_ptr<arrow::Field> field = arrow::field(columnBuilder->name, column->type());
            columns.push_back(column);
            fields.push_back(field);
        }

        this->lookup.clear();
        this->columnBuilders.clear();

        auto schema = arrow::schema(fields);
        return arrow::Table::Make(schema, columns, nRows);
    }
};

#endif  // ARROW_TOOLS_JSON_TABLE_BUILDER_H_
