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
    int64_t intBuilderLength; // intBuilder->length() is wrong in Arrow 0.15.1: https://issues.apache.org/jira/browse/ARROW-7281
    std::unique_ptr<arrow::DoubleBuilder> doubleBuilder;
    size_t nNumbers;
    size_t firstNumberRow;
    size_t nLossyNumbers;
    size_t firstLossyNumberRow;
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
        : name(name_),
          stringBuilder(arrow::default_memory_pool()),
          intBuilderLength(0),
          nNumbers(0),
          firstNumberRow(0),
          nLossyNumbers(0),
          firstLossyNumberRow(0),
          nOverflowNumbers(0),
          firstOverflowNumberRow(0),
          doubleConverter(0, 0, 0, nullptr, nullptr),
          dtype(UNTYPED)
    {
    }

    void writeString(int64_t row, std::string_view str);
    void writeNumber(int64_t row, std::string_view str);

    size_t length() const {
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

    std::shared_ptr<arrow::Array> finish(size_t nRows);

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
    void writeInt64(int64_t row, int64_t value);
    void writeFloat64(int64_t row, double value);
    void convertIntToFloat64();

    double convertIntValueToFloatAndMaybeWarn(int64_t row, int64_t intValue)
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
};


class TableBuilder {
public:
    struct FoundColumnOrNull {
        ColumnBuilder* columnOrNull;
        bool isNew;
    };

    FoundColumnOrNull findOrCreateColumnOrNull(int64_t row, std::string_view name, Warnings& warnings) {
        auto iter = this->lookup.find(name);
        if (iter == this->lookup.end()) {
            // not found
            ColumnBuilder* column = this->createColumnOrNull(row, name, warnings);
            return { column, column ? true : false };
        } else {
            return { iter->second, false };
        }
    }

    /**
     * Destructively build an arrow::Table
     *
     * This resets the TableBuilder to its initial state (and frees RAM).
     */
    std::shared_ptr<arrow::Table> finish(size_t nRows, Warnings& warnings);

private:
    // Chose an unordered_map because in the worst case we have a few hundred
    // keys.... and unordered_map should be quickest in that worst case. See
    // https://playfulprogramming.blogspot.com/2017/08/performance-of-flat-maps.html
    std::vector<std::unique_ptr<ColumnBuilder>> columnBuilders;
    std::unordered_map<std::string_view, ColumnBuilder*> lookup;

    ColumnBuilder* createColumnOrNull(int64_t row, std::string_view name, Warnings& warnings);
};

#endif  // ARROW_TOOLS_JSON_TABLE_BUILDER_H_
