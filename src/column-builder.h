#ifndef ARROW_TOOLS_COLUMN_BUILDER_H_
#define ARROW_TOOLS_COLUMN_BUILDER_H_

#include <memory>
#include <string_view>

#include <arrow/api.h>
#include <double-conversion/double-conversion.h>

class Warnings;

/*
 * Accumulate values for a column.
 *
 * Values may arrive mixed-type, but they will all be cast to a single output
 * type.
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
 *
 * Full type transition diagram:
 *
 *                / INT64 -----+
 *               /    |         \
 *              /     v          \
 *     UNTYPED +--- FLOAT64 ---+  \
 *             |\               \  \
 *             | \               \  \
 *             |  \ TIMESTAMP -+  \  \
 *              \               \  \  \
 *               \               v  v  v
 *                +-------------- STRING
 *
 * How to read this diagram:
 *
 * * When at UNTYPED and encountering a value of another type, transition to
 *   it.
 * * When at UNTYPED or any type and encountering null, stay.
 * * When at any type and encountering a compatible value, stay.
 * * When at INT64 and encountering a FLOAT64, transition to FLOAT64.
 * * Otherwise, transition to STRING and store every value as string.
 */
struct ColumnBuilder {
    std::string name;
    arrow::StringBuilder stringBuilder;
    std::unique_ptr<arrow::AdaptiveIntBuilder> intBuilder;
    std::unique_ptr<arrow::DoubleBuilder> doubleBuilder;
    std::unique_ptr<arrow::TimestampBuilder> timestampBuilder;
    size_t nNumbers;
    size_t firstNumberRow;
    size_t nTimestamps;
    size_t firstTimestampRow;
    size_t nLossyNumbers;
    size_t firstLossyNumberRow;
    size_t nOverflowNumbers;
    size_t firstOverflowNumberRow;
    size_t nOverflowTimestamps;
    size_t firstOverflowTimestampRow;
    double_conversion::StringToDoubleConverter doubleConverter;

    typedef enum _Dtype {
        UNTYPED,
        INT,
        FLOAT,
        TIMESTAMP,
        STRING
    } Dtype;
    Dtype dtype;

    ColumnBuilder(const std::string& name_ = "")
        : name(name_),
          stringBuilder(arrow::default_memory_pool()),
          nNumbers(0),
          firstNumberRow(0),
          nTimestamps(0),
          firstTimestampRow(0),
          nLossyNumbers(0),
          firstLossyNumberRow(0),
          nOverflowNumbers(0),
          firstOverflowNumberRow(0),
          nOverflowTimestamps(0),
          firstOverflowTimestampRow(0),
          doubleConverter(0, 0, 0, nullptr, nullptr),
          dtype(UNTYPED)
    {
    }

    void setName(std::string_view str) {
      this->name = str;
    }

    void writeString(int64_t row, std::string_view str);
    void writeNumber(int64_t row, std::string_view str);
    void writeParsedNumber(int64_t row, double value, std::string_view str);
    void writeParsedTimestamp(int64_t row, int64_t nsSinceEpoch, bool isOverflow, std::string_view str);

    size_t length() const {
        return this->stringBuilder.length();
    }

    void growToLength(int64_t nRows);
    std::shared_ptr<arrow::Array> finish(size_t nRows);
    void warnOnErrors(Warnings& warnings);

    static bool isColumnNameInvalid(std::string_view name);

private:
    void writeInt64(int64_t row, int64_t value);
    void writeFloat64(int64_t row, double value);
    void writeTimestamp(int64_t row, int64_t nsSinceEpoch);
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

struct StringColumnBuilder {
    arrow::StringBuilder arrayBuilder;
    size_t nextRowIndex;

    StringColumnBuilder() : arrayBuilder(arrow::default_memory_pool()), nextRowIndex(0) {}

    void growToLength(size_t nRows);
    void writeValue(size_t row, std::string_view value);
};

#endif  // ARROW_TOOLS_COLUMN_BUILDER_H_
