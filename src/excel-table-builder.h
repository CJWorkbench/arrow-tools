#ifndef ARROW_TOOLS_EXCEL_TABLE_BUILDER_H_
#define ARROW_TOOLS_EXCEL_TABLE_BUILDER_H_

#include <memory>
#include <string_view>
#include <vector>

#include <arrow/api.h>
#include <xlnt/utils/calendar.hpp>

#include "column-builder.h"
#include "json-warnings.h"


struct ExcelTableBuilder {
    int64_t maxRowSeen; // max row index there was a cell for (may have been ignored)
    int64_t maxRowHandled; // max row index of the output table
    uint64_t nBytesTotal;
    Warnings warnings;
    std::vector<std::unique_ptr<ColumnBuilder>> columns;
    StringBuffer colnameTruncator;
    StringBuffer valueTruncator;

    ExcelTableBuilder();

    static std::string buildDefaultColumnName(uint32_t index);

    typedef enum _NextAction {
        CONTINUE,
        STOP, // STOP means "ignore the rest of the file"
    } NextAction;

    ColumnBuilder* column(size_t i);
    std::shared_ptr<arrow::Table> finish();

    void addNumber(ColumnBuilder& cb, int64_t row, double value, std::string_view strValue) const;
    void addString(ColumnBuilder& cb, int64_t row, std::string_view strValue) const;

    /**
     * Add `value` as a datetime.
     *
     * The cell value as a double (which is how Excel stores it -- whole part
     * days, fractional part fraction-of-the-day). We convert to nsSinceEpoch
     * or pass isOverflow.
     */
    void addDatetime(ColumnBuilder& cb, int64_t row, double value, xlnt::calendar calendar, std::string_view strValue) const;
};


#endif  // ARROW_TOOLS_EXCEL_TABLE_BUILDER_H_
