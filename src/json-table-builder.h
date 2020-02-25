#ifndef ARROW_TOOLS_JSON_TABLE_BUILDER_H_
#define ARROW_TOOLS_JSON_TABLE_BUILDER_H_

#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "column-builder.h"


class Warnings;


class TableBuilder {
public:
    struct FoundColumnOrNull {
        ColumnBuilder* columnOrNull;
        bool isNew;
    };

    FoundColumnOrNull findOrCreateColumnOrNull(int64_t row, std::string_view name, Warnings& warnings);

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
