#include <gflags/gflags.h>

#include "json-table-builder.h"
#include "json-warnings.h"

DECLARE_uint32(max_columns);

TableBuilder::FoundColumnOrNull
TableBuilder::findOrCreateColumnOrNull(int64_t row, std::string_view name, Warnings& warnings)
{
    auto iter = this->lookup.find(name);
    if (iter == this->lookup.end()) {
        // not found
        ColumnBuilder* column = this->createColumnOrNull(row, name, warnings);
        return { column, column ? true : false };
    } else {
        return { iter->second, false };
    }
}

ColumnBuilder*
TableBuilder::createColumnOrNull(int64_t row, std::string_view name, Warnings& warnings)
{
    if (ColumnBuilder::isColumnNameInvalid(name)) {
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
