#include <cstdio>

#include <gflags/gflags.h>

#include "json-warnings.h"
#include "string-buffer.h"

DECLARE_uint64(max_rows);
DECLARE_uint32(max_columns);
DECLARE_uint32(max_bytes_per_value);
DECLARE_uint32(max_bytes_per_column_name);
DECLARE_uint64(max_bytes_total);

void printWarnings(const Warnings& warnings)
{
    if (warnings.jsonParseError) {
        printf("JSON parse error at byte %d: %s\n", warnings.jsonParseErrorPos, warnings.jsonParseErrorEn.c_str());
    }
    if (warnings.badRoot) {
        printf("JSON is not an Array or Object containing an Array; got: %s\n", warnings.badRootValue.c_str());
    }

    if (warnings.nRowsSkipped) {
        printf("skipped %d rows (after row limit of %d)\n", warnings.nRowsSkipped, FLAGS_max_rows);
    }
    if (warnings.stoppedOutOfMemory) {
        printf("stopped at limit of %d bytes of data\n", FLAGS_max_bytes_total);
    }
    if (warnings.nRowsInvalid) {
        printf("skipped %d non-Object records; example Array item %d: %s\n", warnings.nRowsInvalid, warnings.firstRowInvalidIndex, warnings.firstRowInvalid.c_str());
    }

    if (warnings.nColumnsSkipped) {
        printf("skipped column %s%s (after column limit of %d)\n", warnings.firstColumnSkipped.c_str(), warnings.nColumnsSkipped > 1 ? " and more" : "", FLAGS_max_columns);
    }
    if (warnings.nColumnsNull) {
        printf("chose string type for null column %s%s\n", warnings.firstColumnNull.c_str(), warnings.nColumnsNull > 1 ? " and more" : "");
    }
    if (warnings.nColumnNamesTruncated) {
        printf("truncated %d column names; example %s\n", warnings.nColumnNamesTruncated, warnings.firstColumnNameTruncated.c_str());
    }
    if (warnings.nColumnNamesInvalid) {
        // JSON-encode. Max size of an invalid column name is sum of:
        // * 2 bytes for quotation marks
        // * 6 bytes per char (because '\0' expands to "\u0000")
        //   (No need to worry about UTF-16 surrogate pairs: we don't escape
        //   anything outside of ASCII, and we don't pass-through surrogate
        //   pairs.)
        // * 1 byte for ending '\0' (to help printf)
        StringBuffer buf(3 + warnings.firstColumnNameInvalid.size() * 6);
        buf.appendAsJsonQuotedString(reinterpret_cast<const uint8_t*>(warnings.firstColumnNameInvalid.c_str()), warnings.firstColumnNameInvalid.size());
        buf.append('\0');
        printf("ignored invalid column %s%s\n", reinterpret_cast<const char*>(&buf.bytes[0]), warnings.nColumnNamesInvalid > 1 ? " and more" : "");
    }
    if (warnings.nColumnNamesDuplicated) {
        printf("ignored duplicate column %s%s starting at row %d\n", warnings.firstColumnNameDuplicated.c_str(), warnings.nColumnNamesDuplicated > 1 ? " and more" : "", warnings.firstColumnNameDuplicatedRow);
    }

    if (warnings.nValuesTruncated) {
        printf("truncated %d values (value byte limit is %d; see row %d column %s)\n", warnings.nValuesTruncated, FLAGS_max_bytes_per_value, warnings.firstValueTruncatedRow, warnings.firstValueTruncatedColumn.c_str());
    }
    if (warnings.nValuesLossyIntToFloat) {
        printf("lost precision converting %d int64 Numbers to float64; see row %d column %s\n", warnings.nValuesLossyIntToFloat, warnings.firstValueLossyIntToFloatRow, warnings.firstValueLossyIntToFloatColumn.c_str());
    }
    if (warnings.nValuesOverflowFloat) {
        printf("replaced infinity with null for %d Numbers; see row %d column %s\n", warnings.nValuesOverflowFloat, warnings.firstValueOverflowFloatRow, warnings.firstValueOverflowFloatColumn.c_str());
    }
    if (warnings.nValuesNumberToText) {
        printf("interpreted %d Numbers as String; see row %d column %s\n", warnings.nValuesNumberToText, warnings.firstValueNumberToTextRow, warnings.firstValueNumberToTextColumn.c_str());
    }
}
