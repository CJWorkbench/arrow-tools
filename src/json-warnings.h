#ifndef ARROW_TOOLS_JSON_WARNINGS_H_
#define ARROW_TOOLS_JSON_WARNINGS_H_
#include <string>
#include <string_view>

#include "string-buffer.h"

struct Warnings
{
    // ------------------------------------------
    // Things that can go wrong with the entire file

    bool jsonParseError;
    size_t jsonParseErrorPos; // byte number, 0-based
    std::string jsonParseErrorEn; // English message

    std::string xlsError;
    std::string xlsxError;

    // We did not find an Array of records.
    bool badRoot;
    std::string badRootValue;
  
    // ------------------------------------------
    // Things that can go wrong with rows

    // We skipped some rows to fit our row limit
    size_t nRowsSkipped;

    // We stopped because we hit our memory limit
    bool stoppedOutOfMemory;

    // We ignored some records that weren't records.
    size_t nRowsInvalid;
    int64_t firstRowInvalidIndex; // 0-based
    std::string firstRowInvalid; // JSON-encoded representation

    // ------------------------------------------
    // Things that can go wrong with column names
    //
    // We can't *count* columns we don't process (because doing so could exhaust
    // memory). So we store enough to construct error messages for the user that
    // answer: "name one column?" and "are there other(s)?"

    // We skipped some columns to fit our column limit
    size_t nColumnsSkipped; // 0, 1 or 2 -- we can't count them all
    std::string firstColumnSkipped;

    // We inferred type=String because the values were all-null
    size_t nColumnsNull;
    std::string firstColumnNull;

    // We truncated some column names to fit our column-name limit
    size_t nColumnNamesTruncated; // we *can* count all these
    std::string firstColumnNameTruncated;

    // We nixed column names that we cannot use (e.g., name="")
    size_t nColumnNamesInvalid; // 0, 1 or 2 -- we can't count them all
    int64_t firstColumnNameInvalidRow; // 0-base
    std::string firstColumnNameInvalid;

    // We nixed some data because its column name was repeated.
    // e.g., the record {"x": 1, "x": 2} -- we will store 1, nix 2
    size_t nColumnNamesDuplicated; // 0, 1 or 2 -- we don't bother counting
    int64_t firstColumnNameDuplicatedRow; // 0-base
    std::string firstColumnNameDuplicated;

    // ------------------------------------
    // Things that can go wrong with values

    // We truncated values (strings or nested-object serializations, almost
    // certainly) because they exceed our value limit
    size_t nValuesTruncated; // truncated because the individual value is too big
    int64_t firstValueTruncatedRow; // 0-base
    std::string firstValueTruncatedColumn;

    // We converted int64=>float64 and lost data
    size_t nValuesLossyIntToFloat;
    int64_t firstValueLossyIntToFloatRow; // 0-base
    std::string firstValueLossyIntToFloatColumn;

    // We replaced Infinity with null and lost data
    size_t nValuesOverflowFloat;
    int64_t firstValueOverflowFloatRow; // 0-base
    std::string firstValueOverflowFloatColumn;

    // We replaced timestamps that 64-bit ns can't handle to null and lost data
    size_t nValuesOverflowTimestamp;
    int64_t firstValueOverflowTimestampRow; // 0-base
    std::string firstValueOverflowTimestampColumn;

    // We converted Number=>Text (users might not expect this)
    size_t nValuesNumberToText;
    int64_t firstValueNumberToTextRow; // 0-base
    std::string firstValueNumberToTextColumn;

    // We converted Timestamp=>Text (users might not expect this)
    size_t nValuesTimestampToText;
    int64_t firstValueTimestampToTextRow; // 0-base
    std::string firstValueTimestampToTextColumn;

    Warnings()
        : jsonParseError(false),
          jsonParseErrorPos(0),
          badRoot(false),
          nRowsSkipped(0),
          stoppedOutOfMemory(false),
          nRowsInvalid(0),
          firstRowInvalidIndex(0),
          nColumnsSkipped(0),
          nColumnsNull(0),
          nColumnNamesTruncated(0),
          nColumnNamesInvalid(0),
          firstColumnNameInvalidRow(0),
          nColumnNamesDuplicated(0),
          firstColumnNameDuplicatedRow(0),
          nValuesTruncated(0),
          firstValueTruncatedRow(0),
          nValuesLossyIntToFloat(0),
          firstValueLossyIntToFloatRow(0),
          nValuesOverflowFloat(0),
          firstValueOverflowFloatRow(0),
          nValuesOverflowTimestamp(0),
          firstValueOverflowTimestampRow(0),
          nValuesNumberToText(0),
          firstValueNumberToTextRow(0),
          nValuesTimestampToText(0),
          firstValueTimestampToTextRow(0)
    {
    }

    void warnJsonParseError(size_t pos, const std::string& en) {
        this->jsonParseError = true;
        this->jsonParseErrorPos = pos;
        this->jsonParseErrorEn = en;
    }

    void warnXlsParseError(const char* what) {
        this->xlsError = what;
    }

    void warnXlsParseError(const std::string_view& what) {
        this->xlsError = what;
    }

    void warnXlsxParseError(const char* what) {
        this->xlsxError = what;
    }

    void warnBadRoot(const StringBuffer& value) {
        this->badRoot = true;
        this->badRootValue = value.copyUtf8String();
    }

    void warnRowsSkipped(size_t nRows) {
        this->nRowsSkipped = nRows;
    }

    void warnStoppedOutOfMemory() {
        this->stoppedOutOfMemory = true;
    }

    void warnRowInvalid(int64_t row, const StringBuffer& json) {
        if (this->nRowsInvalid == 0) {
            this->firstRowInvalidIndex = row;
            this->firstRowInvalid = json.copyUtf8String();
        }
        this->nRowsInvalid++;
    }

    void warnColumnSkipped(std::string_view name) {
        if (this->nColumnsSkipped == 0) {
            this->nColumnsSkipped = 1;
            this->firstColumnSkipped = std::string(name.begin(), name.end());
        } else if (this->nColumnsSkipped == 1 && name != this->firstColumnSkipped) {
            this->nColumnsSkipped = 2;
        }
    }

    void warnColumnNull(const std::string& name) {
        // called once per column name per file
        if (this->nColumnsNull == 0) {
            this->firstColumnNull = name;
        }
        this->nColumnsNull++;
    }

    void warnColumnNameTruncated(const std::string& name) {
        // called once per column name per file
        if (this->nColumnNamesTruncated == 0) {
            this->firstColumnNameTruncated = name;
        }
        this->nColumnNamesTruncated++;
    }

    void warnColumnNameInvalid(int64_t row, std::string_view name) {
        // We don't remember invalid column names; so this can be called many times with the same name
        if (this->nColumnNamesInvalid == 0) {
            this->nColumnNamesInvalid = 1;
            this->firstColumnNameInvalidRow = row;
            this->firstColumnNameInvalid = std::string(name.begin(), name.end());
        } else if (this->nColumnNamesInvalid == 1 && name != this->firstColumnNameInvalid) {
            this->nColumnNamesInvalid = 2;
        }
    }

    void warnColumnNameDuplicated(int64_t row, const StringBuffer& keyBuf) {
        // This can be called many times with the same name -- and even the same name+row
        if (this->nColumnNamesDuplicated == 0) {
            this->nColumnNamesDuplicated = 1;
            this->firstColumnNameDuplicatedRow = row;
            this->firstColumnNameDuplicated = keyBuf.copyUtf8String();
        } else if (this->nColumnNamesDuplicated == 1 && keyBuf.toUtf8StringView() != this->firstColumnNameDuplicated) {
            this->nColumnNamesDuplicated = 2;
        }
    }

    void warnValueTruncated(int64_t row, const std::string& column) {
        if (this->nValuesTruncated == 0) {
            this->firstValueTruncatedRow = row;
            this->firstValueTruncatedColumn = column;
        }
        this->nValuesTruncated++;
    }

    void warnValuesLossyIntToFloat(size_t nValues, int64_t row, const std::string& column) {
        if (this->nValuesLossyIntToFloat == 0) {
            this->firstValueLossyIntToFloatRow = row;
            this->firstValueLossyIntToFloatColumn = column;
        }
        this->nValuesLossyIntToFloat += nValues;
    }

    void warnValuesOverflowFloat(size_t nValues, int64_t row, const std::string& column) {
        if (this->nValuesOverflowFloat == 0) {
            this->firstValueOverflowFloatRow = row;
            this->firstValueOverflowFloatColumn = column;
        }
        this->nValuesOverflowFloat += nValues;
    }

    void warnValuesNumberToText(size_t nValues, int64_t row, const std::string& column) {
        if (this->nValuesNumberToText == 0) {
            this->firstValueNumberToTextRow = row;
            this->firstValueNumberToTextColumn = column;
        }
        this->nValuesNumberToText += nValues;
    }

    void warnValuesTimestampToText(size_t nValues, int64_t row, const std::string& column) {
        if (this->nValuesTimestampToText == 0) {
            this->firstValueTimestampToTextRow = row;
            this->firstValueTimestampToTextColumn = column;
        }
        this->nValuesNumberToText += nValues;
    }

    void warnValuesOverflowTimestamp(size_t nValues, int64_t row, const std::string& column) {
        if (this->nValuesOverflowTimestamp == 0) {
            this->firstValueOverflowTimestampRow = row;
            this->firstValueOverflowTimestampColumn = column;
        }
        this->nValuesOverflowTimestamp += nValues;
    }
};

void printWarnings(const Warnings& warnings);

#endif  // ARROW_TOOLS_JSON_WARNINGS_H_
