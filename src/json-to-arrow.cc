#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <string_view>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

#include <arrow/api.h>
#include <gflags/gflags.h>

#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/reader.h"

DEFINE_uint64(max_rows, std::numeric_limits<uint64_t>::max(), "Skip rows after parsing this many");
DEFINE_uint32(max_columns, std::numeric_limits<uint32_t>::max(), "Skip columns after parsing this many");
DEFINE_uint32(max_bytes_per_value, 1024 * 32, "Truncate each value to at most this size");
DEFINE_uint32(max_bytes_per_error_value, 100, "Truncate each error-message value to at most this size");
DEFINE_uint64(max_bytes_per_column_name, 1024, "Truncate each column header to at most this size");
DEFINE_uint64(max_bytes_total, std::numeric_limits<uint64_t>::max(), "Truncate file if it surpasses this many bytes of useful data");

#include "common.h"
#include "json-table-builder.h"
#include "json-warnings.h"
#include "string-buffer.h"

const size_t READ_BUFFER_SIZE = 1024 * 64; // magic number copied from https://rapidjson.org/md_doc_stream.html

struct JsonHandler : rapidjson::BaseReaderHandler<rapidjson::UTF8<uint8_t>> {
    typedef enum _State {
        START,
        IN_ROOT_OBJECT,
        IN_RECORD_ARRAY,
        IN_RECORD,
        DONE, // DONE means "ignore the rest of the stream". RapidJSON should still raise JSON errors.
    } State;

    State state;
    size_t row;
    bool isRowPartiallyWritten;
    uint64_t nBytesTotal;
    StringBuffer keyBuf;
    StringBuffer valueBuf;
    StringBuffer errorBuf;
    TableBuilder tableBuilder;
    Warnings warnings;

    // this->column is null when we aren't in a record. It's also null when
    // we _are_ in a record, but we've exceeded FLAGS_max_columns. In the
    // too-many-columns case, we ignore the value and skip to the end.
    ColumnBuilder* column;

    /// Number of "]" and/or "}" until we finish parsing this Object/Array.
    ///
    /// This is used when:
    ///
    /// * We're in IN_ROOT_OBJECT, and we want to ignore non-Array values.
    /// * We're in IN_RECORD, and we've encountered an Object/Array value. We
    ///   serialize to this->valueBuf in that case.
    /// * We're IN_ARRAY, and we've encountered an Array value. We serialize
    ///   to this->errorBuf in that case.
    size_t nestLevel;

    /// Flag used alongside nestLevel to help with serialization.
    ///
    /// The flag is only used when we're serializing to this->valueBuf or
    /// this->valueBuf. (See `nestLevel`.)
    ///
    /// In that context, it is set in the following situations:
    ///
    /// * We've just written a value to the Array. If another value comes, we
    ///   want to serialize a comma first.
    /// * We're just written a value to the Object. If another *key* comes, we
    ///   want to serialize a comma first.
    ///
    /// It's not intuitive; but it turns out a boolean is all we need.
    bool nestWantComma;

    JsonHandler()
        : state(START),
          row(0),
          isRowPartiallyWritten(false),
          nBytesTotal(0),
          keyBuf(FLAGS_max_bytes_per_column_name),
          valueBuf(FLAGS_max_bytes_per_value),
          errorBuf(FLAGS_max_bytes_per_error_value),
          column(nullptr),
          nestLevel(0),
          nestWantComma(false)
    {
    }

    bool Null()
    {
        switch (this->state) {
            case START:
                this->errorBuf.append("null", 4);
                this->warnings.warnBadRoot(this->errorBuf);
                this->errorBuf.reset();
                this->state = DONE;
                break;

            case IN_ROOT_OBJECT:
                // We're searching the root object for a sensible Array. This isn't it.
                break;

            case DONE:
                // we don't care -- we ignore all valid JSON
                break;

            case IN_RECORD_ARRAY:
                this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->errorBuf);
                this->errorBuf.append("null", 4);

                if (this->nestLevel == 0) {
                    this->warnings.warnRowInvalid(this->row, this->errorBuf);
                    this->errorBuf.reset();
                }
                break;

            case IN_RECORD:
                if (this->column) {
                    if (this->nestLevel > 0) {
                        this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->valueBuf);
                        this->valueBuf.append("null", 4);
                    } else {
                        this->finishColumnWithNull();
                    }
                }
                break;
        }
        return true;
    }

    bool Bool(bool b)
    {
        const char* value = &"falsetrue"[b ? 5 : 0];
        size_t len = b ? 4 : 5;

        switch (this->state) {
            case START:
                this->errorBuf.append(value, len);
                this->warnings.warnBadRoot(this->errorBuf);
                this->errorBuf.reset();
                this->state = DONE;
                break;

            case IN_ROOT_OBJECT:
                // We're searching for a sensible Array. Ignore this value.
                break;

            case DONE:
                break;

            case IN_RECORD_ARRAY:
                this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->errorBuf);
                this->errorBuf.append(value, len);
                if (this->nestLevel == 0) {
                    this->warnings.warnRowInvalid(this->row, this->errorBuf);
                    this->errorBuf.reset();
                }
                break;

            case IN_RECORD:
                if (this->column) {
                    if (this->nestLevel == 0) {
                        this->valueBuf.append(value, len);
                        this->finishColumnWithStringValue();
                    } else {
                        this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->valueBuf);
                        this->valueBuf.append(value, len);
                    }
                }
                break;
        }
        return true;
    }

    bool RawNumber(const uint8_t* str, uint32_t len, bool copy)
    {
        switch (this->state) {
            case START:
                this->errorBuf.append(str, len);
                this->warnings.warnBadRoot(this->errorBuf);
                this->errorBuf.reset();
                break;

            case IN_ROOT_OBJECT:
                // We're searching for a sensible Array. Ignore this value.
                break;

            case DONE:
                break;

            case IN_RECORD_ARRAY:
                this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->errorBuf);
                this->errorBuf.append(str, len);
                if (this->nestLevel == 0) {
                    this->warnings.warnRowInvalid(this->row, this->errorBuf);
                    this->errorBuf.reset();
                }
                break;

            case IN_RECORD:
                if (this->column) {
                    this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->valueBuf);
                    this->valueBuf.append(str, len); // we know str is valid JSON
                    if (this->nestLevel == 0) {
                        this->finishColumnWithNumberValue();
                    }
                }
                break;
        }
        return true;
    }

    bool String(const uint8_t* str, uint32_t len, bool copy)
    {
        switch (this->state) {
            case START:
                this->errorBuf.appendAsJsonQuotedString(str, len);
                this->warnings.warnBadRoot(this->errorBuf);
                this->errorBuf.reset();
                this->state = DONE;
                break;

            case IN_ROOT_OBJECT:
                // We're searching for an Array. This isn't it.
                break;

            case DONE:
                break;

            case IN_RECORD_ARRAY:
                this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->errorBuf);
                this->errorBuf.appendAsJsonQuotedString(str, len);
                if (this->nestLevel == 0) {
                    this->warnings.warnRowInvalid(this->row, this->errorBuf);
                    this->errorBuf.reset();
                }
                break;

            case IN_RECORD:
                if (this->column) {
                    if (this->nestLevel > 0) {
                        this->appendCommaAndExpectFutureCommaIfWeAreSerializing(this->valueBuf);
                        this->valueBuf.appendAsJsonQuotedString(str, len);
                    } else {
                        // Use valueBuf to truncate the string
                        this->valueBuf.append(str, len);
                        if (this->valueBuf.hasOverflow()) {
                            this->warnings.warnValueTruncated(this->row, this->column->name);
                        }
                        this->finishColumnWithStringValue();
                    }
                }
                break;
        }
        return true;
    }

    bool Key(const uint8_t* str, uint32_t len, bool copy)
    {
        switch (this->state) {
            case START: // impossible
            case DONE:
                break;

            case IN_ROOT_OBJECT:
                // We're happy to see a key; but we don't actually output it so we
                // can ignore it.
                break;

            case IN_RECORD_ARRAY:
                // We're in an Object that is _not_ the direct child of the record
                // array. So this must be nested within a sub-Array -- e.g., the file
                // [[{"x": "y"}]]. We're serializing this record to build a warning; so
                // serialize.
                //
                // this->nestLevel > 0, always.
                if (this->nestWantComma) {
                    this->errorBuf.append(',');
                }
                this->errorBuf.appendAsJsonQuotedString(str, len);
                this->errorBuf.append(':');
                // next comes the value. When it comes, don't prepend a comma
                this->nestWantComma = false;
                break;

            case IN_RECORD:
                if (this->nestLevel == 0) {
                    if (this->row < FLAGS_max_rows) {
                        // Enter a column
                        this->keyBuf.append(str, len);
                        std::string_view name(this->keyBuf.toUtf8StringView());
                        TableBuilder::FoundColumnOrNull foundColumn = this->tableBuilder.findOrCreateColumnOrNull(this->row, name, this->warnings);
                        if (foundColumn.columnOrNull) {
                            if (foundColumn.columnOrNull->length() > this->row) {
                                // do not set this->column: we already have a value
                                // in this row for this column.
                                this->warnings.warnColumnNameDuplicated(this->row, this->keyBuf);
                            } else {
                                this->column = foundColumn.columnOrNull;
                                if (foundColumn.isNew && this->keyBuf.hasOverflow()) {
                                    this->warnings.warnColumnNameTruncated(foundColumn.columnOrNull->name);
                                }
                            }
                        }
                        this->keyBuf.reset();
                    } else {
                        // We've hit our row limit. We still parse the rest of
                        // the file, to find errors and count the number of
                        // rows we missed. But we no longer record any data.
                        // (this->column == nullptr, so the record's values
                        // won't be stored anywhere.
                    }
                } else {
                    if (this->column) {
                        // We're serializing a nested value. Write '"key":'
                        if (this->nestWantComma) {
                            this->valueBuf.append(',');
                        }
                        this->valueBuf.appendAsJsonQuotedString(str, len);
                        this->valueBuf.append(':');
                        this->nestWantComma = false;
                    }
                }
                break;
        }
        return true;
    }

    bool StartObject()
    {
        switch (this->state) {
            case START:
                this->state = IN_ROOT_OBJECT;
                break;

            case DONE:
                break;

            case IN_ROOT_OBJECT:
                // we're entering a nested object. Ignore it.
                this->nestLevel++;
                break;

            case IN_RECORD_ARRAY:
                if (this->nestLevel > 0) {
                    // We're nested inside an Array of Arrays. We're building
                    // a description of the value for the error message.
                    this->errorBuf.append('{');
                    this->nestLevel++;
                    this->nestWantComma = false;
                } else {
                    // Yay! Let's enter a record.
                    this->state = IN_RECORD;
                }
                break;

            case IN_RECORD:
                // A _value_ is an Object or Array.
                // Either we start serializing, or we continue serializing.
                // (The logic is the same either way.)
                if (this->column) {
                    if (this->nestWantComma) {
                        this->valueBuf.append(',');
                    }
                    this->valueBuf.append('{');
                }
                this->nestLevel++;
                this->nestWantComma = false;
                break;
        }
        return true;
    }

    bool EndObject(size_t nMembers)
    {
        switch (this->state) {
            case START: // impossible
            case DONE:
                break;

            case IN_ROOT_OBJECT:
                if (this->nestLevel == 0) {
                    this->state = DONE; // We never found any values
                } else {
                    // we're dancing around in a nested object in the root object
                    this->nestLevel--;
                }
                break;

            case IN_RECORD_ARRAY:
                // We're serializing a nested array -- for a doc like [[{"x": "y"}]]
                //                                      we are here .............^
                //
                // we know nestLevel > 0 because otherwise we'd get EndArray.
                this->errorBuf.append('}');
                this->nestLevel--;
                this->nestWantComma = true; // we just ended a value
                break;

            case IN_RECORD:
                if (this->nestLevel == 0) {
                    // End of record. Now we expect a new record.
                    this->row++;
                    this->isRowPartiallyWritten = false;
                    this->state = IN_RECORD_ARRAY;
                } else {
                    // We're in a nested object inside a column
                    this->nestLevel--;

                    if (this->column) {
                        this->valueBuf.append('}');
                        this->nestWantComma = (this->nestLevel > 0);

                        if (this->nestLevel == 0) {
                            this->finishColumnWithStringValue();
                        }
                    }
                }
                break;
        }
        return true;
    }

    bool StartArray()
    {
        switch (this->state) {
            case START:
                // Yay! The root is an array
                this->state = IN_RECORD_ARRAY;
                break;

            case DONE:
                break;

            case IN_ROOT_OBJECT:
                if (this->nestLevel == 0) {
                    // Yay! The root is an Object, and we just found the first Array within.
                    // We'll make this our record array.
                    this->state = IN_RECORD_ARRAY;
                } else {
                    // We're in an object nested within an object. Ignore.
                    this->nestLevel++;
                }
                break;

            case IN_RECORD_ARRAY:
                // The "record" is actually an Array. Serialize it so we can produce a
                // good error message.
                //
                // For a file like [[1, 2, 3, {"x": ["y"]]]
                //   We are here....^    or here....^
                this->errorBuf.append('[');
                this->nestLevel++;
                this->nestWantComma = false;
                break;

            case IN_RECORD:
                // We're serializing a non-primitive JSON value in a column
                if (this->column) {
                    if (this->nestWantComma) {
                        this->valueBuf.append(',');
                    }
                    this->valueBuf.append('[');
                    this->nestWantComma = false;
                }
                this->nestLevel++;
                break;
        }

        return true;
    }

    bool EndArray(size_t nMembers)
    {
        switch (this->state)
        {
            case START: // impossible
            case DONE:
                break;

            case IN_ROOT_OBJECT:
                // We're in an Array nested somewhere inside an Object inside
                // the root object. (We _must_ be nested, because otherwise
                // state would be IN_RECORD_ARRAY.)
                this->nestLevel--;
                break;

            case IN_RECORD_ARRAY:
                if (this->nestLevel > 0) {
                    // The "record" is actually an Array. Serialize it. If we finish it,
                    // warn.
                    //
                    // In the example doc [[1, 2, {"x": ["y"]}, 4, 5, 6, 7, 8]]
                    //       We are here....................^    or here.....^
                    this->errorBuf.append(']');
                    this->nestLevel--;
                    this->nestWantComma = (this->nestLevel > 0);

                    if (this->nestLevel == 0) {
                        // We're done with this Array. Warn and clear.
                        this->warnings.warnRowInvalid(this->row, this->errorBuf);
                        this->errorBuf.reset();
                    }
                } else {
                    this->state = DONE;
                }
                break;

            case IN_RECORD:
                this->nestLevel--;

                if (this->column) {
                    this->valueBuf.append(']');
                    this->nestWantComma = this->nestLevel > 0;

                    if (this->nestLevel == 0) {
                        if (this->valueBuf.hasOverflow()) {
                            this->warnings.warnValueTruncated(this->row, this->column->name);
                        }
                        this->finishColumnWithStringValue();
                    }
                }
                break;
        }

        return true;
    }

private:
    void finishColumnWithStringValue()
    {
        std::string_view sv(this->valueBuf.toUtf8StringView());
        this->nBytesTotal += sv.size();
        if (this->nBytesTotal > FLAGS_max_bytes_total) {
            this->warnings.warnStoppedOutOfMemory();
            this->state = DONE;
        } else {
            this->column->writeString(this->row, sv);
            this->isRowPartiallyWritten = true;
        }
        this->column = nullptr;
        this->valueBuf.reset();
    }

    void finishColumnWithNumberValue()
    {
        std::string_view sv(this->valueBuf.toRawStringView()); // JSON guarantees it's ASCII
        this->nBytesTotal += sv.size();
        if (this->nBytesTotal > FLAGS_max_bytes_total) {
            this->warnings.warnStoppedOutOfMemory();
            this->state = DONE;
        } else {
            this->column->writeNumber(this->row, sv);
            this->isRowPartiallyWritten = true;
        }
        this->column = nullptr;
        this->valueBuf.reset();
    }

    void finishColumnWithNull()
    {
        // We don't need to write any data to the column. But
        // we extend its length, so that if we can warn about a
        // dup in a record like {"x": null, "x": null}.
        this->column->growToLength(this->row + 1);
        this->column = nullptr;
        this->isRowPartiallyWritten = true;
    }

    void appendCommaAndExpectFutureCommaIfWeAreSerializing(StringBuffer& buf)
    {
        if (this->nestLevel > 0) {
            if (this->nestWantComma) {
                buf.append(',');
            }
            // The caller is serializing a value; after it, we'll need a comma
            this->nestWantComma = true;
        }
    }
};

struct ReadJsonResult {
    Warnings warnings;
    std::shared_ptr<arrow::Table> table;
};

static ReadJsonResult readJson(const char* jsonFilename) {
    JsonHandler handler;

    FILE* fp;
    fp = fopen(jsonFilename, "r");
    if (fp == NULL) {
        perror("fopen");
        std::_Exit(1);
    }

    char readBuffer[READ_BUFFER_SIZE];
    rapidjson::FileReadStream stream(fp, readBuffer, sizeof(readBuffer));

    // We don't need to decode UTF-8 when reading -- we assume it's valid,
    // and the parser can operate one byte at a time. Buf we _do_ need to
    // _encode_ UTF-8 when reading: values like "\uD800" must be parsed.
    //
    // JSON's Unicode escapes are crazy: they're UCS-2 escape codes. Blame
    // Netscape Navigator.
    //
    // rapidjson's "UTF8" and default flags give exactly this behavior.
    rapidjson::GenericReader<rapidjson::UTF8<uint8_t>, rapidjson::UTF8<uint8_t>> reader;
    rapidjson::ParseResult ok = reader.Parse<rapidjson::kParseIterativeFlag | rapidjson::kParseNumbersAsStringsFlag>(stream, handler);

    if (!ok) {
        handler.warnings.warnJsonParseError(ok.Offset(), std::string(rapidjson::GetParseError_En(ok.Code())));
        // and continue, so we'll return whatever we've parsed so far
    }

    if (fclose(fp) != 0) {
        perror("fclose");
        std::_Exit(1);
    }

    size_t nRows = handler.row;
    if (nRows > FLAGS_max_rows) {
        handler.warnings.warnRowsSkipped(nRows - FLAGS_max_rows);
        nRows = FLAGS_max_rows;
    }

    std::shared_ptr<arrow::Table> table(handler.tableBuilder.finish(
          nRows + (handler.isRowPartiallyWritten ? 1 : 0),
          handler.warnings
    ));
    return ReadJsonResult { handler.warnings, table };
}

int main(int argc, char** argv) {
    std::string usage = std::string("Usage: ") + argv[0] + " <CSV_FILENAME> <ARROW_FILENAME>";
    gflags::SetUsageMessage(usage);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 3) {
        gflags::ShowUsageWithFlags(argv[0]);
        std::_Exit(1);
    }

    const char* jsonFilename(argv[1]);
    const std::string arrowFilename(argv[2]);

    ReadJsonResult result = readJson(jsonFilename);
    printWarnings(result.warnings);
    writeArrowTable(*result.table, arrowFilename);
    return 0;
}
