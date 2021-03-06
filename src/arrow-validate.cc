#include <cmath>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/utf8.h>
#include <arrow/visitor_inline.h>
#include <gflags/gflags.h>

#include "common.h"

DEFINE_bool(check_safe, true, "Ensure all utf8() and dictionary(..., utf8()) offsets don't overflow data buffers, utf8 is all valid, plus other built-in Arrow tests");
DEFINE_bool(check_floats_all_finite, false, "Ensure all float16, float32 and float64 values are finite (not NaN or Infinity)");
DEFINE_bool(check_dictionary_values_all_used, false, "Ensure there are no spurious dictionary values");
DEFINE_bool(check_dictionary_values_not_null, false, "Ensure there are no null dictionary values");
DEFINE_bool(check_dictionary_values_unique, false, "Ensure there are no duplicate dictionary values");
DEFINE_bool(check_column_name_control_characters, false, "Ensure no column name includes ASCII control characters");
DEFINE_uint32(check_column_name_max_bytes, 0, "Enforce a maximum column-name length");


static arrow::Status validateArray(const arrow::Array& array);


struct CheckDictionaryValuesAllUsedVisitor {
  const arrow::Array* dictionary;
  bool valid;

  CheckDictionaryValuesAllUsedVisitor(const arrow::Array* dictionary) : dictionary(dictionary), valid(false) {}

  template<typename ArrayType>
  bool check(const ArrayType& indices) {
    if (indices.null_count() == indices.length()) {
      // all-null indices? Then length of non-null data must be 0
      return this->dictionary->length() == 0;
    }

    using value_type = typename ArrayType::value_type;

    // 1. Find max
    value_type max = 0;
    int64_t length = indices.length();
    for (int64_t i = 0; i < length; i++) {
      if (indices.IsValid(i)) {
        value_type value = indices.Value(i);
        if (value > max) {
          max = value;
        }
      }
    }

    // 2. Store all seen values in a set. (The set is of size `max`.)
    // 0 means "not seen", 1 means "seen"
    std::unique_ptr<uint8_t[]> seen = std::make_unique<uint8_t[]>(max + 1);
    for (int64_t i = 0; i < length; i++) {
      if (indices.IsValid(i)) {
        seen[indices.Value(i)] = 1;
      }
    }

    // 3. Check if there's an unseen value.
    for (int64_t i = 0; i < max; i++) {
      if (seen[i] == 0) {
        return false;
      }
    }

    // We've seen all value
    return true;
  }

  arrow::Status Visit(const arrow::Int8Array& array) {
    this->valid = this->check(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Array& array) {
    this->valid = this->check(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Array& array) {
    this->valid = this->check(array);
    return arrow::Status::OK();
  }

  // fallback
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented("Dictionary indices must be uint8/uint16/uint32");
  }
};


static bool
checkDictionaryValuesNotNull(const arrow::Array& array) {
  return array.null_count() == 0;
}


static bool
checkDictionaryValuesAllUsed(const arrow::Array& indices, const arrow::Array& dictionary) {
  CheckDictionaryValuesAllUsedVisitor visitor(&dictionary);
  ASSERT_ARROW_OK(arrow::VisitArrayInline(indices, &visitor), "checking all dictionary values are used");
  return visitor.valid;
}


static bool
checkDictionaryValuesUnique(const std::shared_ptr<arrow::Array> dictionary)
{
  arrow::compute::ExecContext ctx;
  ctx.set_use_threads(false);
  std::shared_ptr<arrow::Array> uniques = ASSERT_ARROW_OK(
    arrow::compute::Unique(dictionary, &ctx),
    "checking dictionary is unique"
  );
  return uniques->length() == dictionary->length();
}


struct ValidateVisitor {
  template<typename ArrayType>
  arrow::Status visitFloatArray(const ArrayType& array)
  {
    if (FLAGS_check_floats_all_finite) {
      int64_t length = array.length();
      for (int64_t i = 0; i < length; i++) {
        if (array.IsValid(i)) {
          if (!std::isfinite(array.Value(i))) {
            return arrow::Status::Invalid("--check-floats-all-finite");
          }
        }
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleArray& array)
  {
    return this->visitFloatArray(array);
  }

  arrow::Status Visit(const arrow::FloatArray& array)
  {
    return this->visitFloatArray(array);
  }

  arrow::Status Visit(const arrow::HalfFloatArray& array)
  {
    return this->visitFloatArray(array);
  }

  arrow::Status Visit(const arrow::PrimitiveArray& array)
  {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array)
  {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DictionaryArray& array)
  {
    arrow::Status status = validateArray(*array.indices());
    if (!status.ok()) return status;

    status = validateArray(*array.dictionary());
    if (!status.ok()) return status;

    if (FLAGS_check_dictionary_values_not_null) {
      if (!checkDictionaryValuesNotNull(*array.dictionary())) {
        return arrow::Status::Invalid("--check-dictionary-values-not-null");
      }
    }

    if (FLAGS_check_dictionary_values_all_used) {
      if (!checkDictionaryValuesAllUsed(*array.indices(), *array.dictionary())) {
        return arrow::Status::Invalid("--check-dictionary-values-all-used");
      }
    }

    if (FLAGS_check_dictionary_values_unique) {
      if (!checkDictionaryValuesUnique(array.dictionary())) {
        return arrow::Status::Invalid("--check-dictionary-values-unique");
      }
    }

    return status;
  }

  // fallback
  arrow::Status Visit(const arrow::Array& array)
  {
    return arrow::Status::NotImplemented(array.type()->name() + " support not yet implemented");
  }
};


static arrow::Status
validateArray(const arrow::Array& array)
{
  ValidateVisitor visitor;
  return arrow::VisitArrayInline(array, &visitor);
}


static bool
validateColumnName(const std::string& name)
{
  if (FLAGS_check_safe) {
    if (!arrow::util::ValidateUTF8(reinterpret_cast<const uint8_t*>(name.c_str()), name.size())) {
      std::cout << "--check-safe failed on a column name with invalid UTF-8" << std::endl;
      return false;
    }
  }

  if (FLAGS_check_column_name_control_characters) {
    for (const uint8_t c : name) {
      if (c < 0x20) {
        std::cout << "--check-column-name-control-characters failed on a column name" << std::endl;
        return false;
      }
    }
  }

  if (FLAGS_check_column_name_max_bytes > 0) {
    if (name.size() > FLAGS_check_column_name_max_bytes) {
      std::cout << "--check-column-name-max-bytes=" << FLAGS_check_column_name_max_bytes << " failed on column " << name << std::endl;
      return false;
    }
  }

  return true;
}


static bool
validateColumn(const std::string& name, const arrow::Array& array)
{
  if (!validateColumnName(name)) return false;

  arrow::Status status = validateArray(array);
  if (status.IsInvalid()) {
    std::cout << status.message() << " failed on column " << name << std::endl;
    return false;
  }
  ASSERT_ARROW_OK(status, "checking for unexpected status");
  return true;
}


static bool
validateRecordBatch(arrow::RecordBatch& batch)
{
  if (FLAGS_check_safe) {
    arrow::Status status = batch.ValidateFull();
    if (!status.ok()) {
      std::cout << "--check-safe failed: " << status.ToString() << std::endl;
      return false;
    }
  }

  int nColumns = batch.num_columns();
  for (int i = 0; i < nColumns; i++) {
    if (!validateColumn(batch.column_name(i), *batch.column(i))) {
      return false;
    }
  }

  return true;
}


static bool
validateArrowFile(const std::string& filename)
{
  std::shared_ptr<arrow::io::MemoryMappedFile> file(ASSERT_ARROW_OK(
    arrow::io::MemoryMappedFile::Open(filename, arrow::io::FileMode::READ),
    "opening Arrow file"
  ));
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader = ASSERT_ARROW_OK(
    arrow::ipc::RecordBatchFileReader::Open(file.get(), arrow::ipc::IpcReadOptions { .use_threads = false }),
    "reading Arrow file header"
  );

  int nBatches = reader->num_record_batches();
  for (int i = 0; i < nBatches; i++) {
    std::shared_ptr<arrow::RecordBatch> batch = ASSERT_ARROW_OK(
      reader->ReadRecordBatch(i),
      "reading record batch"
    );
    if (!validateRecordBatch(*batch)) {
      return false;
    }
  }

  return true;
}


int main(int argc, char** argv)
{
  std::string usage = std::string("Usage: ") + argv[0] + " <ARROW_FILENAME>";
  gflags::SetUsageMessage(usage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 2) {
    gflags::ShowUsageWithFlags(argv[0]);
    std::_Exit(1);
  }

  if (FLAGS_check_safe) {
    arrow::util::InitializeUTF8();
  }

  const std::string arrowFilename(argv[1]);
  return validateArrowFile(arrowFilename) ? 0 : 1;
}
