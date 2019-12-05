#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/visitor_inline.h>
#include <gflags/gflags.h>
#include <simdutf8check.h>

#include "common.h"

DEFINE_bool(check_utf8, true, "Ensure all utf8() and dictionary(..., utf8()) columns are well-formed");
DEFINE_bool(check_offsets_dont_overflow, true, "Ensure all utf8() and dictionary(..., utf8()) offsets don't overflow data buffers");
DEFINE_bool(check_floats_all_finite, false, "Ensure all float16, float32 and float64 values are finite (not NaN or Infinity)");
DEFINE_bool(check_dictionary_values_all_used, false, "Ensure there are no spurious dictionary values");
DEFINE_bool(check_dictionary_values_unique, false, "Ensure there are no duplicate dictionary values");
DEFINE_bool(check_column_name_control_characters, false, "Ensure no column name includes ASCII control characters");
DEFINE_uint32(check_column_name_max_bytes, 0, "Enforce a maximum column-name length");


template<typename ArrayType>
bool
checkOffsetsDontOverflow(const ArrayType& array)
{
  // arrow's ValidateOffsets() already ensures offset 0 is 0. It also ensures
  // offsets increase monotonically, and that null-valued offsets copy the
  // previous offset, and that N values lead to N+1 offsets.

  using offset_type = typename ArrayType::offset_type;
  auto offsets = array.value_offsets();
  if (!offsets) return true; // no offsets means no overflow offsets
  offset_type lastOffset = array.value_offset(array.length());
  offset_type maxValidOffset = static_cast<offset_type>(array.value_data()->size());
  return lastOffset <= maxValidOffset;
}


int
checkUtf8(const arrow::StringArray& array)
{
  // assume checkOffsetsDontOverflow() was already called
  const int32_t* offsets = array.raw_value_offsets();
  if (!offsets) return 0; // all-null array
  const int32_t* offsetsEnd = &offsets[array.length()];
  const char* data = reinterpret_cast<const char*>(array.value_data()->data());

  uint32_t prevOffset = *offsets; // 0
  ++offsets; // advance to next offset (for N values, Arrow stores N+1 offsets)
  while (offsets <= offsetsEnd) {
    const uint32_t offset = *offsets;
    ++offsets;
    if (offset == prevOffset) continue; // value is null or empty string
    if (!validate_utf8_fast_avx_asciipath(&data[prevOffset], offset - prevOffset)) {
      return false;
    }
    prevOffset = offset;
  }
  return true;
}


static arrow::Status validateArray(const arrow::Array& array);


struct ValidateVisitor {
  arrow::Status Visit(const arrow::PrimitiveArray& array)
  {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray& array)
  {
    if (FLAGS_check_offsets_dont_overflow) {
      if (!checkOffsetsDontOverflow(array)) {
        return arrow::Status::Invalid("--check-offsets-dont-overflow");
      }
    }
    if (FLAGS_check_utf8) {
      if (!checkUtf8(array)) {
        return arrow::Status::Invalid("--check-utf8");
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DictionaryArray& array)
  {
    arrow::Status status = validateArray(*array.indices());
    if (!status.ok()) return status;
    return validateArray(*array.dictionary());
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
  if (FLAGS_check_utf8) {
    if (!validate_utf8_fast_avx_asciipath(name.c_str(), name.size())) {
      std::cout << "--check-utf8 failed on a column name" << std::endl;
      return false;
    }
  }

  if (FLAGS_check_column_name_control_characters) {
    for (const char c : name) {
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
  ASSERT_ARROW_OK(batch.Validate(), "validating batch for schema or length inconsistencies");

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
  std::shared_ptr<arrow::io::MemoryMappedFile> file;
  ASSERT_ARROW_OK(arrow::io::MemoryMappedFile::Open(filename, arrow::io::FileMode::READ, &file), "opening Arrow file");
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
  ASSERT_ARROW_OK(arrow::ipc::RecordBatchFileReader::Open(file, &reader), "reading Arrow file header");

  int nBatches = reader->num_record_batches();
  for (int i = 0; i < nBatches; i++) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_ARROW_OK(reader->ReadRecordBatch(i, &batch), "reading record batch");
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

  if (FLAGS_check_utf8 && !FLAGS_check_offsets_dont_overflow) {
    std::cerr << "--check-utf8 requires you also enable --check-offsets-dont-overflow" << std::endl;
    return 1;
  }

  const std::string arrowFilename(argv[1]);
  return validateArrowFile(arrowFilename) ? 0 : 1;
}
