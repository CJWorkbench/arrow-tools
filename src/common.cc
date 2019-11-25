#include <memory>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "common.h"

void writeArrowTable(const arrow::Table& arrowTable, const std::string& path)
{
  std::shared_ptr<arrow::io::FileOutputStream> outputStream;
  ASSERT_ARROW_OK(arrow::io::FileOutputStream::Open(path, &outputStream), "opening output stream");
  std::shared_ptr<arrow::ipc::RecordBatchWriter> fileWriter;
  ASSERT_ARROW_OK(arrow::ipc::RecordBatchFileWriter::Open(outputStream.get(), arrowTable.schema(), &fileWriter), "creating file writer");
  ASSERT_ARROW_OK(fileWriter->WriteTable(arrowTable), "writing Arrow table");
  ASSERT_ARROW_OK(fileWriter->Close(), "closing Arrow file");
}
