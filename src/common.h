#include <cstdlib>
#include <iostream>
#include <arrow/api.h>


static inline void ASSERT_ARROW_OK(arrow::Status status, const char* message)
{
  if (!status.ok()) {
    std::cerr << "Failure " << message << ": " << status.ToString() << std::endl;
    std::_Exit(1);
  }
}


void writeArrowTable(const arrow::Table& arrowTable, const std::string& path);
