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

template <typename T>
static inline T ASSERT_ARROW_OK(arrow::Result<T> result, const char* message)
{
  ASSERT_ARROW_OK(result.status(), message);
  return result.ValueOrDie(); // TODO next version of Arrow has ValueUnsafe()
}

void writeArrowTable(const arrow::Table& arrowTable, const std::string& path);
