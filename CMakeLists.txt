# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

project(arrow-tools)

include(FindPkgConfig)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# We require AVX2 extensions
add_compile_options(-Wall -Werror)

# Look for installed packages the system
find_package(gflags REQUIRED)
find_package(Arrow REQUIRED)
# From https://github.com/apache/arrow/blob/afe5515754d4c4f67a0e5ee066513f8383f00d70/cpp/examples/minimal_build/CMakeLists.txt#L37
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR} ${GFLAGS_INCLUDE_DIRS})

add_library(common src/common.cc)
add_library(column-builder src/column-builder.cc)
target_link_libraries(column-builder arrow)
add_library(excel-table-builder src/excel-table-builder.cc)
target_link_libraries(excel-table-builder column-builder)
add_library(json-warnings src/json-warnings.cc)

set(COMMON_LIBS Threads::Threads arrow_static gflags_static)

add_executable(arrow-validate src/arrow-validate.cc)
target_link_libraries(arrow-validate PRIVATE -static common ${COMMON_LIBS})

add_executable(csv-to-arrow src/csv-to-arrow.cc)
target_link_libraries(csv-to-arrow PRIVATE -static column-builder common ${COMMON_LIBS})

add_executable(json-to-arrow src/json-to-arrow.cc src/json-table-builder.cc)
target_include_directories(json-to-arrow PRIVATE vendor/rapidjson-1.1.0)
target_link_libraries(json-to-arrow PRIVATE -static common json-warnings column-builder ${COMMON_LIBS})

add_executable(xls-to-arrow src/xls-to-arrow.cc)
# -lxlnt is for xlnt::calendar (trivial) and xlnt::number_format (complex)
target_link_libraries(xls-to-arrow PRIVATE -static common json-warnings column-builder excel-table-builder ${COMMON_LIBS} -lxlnt -lxlsreader)

add_executable(xlsx-to-arrow src/xlsx-to-arrow.cc)
target_link_libraries(xlsx-to-arrow PRIVATE -static common column-builder json-warnings excel-table-builder ${COMMON_LIBS} -lxlnt)

install(TARGETS arrow-validate csv-to-arrow json-to-arrow xls-to-arrow xlsx-to-arrow DESTINATION /usr/bin)
