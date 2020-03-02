# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

project(arrow-tools)

include(FindPkgConfig)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# We require AVX2 extensions
add_compile_options(-march=haswell -Wall -Werror)

# Look for installed packages the system
pkg_search_module(ARROW REQUIRED arrow)
pkg_search_module(JEMALLOC REQUIRED jemalloc)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR})

add_library(common src/common.cc)
add_library(column-builder src/column-builder.cc)
target_link_libraries(column-builder ${ARROW_STATIC_LDFLAGS})
add_library(excel-table-builder src/excel-table-builder.cc)
target_link_libraries(excel-table-builder column-builder)
add_library(json-warnings src/json-warnings.cc)

add_executable(arrow-validate src/arrow-validate.cc)
target_include_directories(arrow-validate PRIVATE vendor/fastvalidate-utf-8/include)
target_link_libraries(arrow-validate common ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} -static -ldouble-conversion -lgflags -lpthread -lboost_filesystem -lboost_system)

add_executable(csv-to-arrow src/csv-to-arrow.cc)
target_link_libraries(csv-to-arrow common ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} -static -ldouble-conversion -lgflags -lpthread -lboost_filesystem -lboost_system)

add_executable(json-to-arrow src/json-to-arrow.cc src/json-table-builder.cc)
target_include_directories(json-to-arrow PRIVATE vendor/rapidjson-1.1.0)
target_link_libraries(json-to-arrow common json-warnings column-builder ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} -static -ldouble-conversion -lgflags -lpthread -lboost_filesystem -lboost_system)

add_executable(xls-to-arrow src/xls-to-arrow.cc)
# -lxlnt is for xlnt::calendar (trivial) and xlnt::number_format (complex)
target_link_libraries(xls-to-arrow common column-builder json-warnings excel-table-builder ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} -static -ldouble-conversion -lgflags -lxlnt -lxlsreader -lpthread -lboost_filesystem -lboost_system)

add_executable(xlsx-to-arrow src/xlsx-to-arrow.cc)
target_link_libraries(xlsx-to-arrow common column-builder json-warnings excel-table-builder ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} -static -ldouble-conversion -lgflags -lxlnt -lpthread -lboost_filesystem -lboost_system)
