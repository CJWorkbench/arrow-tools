# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

project(arrow-tools)

include(FindPkgConfig)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Look for installed packages the system
pkg_search_module(ARROW REQUIRED arrow)
find_package(glog REQUIRED)
find_package(double-conversion REQUIRED)
pkg_search_module(JEMALLOC REQUIRED jemalloc)
pkg_search_module(GFLAGS REQUIRED gflags)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR} ${GFLAGS_INCLUDE_DIRS})

add_executable(csv-to-arrow src/csv-to-arrow.cc src/common.cc)
target_link_libraries(csv-to-arrow ${ARROW_STATIC_LDFLAGS} ${JEMALLOC_STATIC_LDFLAGS} glog::glog double-conversion::double-conversion ${GFLAGS_STATIC_LDFLAGS} -static -lpthread -lboost_filesystem -lboost_system)