cmake_minimum_required(VERSION 3.16)

project(colun_storage_test DESCRIPTION "test columna storage like parquet and arrow" VERSION 1.0 LANGUAGES CXX)

include(GNUInstallDirs)

enable_language(CXX)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
else()
    message(FATAL_ERROR "invalid CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE}")
endif()

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")
set(CMAKE_INSTALL_MESSAGE LAZY)

set(CMAKE_CXX_FLAGS "-ftemplate-backtrace-limit=20 -Wall -Wextra -Werror -Wfloat-conversion -Wconversion -Wpedantic -Wstrict-aliasing")
include(CheckCXXCompilerFlag)

function(setup_arrow_exec target src_file)
    add_executable(${target} ${src_file})
    target_link_libraries(${target} PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
endfunction()

setup_arrow_exec(hello_world "hello_world.x.cpp")
setup_arrow_exec(compressed_table "compressed_table.x.cpp")
setup_arrow_exec(perf_bench_arr_creation "perf_bench_arr_creation.x.cpp")
setup_arrow_exec(perf_bench_table_creation "perf_bench_table_creation.x.cpp")
