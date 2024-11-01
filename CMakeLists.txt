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

add_executable(hello_world "hello_world.x.cpp")
target_link_libraries(hello_world PRIVATE Arrow::arrow_shared Parquet::parquet_shared)
