cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_SCAN_FOR_MODULES ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

project(
  scaler
  LANGUAGES C CXX
  VERSION 0.1
)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
  message(STATUS "${PROJECT_NAME} Release build")
else()
  set(CMAKE_BUILD_TYPE "Debug")
  message(STATUS "${PROJECT_NAME} Debug build")
endif()

add_subdirectory(scaler)
