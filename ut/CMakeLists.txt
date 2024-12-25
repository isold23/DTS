cmake_minimum_required(VERSION 3.14)
project(dts_ut)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  dts_ut
  ut.cpp
)
target_link_libraries(
  dts_ut
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(dts_ut)