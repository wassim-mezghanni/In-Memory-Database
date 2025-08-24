cmake_minimum_required(VERSION 3.15)
project(inmemdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(inmemdb
    src/lexer.cpp
    src/parser.cpp
    src/storage.cpp
    src/executor.cpp
)

target_include_directories(inmemdb PUBLIC include)

add_executable(inmemdb_cli src/main.cpp)

target_link_libraries(inmemdb_cli PRIVATE inmemdb)

enable_testing()
add_executable(inmemdb_tests tests/test_inmemdb.cpp)
target_link_libraries(inmemdb_tests PRIVATE inmemdb)
add_test(NAME inmemdb_tests COMMAND inmemdb_tests)
