cmake_minimum_required(VERSION 3.16)
project(Source_Migration)

set(CMAKE_CXX_FLAGS "-O2")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include_directories(./src/include/)
include_directories(/usr/local/include/)
include_directories(../../../utils/)

file(GLOB_RECURSE ALL_OBJECT_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE ALL_OBJECT_FILES ${PROJECT_SOURCE_DIR}/../../../utils/*.cpp)
add_library(src_files STATIC ${ALL_OBJECT_FILES})

add_executable(Source_Migration ./src/main.cpp ./src/SourceMigrationManager.cpp ./src/DestinationMigrationManager.cpp ./src/helper_func.cpp ./src/UDP_file_transfer.cpp ./src/test.cpp ./src/crc64.cpp ./src/crcspeed.cpp)

find_path(HIREDIS_HEADER hiredis)
target_include_directories(Source_Migration PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_LIB hiredis)
target_link_libraries(Source_Migration ${HIREDIS_LIB})

find_path(REDIS_PLUS_PLUS_HEADER sw)
target_include_directories(Source_Migration PUBLIC ${REDIS_PLUS_PLUS_HEADER})

find_library(REDIS_PLUS_PLUS_LIB redis++)

target_link_libraries(Source_Migration src_files Threads::Threads)
target_link_libraries(Source_Migration ${REDIS_PLUS_PLUS_LIB} hiredis)