cmake_minimum_required(VERSION 3.16)
project(Fulva)

set(CMAKE_CXX_FLAGS "-O2")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include(../cmake/common.cmake)

# Proto file
get_filename_component(kvs_proto "protos/keyvaluestore.proto" ABSOLUTE)
get_filename_component(kvs_proto_path "${kvs_proto}" PATH)

# Generated sources
set(kvs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.pb.cc")
set(kvs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.pb.h")
set(kvs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.grpc.pb.cc")
set(kvs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/keyvaluestore.grpc.pb.h")
add_custom_command(
      OUTPUT "${kvs_proto_srcs}" "${kvs_proto_hdrs}" "${kvs_grpc_srcs}" "${kvs_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${kvs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${kvs_proto}"
      DEPENDS "${kvs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# kvs_grpc_proto
add_library(kvs_grpc_proto
  ${kvs_grpc_srcs}
  ${kvs_grpc_hdrs}
  ${kvs_proto_srcs}
  ${kvs_proto_hdrs})
target_link_libraries(kvs_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})


include_directories(./src/include/)
include_directories(/usr/local/include/)
include_directories(../../utils/)

file(GLOB_RECURSE ALL_OBJECT_FILES ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB_RECURSE ALL_OBJECT_FILES ${PROJECT_SOURCE_DIR}/../../utils/*.cpp)
add_library(src_files STATIC ${ALL_OBJECT_FILES})

add_executable(Fulva ./src/main.cpp ./src/SourceMigrationManager.cpp ./src/DestinationMigrationManager.cpp)

find_path(HIREDIS_HEADER hiredis)
target_include_directories(Fulva PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_LIB hiredis)
target_link_libraries(Fulva ${HIREDIS_LIB})

find_path(REDIS_PLUS_PLUS_HEADER sw)
target_include_directories(Fulva PUBLIC ${REDIS_PLUS_PLUS_HEADER})

find_library(REDIS_PLUS_PLUS_LIB redis++)

target_link_libraries(Fulva kvs_grpc_proto ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})
target_link_libraries(Fulva src_files Threads::Threads)
target_link_libraries(Fulva ${REDIS_PLUS_PLUS_LIB} hiredis)
