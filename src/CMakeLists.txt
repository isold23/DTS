cmake_minimum_required(VERSION 3.10)
project(dts LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package (glog REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibXml2 REQUIRED)

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(PROTO_FILES protobuf/record.proto)

set(MYSQL_INCLUDE_DIR "/usr/include/mysql/")
set(MYSQL_LIBRARIES "/usr/lib64/mysql/libmysqlclient.so.21.2.39")

link_directories(/usr/local/lib64)

protobuf_generate_cpp(PROTO_SRC PROTO_HDR ${PROTO_FILES})

include_directories(${MYSQL_INCLUDE_DIR})

file(GLOB SOURCES "*.cpp")
add_executable(dts ${SOURCES} ${PROTO_SRC})

target_link_libraries(dts ${Protobuf_LIBRARIES})
target_link_libraries (dts glog::glog)

target_link_libraries(dts ${MYSQL_LIBRARIES})

target_link_libraries(dts OpenSSL::Crypto OpenSSL::SSL)
target_link_libraries(dts ${LIBXML2_LIBRARIES})
target_include_directories(dts PRIVATE ${LIBXML2_INCLUDE_DIRS})


