add_definitions(-std=c++17)

include_directories(.)
include_directories(..)

file(GLOB src *.cpp)

add_library(henhouse_service STATIC ${src})
