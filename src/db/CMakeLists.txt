ADD_DEFINITIONS(-std=c++14)

include_directories(.)
include_directories(..)

file(GLOB src *.cpp)

add_library(flyfish_db STATIC ${src})
