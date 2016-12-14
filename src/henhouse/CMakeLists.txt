ADD_DEFINITIONS(-std=c++14)

include_directories(.)
include_directories(..)

file(GLOB src *.cpp)
file(GLOB headers *.hpp)

add_executable(
    henhouse
    ${src})

target_link_libraries(
    henhouse
    henhouse_service
    henhouse_db
    henhouse_util
    ${Boost_LIBRARIES}
    ${MISC_LIBRARIES})

add_dependencies(
    henhouse 
    henhouse_service
    henhouse_db
    henhouse_util)

install(TARGETS henhouse DESTINATION bin)
