ADD_DEFINITIONS(-std=c++14)

include_directories(.)
include_directories(..)

file(GLOB src *.cpp)
file(GLOB headers *.hpp)

add_executable(
    flyfish
    ${src}
    ${moc_resources})

target_link_libraries(
    flyfish
    flyfish_util
    ${Boost_LIBRARIES}
    ${MISC_LIBRARIES})

add_dependencies(
    flyfish 
    flyfish_util)

install(TARGETS flyfish DESTINATION bin)
