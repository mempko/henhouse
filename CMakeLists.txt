cmake_minimum_required(VERSION 2.8.11)
project(henhouse)

add_definitions(-std=c++17)
add_definitions(-fPIC)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/)

include_directories(.)
include_directories(..)

if(NOT DEFINED PROXYGEN_DIR) 
    message(FATAL_ERROR "PROXYGEN_DIR must point to the proxygen root directory")
endif()

#setup boost
set(Boost_USE_STATIC_LIBS on)
set(Boost_USE_MULTITHREADED ON)
unset(Boost_INCLUDE_DIR CACHE)
unset(Boost_LIBRARY_DIRS CACHE)

find_package(Boost COMPONENTS system program_options filesystem regex iostreams thread serialization context REQUIRED)
link_directories(
    ${PROXYGEN_DIR}/proxygen/_build/lib
    ${Boost_LIBRARY_DIRS})
include_directories(
    ${PROXYGEN_DIR}/proxygen/_build/include
    ${Boost_INCLUDE_DIRS})

set(MISC_LIBRARIES
    librt.so
    proxygenhttpserver
    proxygen
    libz.a
    wangle
    fizz
    folly
    fmt
    snappy
    sodium
    iberty
    lz4
    libevent.a
    libpthread.so
    libboost_context.a
    dl
    ssl
    crypto
    glog
    gflags
    double-conversion)

add_subdirectory(src)
