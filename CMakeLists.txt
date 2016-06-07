cmake_minimum_required(VERSION 2.8.11)
project(flyfish)

include_directories(.)
include_directories(..)

#setup boost
set(Boost_USE_STATIC_LIBS on)
find_package(Boost COMPONENTS system program_options filesystem regex iostreams thread mpi serialization REQUIRED)
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})

set(MISC_LIBRARIES libpthread.so librt.so libmpi.so libmpi++.so)

add_subdirectory(src)
