#ifndef FLYFISH_MMAP_H
#define FLYFISH_MMAP_H

#include "util/dbc.hpp"

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>

namespace bio = boost::iostreams;
namespace bf =  boost::filesystem;

namespace henhouse 
{
    namespace util 
    {
        using mapped_file_ptr = std::shared_ptr<bio::mapped_file>;

        const std::size_t PAGE_SIZE = bio::mapped_file::alignment();
        const float GROW_FACTOR = 1.5;

        void open(bio::mapped_file& file, bf::path path, std::size_t new_size);

        template<class T>
            T* open_as(bio::mapped_file& file, bf::path path, std::size_t new_size = sizeof(T))
            {
                REQUIRE_GREATER_EQUAL(new_size, sizeof(T));

                const auto size = std::max(PAGE_SIZE, std::max(new_size, sizeof(T)));

                open(file, path, size);

                ENSURE_GREATER_EQUAL(file.size(), sizeof(T));
                return reinterpret_cast<T*>(file.data());
            }
    }
}
#endif
