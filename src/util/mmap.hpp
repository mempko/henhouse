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

        bool open(bio::mapped_file& file, bf::path path, std::size_t new_size);
    }
}
#endif
