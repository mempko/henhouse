#include "util/mmap.hpp" 

namespace henhouse 
{
    namespace util 
    {
        void open(bio::mapped_file& file, bf::path path, std::size_t new_size)
        {
            REQUIRE_GREATER(new_size, 0);

            if(bf::exists(path)) 
            {
                file.open(path);
            }
            else 
            {
                bio::mapped_file_params p;
                p.path = path.string();
                p.new_file_size = new_size;
                p.flags = bio::mapped_file::readwrite;

                file.open(p);
            }

            if(!file.is_open())
                throw std::runtime_error{"unable to mmap " + path.string()};
        }
    }
}
