#include <iostream>

#include "util/dbc.hpp"

#include <deque>
#include <string>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>
#include <exception>
#include <algorithm>

namespace bio = boost::iostreams;
namespace bf =  boost::filesystem;

namespace flyfish
{
    using time_type = std::uint64_t;
    using count_type = std::uint64_t;
    using offset_type = std::uint64_t;

    void open(bio::mapped_file& file, bf::path path, std::size_t new_size)
    {
        REQUIRE_GREATER(new_size, 0);

        if(bf::exists(path)) 
        {
            file.open(path);

            std::cerr << "opened: " << path << std::endl;
            std::cerr << "size: " << file.size() << std::endl;
        }
        else 
        {
            bio::mapped_file_params p;
            p.path = path.string();
            p.new_file_size = new_size;
            p.flags = bio::mapped_file::readwrite;

            std::cerr << "creating: " << path << std::endl;
            std::cerr << "size: " << p.new_file_size << std::endl;

            file.open(p);
        }
    }

    template<class T>
        T* open_as(bio::mapped_file& file, bf::path path, std::size_t new_size = sizeof(T))
        {
            REQUIRE_GREATER_EQUAL(new_size, sizeof(T));

            const auto size = std::max(new_size, sizeof(T));

            open(file, path, size);

            ENSURE_GREATER_EQUAL(file.size(), sizeof(T));
            return reinterpret_cast<T*>(file.data());
        }


    struct index_item
    {
        time_type time = 0;
        offset_type pos = 0;
    };

    struct index_metadata
    {
        std::uint64_t size = 0 ;
        std::uint64_t resolution = 0;
    };

    const std::size_t FRAME_SIZE = 4096;
    const std::uint64_t DEFAULT_RESOLUTION = 100;
    const std::size_t INDEX_SIZE = 4096;

    template<class meta_t, class data_t>
    class mapped_vector
    {
        public:
            mapped_vector(){};
            mapped_vector(const bf::path& meta_file, const bf::path& data_file) 
            {
                //open index metadata
                _metadata = open_as<meta_t>(_meta_file, meta_file);

                //open index data. New file size is INDEX_SIZE
                open(_data_file, data_file, INDEX_SIZE);
                _items = reinterpret_cast<data_t*>(_data_file.data());

                //compute max elements
                _max_items = _data_file.size() / sizeof(data_t);
                CHECK_LESS_EQUAL(_metadata->size, _max_items);

                ENSURE(_metadata != nullptr);
                ENSURE(_items != nullptr);
            }

            std::uint64_t size() const 
            {
                REQUIRE(_metadata)
                ENSURE_LESS_EQUAL(_metadata->size, _max_items);
                return _metadata->size;
            }

            data_t get(size_t pos) const 
            {
                REQUIRE(_metadata); 
                REQUIRE(_items); 
                REQUIRE_LESS(pos, _metadata->size);

                return _items[pos];
            }

        protected:
            meta_t* _metadata = nullptr;
            data_t* _items = nullptr;
            std::size_t _max_items = 0;
            bio::mapped_file _meta_file;
            bio::mapped_file _data_file;
    };

    class index : public mapped_vector<index_metadata, index_item>
    {
        public:
            index(){};
            index(const bf::path& meta_file, const bf::path& data_file) :
                mapped_vector{meta_file, data_file}
            {
                REQUIRE(_metadata);

                if(_metadata->resolution == 0) _metadata->resolution = DEFAULT_RESOLUTION;
                _range_size = FRAME_SIZE * _metadata->resolution;

                ENSURE(_metadata != nullptr);
                ENSURE(_items != nullptr);
            }

            index_item find_range(time_type t) const 
            {
                REQUIRE(_metadata);
                REQUIRE(_items);

                const auto end = _items + _metadata->size;
                auto r = std::upper_bound(_items, end, t, 
                        [](time_type l, const auto& r) { return l < r.time;});

                return r != _items ? *(r - 1) : *_items;
            }
            
            bool index_offset(time_type t, offset_type o) 
            {
                REQUIRE(_metadata);
                REQUIRE(_items);

                auto next_pos = 0;
                if(_metadata->size > 0) 
                {
                    const auto pos = _metadata->size - 1;
                    const auto latest_time = _items[pos].time;

                    //check if we need new range, otherwise break if(t < latest_time) return false; if((t - latest_time) < _range_size) return false;

                    //determine next range
                    next_pos = pos + 1;
                    //TODO: RESIZE index file if we reach end
                    CHECK_LESS(next_pos, _max_items);
                } 

                index_item i = {t, o};
                _items[next_pos] = i;
                _metadata->size++;

                return true;
            }

        private:
            std::size_t _range_size = 0;
    };

    using key_t = std::string;

    struct data_metadata
    {
        std::uint64_t size = 0;
    };

    struct data_item
    {
        count_type value = 0;
    };

    const std::size_t DATA_SIZE = 4096;

    //TODO: pull out new 'mapped_vector' which stores vectored data
    //Should support resizing and remapping
    class data : public mapped_vector<data_metadata, data_item>
    {
        public:
            data(){}
            data(const bf::path& meta_file, const bf::path& data_file) :
                mapped_vector{meta_file, data_file}
            {
                ENSURE(_metadata != nullptr);
                ENSURE(_items != nullptr);
            }

        private:
    };

    struct time_db
    {
        key_t key;
        index idx;

        data count;
        data total;
        data integral;
    };


    time_db from_directory(const std::string& path) 
    {
        if(!bf::is_directory(path))
           throw std::runtime_error("path " + path + " is not a directory"); 
    
        bf::path root = path;


        time_db db;
        db.key = root.filename().string();

        bf::path imeta = root / "imeta";
        bf::path idata = root / "idata";
        db.idx = index(imeta, idata);

        
        return db;
    }
}

int main(int argc, char** argv)
try
{
    auto db = flyfish::from_directory("./tmp");
    db.idx.index_offset(10, 10);
    db.idx.index_offset(1000000, 20);
    db.idx.index_offset(20000000, 30);
    db.idx.index_offset(40000000, 50);
    auto p = db.idx.find_range(10000000);
    std::cout << "found: " << p.pos << std::endl;

    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "Error! " << e.what() << std::endl;
    return 1;
}
