#include <iostream>

#include "util/dbc.hpp"

#include <deque>
#include <string>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>
#include <exception>
#include <algorithm>
#include <fstream>

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

    template<class T>
        T* open_as(bio::mapped_file& file, bf::path path, std::size_t new_size = sizeof(T))
        {
            REQUIRE_GREATER_EQUAL(new_size, sizeof(T));

            const auto size = std::max(new_size, sizeof(T));

            open(file, path, size);

            ENSURE_GREATER_EQUAL(file.size(), sizeof(T));
            return reinterpret_cast<T*>(file.data());
        }

    const std::size_t DEFAULT_NEW_SIZE = 4096;

    template<class meta_t, class data_type>
    class mapped_vector
    {
        public:
            mapped_vector(){};
            mapped_vector(
                    const bf::path& meta_file, 
                    const bf::path& data_file, 
                    const int new_size = DEFAULT_NEW_SIZE) 
            {
                _data_file_path = data_file;
                _new_size = new_size;

                //open index metadata
                _metadata = open_as<meta_t>(_meta_file, meta_file);

                //open index data. New file size is new_size
                open(_data_file, data_file, new_size);
                _items = reinterpret_cast<data_type*>(_data_file.data());

                //compute max elements
                _max_items = _data_file.size() / sizeof(data_type);
                CHECK_LESS_EQUAL(_metadata->size, _max_items);

                ENSURE(_metadata != nullptr);
                ENSURE(_items != nullptr);
            }

            meta_t& meta() 
            {
                REQUIRE(_metadata);
                return *_metadata;
            }

            const meta_t& meta() const
            {
                REQUIRE(_metadata);
                return *_metadata;
            }

            std::uint64_t size() const 
            {
                REQUIRE(_metadata)
                ENSURE_LESS_EQUAL(_metadata->size, _max_items);
                return _metadata->size;
            }

            const data_type& operator[](size_t pos) const 
            {
                REQUIRE(_metadata); 
                REQUIRE(_items); 
                REQUIRE_LESS(pos, _metadata->size);

                return _items[pos];
            }

            data_type& operator[](size_t pos)
            {
                REQUIRE(_metadata); 
                REQUIRE(_items); 
                REQUIRE_LESS(pos, _metadata->size);

                return _items[pos];
            }

            void push_back(const data_type& v) 
            {
                REQUIRE(_metadata);
                const auto next_pos = _metadata->size;

                if(next_pos >= _max_items)
                    resize(_data_file.size() + _new_size);

                _items[next_pos] = v;
                _metadata->size++;
            }

            data_type* begin() 
            { 
                REQUIRE(_items);
                return _items;
            }

            data_type* end() 
            { 
                REQUIRE(_items);
                REQUIRE(_metadata);
                return _items + size();
            }

            data_type* begin() const
            { 
                REQUIRE(_items);
                return _items;
            }

            data_type* end() const
            { 
                REQUIRE(_items);
                REQUIRE(_metadata);
                return _items + size();
            }

            const data_type* cbegin() { return begin(); }
            const data_type* cend() { return end(); }
            const data_type* cbegin() const { return begin(); }
            const data_type* cend() const { return end(); }

            data_type& front()
            {
                REQUIRE(_items);
                return *_items;
            }

            const data_type& front() const
            {
                REQUIRE(_items);
                return *_items;
            }

            data_type& back()
            {
                REQUIRE(_items);
                REQUIRE(_metadata);
                REQUIRE_GREATER(_metadata->size, 0);
                return *(_items + (_metadata->size - 1));
            }

            const data_type& back() const
            {
                REQUIRE(_items);
                REQUIRE(_metadata);
                REQUIRE_GREATER(_metadata->size, 0);
                return *(_items + (_metadata->size - 1));
            }

        private:

            void resize(size_t new_size) 
            {
                _data_file.resize(new_size);
                _items = reinterpret_cast<data_type*>(_data_file.data());
                _max_items = _data_file.size() / sizeof(data_type);
            }


        protected:
            meta_t* _metadata = nullptr;
            data_type* _items = nullptr;
            std::size_t _max_items = 0;
            std::size_t _new_size = 0;
            bio::mapped_file _meta_file;
            bio::mapped_file _data_file;
            bf::path _data_file_path;
    };


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

    struct pos_result
    {
        index_item range;
        offset_type offset;
        offset_type pos;
    };

    class index_type : public mapped_vector<index_metadata, index_item>
    {
        public:
            index_type() : mapped_vector{} {};
            index_type(const bf::path& meta_file, const bf::path& data_file) :
                mapped_vector{meta_file, data_file, INDEX_SIZE}
            {
                REQUIRE(_metadata);

                if(_metadata->resolution == 0) _metadata->resolution = DEFAULT_RESOLUTION;
                _range_size = bucket_size() * _metadata->resolution;
            }

            std::size_t range_size() const { return _range_size;}
            std::size_t bucket_size() const { return FRAME_SIZE;}

            index_item find_range(time_type t) const 
            {
                REQUIRE(_metadata);
                REQUIRE(_items);

                auto r = std::upper_bound(cbegin(), cend(), t, 
                        [](time_type l, const auto& r) { return l < r.time;});

                return r != cbegin() ? *(r - 1) : front();
            }

            pos_result find_pos(time_type t, const index_item range) const
            {
                REQUIRE(_metadata);

                t = std::max(t, range.time);
                const auto offset_time = t - range.time;
                const auto offset = offset_time / _metadata->resolution;
                return pos_result{ range, offset, range.pos + offset};
            }

            pos_result find_pos(time_type t) const
            {
                if(size() == 0) return pos_result {index_item { t, 0}, 0, 0};
                const auto range = find_range(t);
                return find_pos(t, range);
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
        count_type total = 0;
        count_type integral = 0;
    };

    const std::size_t DATA_SIZE = 4096000;

    using data_type = mapped_vector<data_metadata, data_item>;

    void propogate(const data_item& prev, data_item& current)
    {
        current.total = prev.total + current.value;
        const auto change = current.value - prev.value;
        current.integral = prev.integral + change;
    }

    void update_current(const data_item& prev, data_item& current, count_type c)
    {
        current.value += c;
        propogate(prev, current);
    }

    data_item diff_buckets(const data_item& a, const data_item& b, std::size_t n)
    {
        const auto total = b.total - a.total;
        const auto avg = n > 0 ? total / n : a.value;
        return data_item { 
            avg, 
            b.total - a.total, 
            b.integral - a.integral};
    }

    struct timeline
    {
        key_t key;
        index_type index;
        data_type data;

        bool put(time_type t, count_type c)
        {
            const auto resolution = index.meta().resolution;

            //get last position only because we want to keep 
            //a specific performance profile. This is a deliberate limitation.
            auto r = index.size() > 0 ? 
                index.find_pos(t, index.back()) : 
                pos_result {index_item { t, 0}, 0, 0};

            //don't add if time is before last range
            if(t < r.range.time) return false;

            //if we move beyond end, append data 
            if(r.pos >= data.size())
            {
                r.pos = data.size();
                const auto prev = data.size() > 0 ? data[r.pos - 1] : data_item {0, 0, 0};
                data_item current = { c, 0, 0 };
                propogate(prev, current);
                data.push_back(current);
                
                //index only if the offset is bigger than the frame size.
                if(index.size() == 0 || r.offset >= FRAME_SIZE) 
                {
                    const auto aliased_time = r.range.time + (r.offset * resolution);
                    index_item index_entry = {aliased_time, r.pos};
                    index.push_back(index_entry);
                }
            }
            //otherwise bucket is current or in the past, no need to index.
            else
            {
                CHECK_GREATER(data.size(), 0);

                const auto prev = r.pos > 0 ? data[r.pos - 1] : data_item {0, 0, 0};
                update_current(prev, data[r.pos], c);
                for(auto p = r.pos + 1; p < data.size(); p++)
                    propogate(data[p-1], data[p]);
            }

            return true;
        }

        const data_item& get(time_t t) const  
        {
            const auto r = index.find_pos(t);
            REQUIRE_RANGE(r.pos, 0, data.size());
            return data[r.pos];
        }

        data_item diff(time_type a, time_type b) 
        {
            if(a > b) std::swap(a,b);
            if(data.size() == 0) return data_item { 0, 0, 0};

            auto ar = index.find_pos(a);
            auto br = index.find_pos(b);

            ar.pos = std::max<offset_type>(0, ar.pos);
            ar.pos = std::min<offset_type>(data.size() - 1, ar.pos);

            br.pos = std::max<offset_type>(0, br.pos);
            br.pos = std::min<offset_type>(data.size() - 1, br.pos);

            const auto& ad = data[ar.pos]; 
            const auto& bd = data[br.pos]; 
            return diff_buckets(ad, bd, br.pos - ar.pos);
        }
    };

    timeline from_directory(const std::string& path) 
    {
        if(!bf::is_directory(path))
           throw std::runtime_error{"path " + path + " is not a directory"}; 
    
        bf::path root = path;

        timeline t;
        t.key = root.filename().string();

        bf::path idx_meta = root / "im";
        bf::path idx_data = root / "id";
        t.index = index_type{idx_meta, idx_data};
        
        bf::path cmeta = root / "m";
        bf::path cdata = root / "d";

        t.data = data_type{cmeta, cdata, DATA_SIZE};
        
        return t;
    }
}

int main(int argc, char** argv)
try
{
    auto db = flyfish::from_directory("./tmp");

    int tm = 0;

    const int TOTAL = 100000000;
    //const int TOTAL = 10000000;
    const int TIME_INC = 10;
    const int CHANGE = 2;

    if(db.data.size() == 0)
    {
        for(int a = 0; a < TOTAL; a++)
        {
            tm += TIME_INC;
            db.put(tm, 1);
        }
    }
    else
    {
        tm = TOTAL * TIME_INC;
    }

    auto d = db.diff(tm/2, tm/4);

    std::cout << "diff avg: " << d.value << std::endl;
    std::cout << "diff total: " << d.total << std::endl;
    std::cout << "diff integral: " << d.integral << std::endl;

    std::cout << "total puts: " << TOTAL << std::endl;
    std::cout << "ranges: " << db.index.size() << std::endl;
    std::cout << "buckets: " << db.data.size() << std::endl;


    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "Error! " << e.what() << std::endl;
    return 1;
}
