#ifndef FLYFISH_MVECTOR_H
#define FLYFISH_MVECTOR_H

#include "util/dbc.hpp"
#include "util/mmap.hpp"

#include <memory>

namespace henhouse::util
{
    template<typename meta_t, typename data_type>
        class mapped_vector
        {
            public:
                mapped_vector(){};
                mapped_vector(
                        const boost::filesystem::path& data_file, 
                        const size_t new_size = PAGE_SIZE,
                        const float new_size_factor = GROW_FACTOR) 
                {
                    REQUIRE_GREATER(new_size, 0);

                    _new_size = std::max(new_size, sizeof(meta_t) + sizeof(data_type));
                    _data_file_path = data_file;
                    _new_size_factor = new_size_factor;

                    //open index data. New file size is new_size
                    _data_file = std::make_shared<bio::mapped_file>();
                    const bool created = open(*_data_file, data_file, new_size);

                    _metadata = reinterpret_cast<meta_t*>(_data_file->data());
                    _items = reinterpret_cast<data_type*>(_data_file->data() + sizeof(meta_t));

                    if(created) 
                        *_metadata = meta_t{};

                    //compute max elements
                    _max_items = (_data_file->size() - sizeof(meta_t)) / sizeof(data_type);
                    CHECK_LESS_EQUAL(_metadata->size, _max_items);

                    ENSURE(_data_file);
                    ENSURE(_metadata != nullptr);
                    ENSURE(_items != nullptr);
                    ENSURE_LESS_EQUAL(_metadata->size, _max_items);
                }

                meta_t& meta() 
                {
                    INVARIANT(_metadata);
                    return *_metadata;
                }

                const meta_t& meta() const
                {
                    INVARIANT(_metadata);
                    return *_metadata;
                }

                std::uint64_t size() const 
                {
                    INVARIANT(_metadata);
                    INVARIANT_LESS_EQUAL(_metadata->size, _max_items);
                    return _metadata->size;
                }

                bool empty() const
                {
                    return size() == 0;
                }

                const data_type& operator[](size_t pos) const 
                {
                    INVARIANT(_metadata); 
                    INVARIANT(_items); 
                    REQUIRE_LESS(pos, _metadata->size);

                    return _items[pos];
                }

                data_type& operator[](size_t pos)
                {
                    INVARIANT(_metadata); 
                    INVARIANT(_items); 
                    REQUIRE_LESS(pos, _metadata->size);

                    return _items[pos];
                }

                void push_back(const data_type& v) 
                {
                    INVARIANT(_data_file);
                    INVARIANT(_metadata);
                    REQUIRE_LESS_EQUAL(_metadata->size, _max_items);

                    const auto next_pos = _metadata->size;

                    if(next_pos >= _max_items)
                    {
                        auto new_size = std::max(
                                static_cast<std::size_t>(_data_file->size() + sizeof(data_type)), 
                                (static_cast<std::size_t>(_data_file->size() * _new_size_factor) + sizeof(data_type)));
                        resize(new_size);
                    }

                    _metadata->size++;
                    _items[next_pos] = v;

                    ENSURE_GREATER_EQUAL(_max_items, _metadata->size);
                }

                data_type* begin() 
                { 
                    INVARIANT(_items);
                    return _items;
                }

                data_type* end() 
                { 
                    INVARIANT(_items);
                    INVARIANT(_metadata);
                    return _items + size();
                }

                data_type* begin() const
                { 
                    INVARIANT(_items);
                    return _items;
                }

                data_type* end() const
                { 
                    INVARIANT(_items);
                    INVARIANT(_metadata);
                    return _items + size();
                }

                const data_type* cbegin() { return begin(); }
                const data_type* cend() { return end(); }
                const data_type* cbegin() const { return begin(); }
                const data_type* cend() const { return end(); }

                data_type& front()
                {
                    INVARIANT(_items);
                    return *_items;
                }

                const data_type& front() const
                {
                    INVARIANT(_items);
                    return *_items;
                }

                data_type& back()
                {
                    INVARIANT(_items);
                    INVARIANT(_metadata);
                    REQUIRE_GREATER(_metadata->size, 0);
                    return *(_items + (_metadata->size - 1));
                }

                const data_type& back() const
                {
                    INVARIANT(_items);
                    INVARIANT(_metadata);
                    REQUIRE_GREATER(_metadata->size, 0);
                    return *(_items + (_metadata->size - 1));
                }

            private:

                void resize(size_t new_size) 
                {
                    INVARIANT(_data_file);
                    REQUIRE_GREATER_EQUAL(new_size, _data_file->size() + sizeof(data_type));

                    const auto old_max = _max_items;

                    _data_file->resize(new_size);
                    _metadata = reinterpret_cast<meta_t*>(_data_file->data());
                    _items = reinterpret_cast<data_type*>(_data_file->data() + sizeof(meta_t));
                    _max_items = (_data_file->size() - sizeof(meta_t)) / sizeof(data_type);

                    ENSURE_GREATER(_max_items, old_max);
                }

            protected:
                meta_t* _metadata = nullptr;
                data_type* _items = nullptr;
                std::size_t _new_size = 0;
                std::size_t _max_items = 0;
                float _new_size_factor = 0;
                mapped_file_ptr _data_file;
                boost::filesystem::path _data_file_path;
        };
}
#endif
