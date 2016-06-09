#ifndef FLYFISH_TIMELINE_H
#define FLYFISH_TIMELINE_H

#include "util/dbc.hpp"
#include "util/mapped_vector.hpp"

#include <string>
#include <algorithm>
#include <cmath>

namespace flyfish
{
    namespace db 
    {
        using time_type = std::uint64_t;
        using count_type = std::uint64_t;
        using change_type = std::int64_t;
        using offset_type = std::uint64_t;

        struct index_item
        {
            time_type time = 0;
            offset_type pos = 0;
        };

        struct index_metadata
        {
            std::uint64_t size = 0 ;
            std::uint64_t resolution = 0;
            std::uint64_t frame_size = 0;
        };

        struct data_metadata
        {
            std::uint64_t size = 0;
        };

        struct data_item
        {
            count_type value;
            count_type integral;
            count_type second_integral;
        };

        const std::size_t DATA_SIZE = util::PAGE_SIZE;
        const std::uint64_t DEFAULT_RESOLUTION = 5; //seconds
        const std::size_t INDEX_SIZE = util::PAGE_SIZE;
        const std::size_t FRAME_SIZE = DATA_SIZE / sizeof(data_item);

        struct pos_result
        {
            index_item range;
            offset_type offset;
            offset_type pos;
        };

        class index_type : public util::mapped_vector<index_metadata, index_item>
        {
            public:
                index_type() : util::mapped_vector<index_metadata, index_item>{} {};
                index_type(const bf::path& meta_file, const bf::path& data_file) :
                    util::mapped_vector<index_metadata, index_item>{meta_file, data_file, INDEX_SIZE}
                {
                    REQUIRE(_metadata);

                    if(_metadata->resolution == 0) _metadata->resolution = DEFAULT_RESOLUTION;
                    if(_metadata->frame_size == 0) _metadata->frame_size = FRAME_SIZE;
                }

                index_item find_range(time_type t) const 
                {
                    REQUIRE(_metadata);
                    REQUIRE(_items);

                    auto r = std::upper_bound(cbegin(), cend(), t, 
                            [](time_type l, const auto& r) { return l < r.time;});

                    if(r == cend()) return back();
                    return r != cbegin() ? *(r - 1) : front();
                }

                pos_result find_pos_from_range(time_type t, const index_item range) const
                {
                    REQUIRE(_metadata);

                    t = std::max(t, range.time);
                    const auto offset_time = t - range.time;
                    const auto offset = offset_time / _metadata->resolution;
                    return pos_result{ range, offset, range.pos + offset};
                }

                pos_result find_pos_within_range(time_type t, const index_item range) const
                {
                    REQUIRE(_metadata);

                    t = std::max(t, range.time);

                    const auto offset_time = t - range.time;
                    const auto offset = std::min(
                            offset_time / _metadata->resolution, 
                            _metadata->frame_size-1);

                    return pos_result{ range, offset, range.pos + offset};
                }

                pos_result find_pos(time_type t) const
                {
                    if(size() == 0) return pos_result {index_item { t, 0}, 0, 0};
                    const auto range = find_range(t);
                    return find_pos_within_range(t, range);
                }
        };

        using key_t = std::string;

        using data_type = util::mapped_vector<data_metadata, data_item>;

        struct diff_result
        {
            count_type sum;
            count_type mean;
            count_type variance;
            change_type change;
            count_type size;
        };

        struct timeline
        {
            key_t key;
            index_type index;
            data_type data;

            bool put(time_type t, count_type c);
            const data_item& get(time_t t) const;  
            diff_result diff(time_type a, time_type b) const;
        };

        timeline from_directory(const std::string& path);
    }
}
#endif
