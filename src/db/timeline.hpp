#ifndef FLYFISH_TIMELINE_H
#define FLYFISH_TIMELINE_H

#include "util/dbc.hpp"
#include "util/mapped_vector.hpp"

#include <string>
#include <algorithm>
#include <cmath>

namespace henhouse::db
{
    using time_type = std::uint64_t;
    using count_type = std::int64_t;
    using offset_type = std::uint64_t;

    //TODO, use rational numbers instead in the future.
    using mean_type = double;
    using variance_type = double;

    struct index_item
    {
        time_type time = 0;
        offset_type pos = 0;
    };

    struct index_metadata
    {
        std::size_t size = 0 ;
        time_type resolution = 0;
    };

    struct data_metadata
    {
        std::size_t size = 0;
    };

    struct data_item
    {
        count_type value;
        count_type integral;
        count_type second_integral;
    };

    const std::size_t DATA_SIZE = util::PAGE_SIZE;
    const std::size_t INDEX_SIZE = util::PAGE_SIZE;
    const std::size_t FRAME_SIZE = DATA_SIZE / sizeof(data_item);

    struct pos_result
    {
        offset_type index_offset;
        time_type time;
        offset_type pos;
        offset_type offset;
        bool empty;
    };

    class index_type : public util::mapped_vector<index_metadata, index_item>
    {
        public:
            index_type() : util::mapped_vector<index_metadata, index_item>{} {};
            index_type(
                    const boost::filesystem::path& data_file, 
                    const time_type resolution) :
                util::mapped_vector<index_metadata, index_item>{data_file, INDEX_SIZE}
            {
                REQUIRE_GREATER(resolution, 0);
                INVARIANT(_metadata);

                if(_metadata->resolution == 0) _metadata->resolution = resolution;
            }

            const index_item* find_range(time_type t, const offset_type offset) const 
            {
                REQUIRE_LESS(offset, size());
                INVARIANT(_metadata);
                INVARIANT(_items);

                auto r = std::upper_bound(cbegin() + offset, cend(), t, 
                        [](time_type l, const auto& r) { return l < r.time;});

                return r != cbegin() ? r - 1: nullptr;
            }

            pos_result find_pos_from_range(
                    time_type t, 
                    const index_item* range, 
                    const index_item* next) const
            {
                INVARIANT(_metadata);
                REQUIRE(range);
                REQUIRE(next);

                t = std::max(t, range->time);
                const auto offset_time = t - range->time;
                auto offset = offset_time / _metadata->resolution;
                const auto pos = range->pos + offset;

                bool empty = false;
                //if we are not in last range, then check for overlap
                //with next index element
                if(next != cend() && pos >= next->pos)
                {
                    offset = next->pos - range->pos - 1;
                    empty = true;
                }
                const offset_type index_offset = range - cbegin();
                return pos_result{index_offset, range->time, range->pos, offset, empty};
            }

            pos_result find_pos(time_type t, const offset_type offset) const
            {
                if(empty()) return pos_result {0, t, 0, 0, true};
                const auto range = find_range(t, offset);
                if(range == nullptr) return pos_result{0, front().time, 0, 0, true};

                return find_pos_from_range(t, range, range + 1);
            }
    };

    using data_type = util::mapped_vector<data_metadata, data_item>;

    struct summary_result
    {
        time_type from;
        time_type to;
        time_type resolution;
        count_type sum;
        mean_type mean;
        variance_type variance;
        count_type size;
    };

    struct get_result
    {
        offset_type index_offset;
        time_type query_time;
        time_type range_time;
        offset_type pos;
        offset_type offset;
        data_item value;
    };

    struct diff_result
    {
        time_type resolution;       //resolution of smallest bucket
        offset_type index_offset;   //used for faster retrieval of a range of values
        count_type sum;             //values added within time range
        mean_type mean;             //mean of values added within time range
        variance_type variance;     //variance of values added within time range.
        count_type size;
        data_item left;             //left bucket
        data_item right;            //right bucket. 
    };

    struct timeline
    {
        index_type index;
        data_type data;

        bool put(time_type t, count_type c);

        summary_result summary() const;  

        //get_a returns the value at the bucket before time t bucket.
        get_result get_a(time_type t, const offset_type index_offset) const;  
        //get_v returns the value at the bucket at time t.
        get_result get_b(time_type t, const offset_type index_offset) const;  

        diff_result diff(time_type a, time_type b, const offset_type index_offset) const;
    };

    timeline from_directory(const std::string& path, const time_type resolution);
}
#endif
