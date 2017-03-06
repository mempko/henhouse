
#include "db/timeline.hpp"

#include <exception>
#include <thread>
#include <fstream>
#include <functional>
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

namespace henhouse::db
{
    const offset_type ADD_BUCKET_BACK_LIMIT = 60;

    /**
     * This is the main function to compute the partial sums given previous bucket.
     * It turns the current non-summed bucket into a summed bucket.
     *
     * current.value is assumed to be set to the count in that bucket.
     *
     * It computes partial sum(X) and partial sum(X^2) up the current bucket.
     */
    void propogate(data_item prev, data_item& current)
    {
        current.integral = prev.integral + current.value;
        current.second_integral = prev.second_integral + (current.value * current.value);
    }

    //Adds a count c to the current bucket and updates the partial sum
    //values of the current bucket. 
    void update_current(data_item prev, data_item& current, count_type c)
    {
        current.value += c;
        propogate(prev, current);
    }

    /**
     *
     * Mean is computing as the (running sum of x) / N.
     * In addition, variance requires maintaining the running sum of x^2
     *
     * mean = sum(x) / N
     * mean of squared x = sum(x^2) / N
     *
     * variance = (sum(x^2) / N) - (sum(x) / N )^2
     *          = (sum(x^2) / N) - mean^2
     *          = (mean of squared x) - mean^2
     *          = (mean of squared x) - (mean squared)
     */
    diff_result diff_buckets(const time_type resolution, const offset_type index_offset, const data_item a, const data_item b, const count_type n)
    {
        REQUIRE_GREATER(resolution, 0);
        REQUIRE_GREATER(n, 0);

        //Sum here is the values added within 
        const auto sum = b.integral - a.integral;
        const auto second_sum = b.second_integral - a.second_integral;
        const auto mean = static_cast<mean_type>(sum) / n;
        const auto mean_squared = mean * mean;
        const auto second_mean = static_cast<mean_type>(second_sum) / n;
        const auto variance = second_mean - mean_squared;

        return diff_result 
        { 
            resolution,
                index_offset,
                sum, 
                mean, 
                variance,
                n,
                a,
                b,
        };
    }

    bool timeline::put(time_type t, count_type c)
    {
        if(index.size() > 0)
        {
            const auto last_range = index.cend() - 1;

            //don't add if time is before last range
            if(t >= last_range->time) 
            {
                //get last position only because we want to keep 
                //a specific performance profile. This is a deliberate limitation.
                auto p = index.find_pos_from_range(t, last_range, index.cend());
                const auto pos = p.pos + p.offset;

                //bucket is current or in the past, no need to index.
                if(pos < data.size())
                {
                    //if we are too far back in the range, skip it,
                    //otherwise propogate the values up.
                    //This limitation is to keep performance predictable for
                    //inserts while providing a buffer for slow inserters
                    //to catch up.
                    if(data.size() - pos < ADD_BUCKET_BACK_LIMIT)
                    {
                        const auto prev = pos > 0 ? data[pos - 1] : data_item {0, 0, 0};
                        update_current(prev, data[pos], c);
                        for(auto p = pos + 1; p < data.size(); p++)
                            propogate(data[p-1], data[p]);
                    }
                    else return false;
                }
                //if we move beyond end, append data 
                else
                {
                    const auto last_pos = data.size() - 1;
                    const auto prev = data[last_pos];
                    data_item current = { c, 0};
                    propogate(prev, current);
                    data.push_back(current);

                    //skip if we have no gaps, otherwise index.
                    auto new_pos = last_pos + 1;
                    if(pos == new_pos) return true;

                    //index position
                    const auto resolution = index.meta().resolution;
                    CHECK_GREATER(resolution, 0);

                    const auto aliased_time = p.time + (p.offset * resolution);
                    index_item index_entry = {aliased_time, new_pos};

                    CHECK_LESS_EQUAL(aliased_time, t);
                    index.push_back(index_entry);
                }
            }
            else return false;
        }
        else
        {
            CHECK_EQUAL(data.size(), 0);

            data_item v{ c, 0, 0};
            data.push_back(v);

            index_item i = {t, 0};
            index.push_back(i);
        }

        return true;
    }

    summary_result timeline::summary() const  
    {
        const auto resolution = index.meta().resolution;
        CHECK_GREATER(resolution, 0);

        if(index.empty()) return summary_result{0,0,resolution, 0,0,0,0};
        REQUIRE(!data.empty());

        const auto front = index.front();
        const auto back = index.back();

        //time of first bucket
        const auto from = front.time;

        //compute time of last bucket
        CHECK_GREATER(data.size(), back.pos);
        auto last_buckets = data.size() - back.pos;
        auto to = back.time + (last_buckets * resolution);

        CHECK_GREATER(to, from);

        count_type n = (to - from) /  resolution;

        //if we have one bucket then first is empty data item
        auto first_bucket = data_item{0,0,0};
        auto last_bucket = data.back();

        //diff the two buckets
        auto diff = diff_buckets(resolution, 0, first_bucket, last_bucket, n);
        return summary_result 
        {
            from,
                to,
                resolution,
                diff.sum, 
                diff.mean, 
                diff.variance,
                n
        };
    }

    void clamp(pos_result& r, std::size_t size)
    {
        REQUIRE_LESS(r.pos, size);

        const auto pos = r.pos + r.offset;
        if(pos < size) return;
        r.offset = size - r.pos - 1;
        r.empty = true;

        ENSURE_RANGE(r.pos + r.offset, 0, size);
    }


    get_result timeline::get_a(time_type t, const offset_type index_offset) const  
    {
        auto p = index.find_pos(t, index_offset);

        clamp(p, data.size());
        const auto i = p.pos + p.offset + (p.empty ? 1 : 0);
        const auto dat = i > 0 ? data[i-1] : data_item{0,0,0};

        return get_result 
        { 
            p.index_offset,
                t,
                p.time, 
                p.pos,
                p.offset,
                dat
        };
    }

    get_result timeline::get_b(time_type t, const offset_type index_offset) const  
    {
        auto p = index.find_pos(t, index_offset);

        clamp(p, data.size());
        const auto dat = data[p.pos + p.offset];

        return get_result 
        { 
            p.index_offset,
                t,
                p.time, 
                p.pos,
                p.offset,
                dat
        };
    }

    diff_result timeline::diff(time_type a, time_type b, const offset_type index_offset) const
    {
        const auto resolution = index.meta().resolution;
        CHECK_GREATER(resolution, 0);

        if(a > b) std::swap(a,b);
        if(data.size() == 0) return diff_result{ resolution, 0, 0, 0, 0, 0, {0}, {0}};

        auto ar = get_a(a, index_offset);
        auto br = get_b(b, index_offset);

        b = std::max(br.query_time, br.range_time);
        a = std::min(ar.query_time, b);

        const auto time_diff = b - a;
        auto n = time_diff / resolution;

        if(n == 0) return diff_result{ resolution, 0, 0, 0, 0, 0, ar.value, br.value};

        CHECK_GREATER(n , 0);
        CHECK_LESS_EQUAL(ar.index_offset, br.index_offset);
        return diff_buckets(resolution, ar.index_offset, ar.value, br.value, n);
    }

    timeline from_directory(const std::string& path, const time_type resolution) 
    {
        REQUIRE(!path.empty());
        REQUIRE_GREATER(resolution, 0);

        fs::create_directory(path);
        if(!fs::is_directory(path))
            throw std::runtime_error{"path " + path + " is not a directory"}; 

        fs::path root = path;

        timeline t;

        fs::path idx_data = root / "_.i";
        t.index = std::move(index_type{idx_data, resolution});

        fs::path cdata = root / "_.d";
        t.data = std::move(data_type{cdata, DATA_SIZE});

        return t;
    }
}
