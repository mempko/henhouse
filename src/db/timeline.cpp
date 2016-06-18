
#include "db/timeline.hpp"

#include <exception>
#include <fstream>
#include <functional>

namespace henhouse
{
    namespace db
    {
        const offset_type ADD_BUCKET_BACK_LIMIT = 60;

        void propogate(data_item prev, data_item& current)
        {
            current.integral = prev.integral + current.value;
            current.second_integral = prev.second_integral + (current.value * current.value);
        }

        void update_current(data_item prev, data_item& current, count_type c)
        {
            current.value += c;
            propogate(prev, current);
        }

        diff_result smooth_diff_bucket(time_type time_diff, data_item a, time_type resolution)
        {
            REQUIRE_GREATER(time_diff, 0);
            REQUIRE_GREATER_EQUAL(resolution, time_diff);

            const count_type sub_size = resolution / time_diff;
            const auto sum = a.value / sub_size;
            const auto mean = sum;
            const auto variance = 0;
            const change_type change = 0;

            return diff_result 
            { 
                sum, 
                    mean, 
                    variance,
                    change,
                    0
            };
        }

        diff_result diff_buckets(data_item a, data_item b, count_type n)
        {
            REQUIRE_GREATER_EQUAL(n, 0);

            const auto sum = b.integral - a.integral;
            const auto second_sum = b.second_integral - a.second_integral;
            const auto mean = n > 0 ? sum / n : a.value;
            const auto mean_squared = mean * mean;
            const auto second_mean = n > 0 ? second_sum / n : (a.value * a.value);
            const auto variance = second_mean - mean_squared;
            const change_type change = b.value > a.value ? (b.value - a.value) : -(a.value - b.value);

            return diff_result 
            { 
                sum, 
                mean, 
                variance,
                change,
                n
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

        void clamp(pos_result& r, std::size_t size)
        {
            REQUIRE_LESS(r.pos, size);

            const auto pos = r.pos + r.offset;
            if(pos < size) return;
            r.offset = size - r.pos - 1;
        }

        get_result timeline::get(time_t t) const  
        {
            auto p = index.find_pos(t);
            clamp(p, data.size());
            return get_result 
            { 
                p.time, 
                p.pos,
                p.offset,
                data[p.pos + p.offset] 
            };
        }

        template <class T>
            T within(T v, T a, T b)
            {
                v = std::max(a, v);
                v = std::min(v, b);
                return v;
            }

        diff_result timeline::diff(time_type a, time_type b) const
        {
            if(a > b) std::swap(a,b);
            if(data.size() == 0) return diff_result{ 0, 0, 0};

            auto ar = get(a);
            auto br = get(b);

            b = std::max(b, br.time);
            a = within(a, ar.time, b);

            if(a == b) return diff_result{ 0, 0, 0 };

            CHECK_GREATER_EQUAL(b, a);
            CHECK_GREATER_EQUAL(br.pos, ar.pos);

            const auto resolution = index.meta().resolution;
            const auto time_diff = b - a;
            const auto n = time_diff / resolution;

            return n > 0 ? 
                diff_buckets(ar.value, br.value, n) : 
                smooth_diff_bucket(time_diff, ar.value, resolution);
        }

        timeline from_directory(const std::string& path) 
        {
            bf::create_directory(path);
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
}
