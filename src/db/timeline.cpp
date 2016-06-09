
#include "db/timeline.hpp"

#include <exception>
#include <fstream>
#include <functional>

namespace flyfish
{
    namespace db
    {
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

        diff_result smooth_diff_bucket(time_type time_diff, data_item a, std::uint64_t resolution)
        {
            REQUIRE_GREATER(time_diff, 0);
            REQUIRE_GREATER_EQUAL(resolution, time_diff);

            const auto sub_size = resolution / time_diff;
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

        diff_result diff_buckets(data_item a, data_item b, std::size_t n)
        {
            REQUIRE_GREATER_EQUAL(b.integral, a.integral);
            REQUIRE_GREATER_EQUAL(b.second_integral, a.second_integral);

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
                const auto& last_range = index.back();

                //don't add if time is before last range
                if(t >= last_range.time) 
                {
                    //get last position only because we want to keep 
                    //a specific performance profile. This is a deliberate limitation.
                    auto r = index.find_pos_from_range(t, last_range);

                    //bucket is current or in the past, no need to index.
                    if(r.pos < data.size())
                    {
                        const auto prev = r.pos > 0 ? data[r.pos - 1] : data_item {0, 0, 0};
                        update_current(prev, data[r.pos], c);
                        for(auto p = r.pos + 1; p < data.size(); p++)
                            propogate(data[p-1], data[p]);
                    }
                    //if we move beyond end, append data 
                    else
                    {
                        r.pos = data.size();
                        const auto prev = data[r.pos - 1];
                        data_item current = { c, 0};
                        propogate(prev, current);
                        data.push_back(current);

                        if(r.offset < index.meta().frame_size) return true;

                        //index only if the offset is same or bigger than the frame size.
                        const auto resolution = index.meta().resolution;
                        const auto aliased_time = r.range.time + (r.offset * resolution);
                        index_item index_entry = {aliased_time, r.pos};
                        index.push_back(index_entry);
                    }
                }
                else return false;
            }
            else
            {
                CHECK_EQUAL(data.size(), 0);

                data_item v{ c, 0};
                data.push_back(v);

                index_item i = {t, 0};
                index.push_back(i);
            }

            return true;
        }

        const data_item& timeline::get(time_t t) const  
        {
            const auto r = index.find_pos(t);
            REQUIRE_RANGE(r.pos, 0, data.size());
            return data[r.pos];
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

            auto ar = index.find_pos(a);
            auto br = index.find_pos(b);

            b = std::max(b, br.range.time);
            a = within(a, ar.range.time, b);

            if(a == b) return diff_result{ 0, 0, 0};

            ar.pos = within<offset_type>(ar.pos, 0, data.size() - 1);
            br.pos = within<offset_type>(br.pos, 0, data.size() - 1);

            const auto& ad = data[ar.pos]; 
            const auto& bd = data[br.pos]; 

            CHECK_GREATER_EQUAL(b, a);
            const auto resolution = index.meta().resolution;
            const auto time_diff = b - a;
            const auto n = time_diff / resolution;

            return n > 0 ? diff_buckets(ad, bd, n) : smooth_diff_bucket(time_diff, ad, resolution);
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
