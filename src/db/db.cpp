#include "db/db.hpp"

#include <algorithm>
#include <functional>

#include <boost/regex.hpp>

namespace henhouse
{
    namespace db
    {
        namespace 
        {
            std::string sanatize_key(const std::string& key)
            {
                std::string res;
                res.resize(key.size());
                std::transform(std::begin(key), std::end(key),
                        std::begin(res),
                        [](char c)
                        {
                            if((c >= '0' && c <= '9') ||
                               (c >= 'A' && c <= 'Z') ||
                               (c >= 'a' && c <= 'Z')) return c;
                            else return '.';
                        });
                return res;
            }

            bf::path get_key_dir(const bf::path root, const std::string& key)
            {
                REQUIRE(!key.empty());
                if(key.size() <=2)
                    return root / key;

                return root / key.substr(0, 2) / key.substr(2);
            }
        }


        get_result timeline_db::get(const std::string& key, time_type t) const 
        {
            const auto& tl = get_tl(key);
            return tl.get_b(t);
        }

        bool timeline_db::put(const std::string& key, time_type t, count_type count)
        {
            auto& tl = get_tl(key);
            return tl.put(t, count);
        }

        diff_result timeline_db::diff(const std::string& key, time_type a, time_type b) const
        {
            const auto& tl = get_tl(key);
            return tl.diff(a, b);
        }

        std::size_t timeline_db::key_index_size(const std::string& key) const
        {
            const auto& tl = get_tl(key);
            return tl.index.size();
        }

        std::size_t timeline_db::key_data_size(const std::string& key) const
        {
            const auto& tl = get_tl(key);
            return tl.data.size();
        }

        timeline& timeline_db::get_tl(const std::string& key)
        {
            const auto clean_key = sanatize_key(key);
            const auto t = _tls.find(clean_key);
            if(t != std::end(_tls)) return t->second;

            bf::path key_dir = get_key_dir(_root, clean_key);

            if(!bf::exists(key_dir)) bf::create_directories(key_dir);

            _tls.set(clean_key, from_directory(key_dir.string()));
            auto p = _tls.find(clean_key);
            return p->second;
        }

        const timeline& timeline_db::get_tl(const std::string& key) const
        {
            const auto clean_key = sanatize_key(key);
            const auto t = _tls.find(clean_key);
            if(t != std::end(_tls)) return t->second;

            bf::path key_dir = get_key_dir(_root, clean_key);

            if(!bf::exists(key_dir)) bf::create_directories(key_dir);

            _tls.set(clean_key, from_directory(key_dir.string()));
            auto p = _tls.find(clean_key);
            return p->second;
        }
    }
}
