#include "db/db.hpp"

#include <algorithm>
#include <functional>

namespace henhouse
{
    namespace db
    {
        std::string sanatize_key(const std::string& key)
        {
            //clean up key here
            return key;
        }

        get_result timeline_db::get(const std::string& key, time_type t) const 
        {
            SYNCHRONIZED(c, _tls)
            {
                auto tl = get_tl(*c, key);
                return tl->get_b(t);
            }
        }

        bool timeline_db::put(const std::string& key, time_type t, count_type count)
        {
            SYNCHRONIZED(c, _tls)
            {
                auto tl = get_tl(*c, key);
                return tl->put(t, count);
            }
        }

        diff_result timeline_db::diff(const std::string& key, time_type a, time_type b) const
        {
            SYNCHRONIZED(c, _tls)
            {
                auto tl = get_tl(*c, key);
                return tl->diff(a, b);
            }
        }

        std::size_t timeline_db::key_index_size(const std::string& key) const
        {
            SYNCHRONIZED(c, _tls)
            {
                auto tl = get_tl(*c, key);
                return tl->index.size();
            }
        }

        std::size_t timeline_db::key_data_size(const std::string& key) const
        {
            SYNCHRONIZED(c, _tls)
            {
                auto tl = get_tl(*c, key);
                return tl->data.size();
            }
        }

        safe_timeline timeline_db::get_tl(timeline_cache& c, const std::string& key)
        {
            const auto clean_key = sanatize_key(key);
            auto t = c.find(clean_key);
            if(t != std::end(c)) return t->second;

            bf::path key_dir = _root / clean_key;

            c.set(clean_key, safe_timeline{from_directory(key_dir.string())});
            auto p = c.find(clean_key);
            return p->second;
        }

        safe_timeline timeline_db::get_tl(timeline_cache& c, const std::string& key) const
        {
            const auto clean_key = sanatize_key(key);
            auto t = c.find(clean_key);
            if(t != std::end(c)) return t->second;

            bf::path key_dir = _root / clean_key;

            c.set(clean_key, safe_timeline{from_directory(key_dir.string())});
            auto p = c.find(clean_key);
            return p->second;
        }
    }
}
