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
            auto t = _tls.find(clean_key);
            if(t != std::end(_tls)) return t->second;

            bf::path key_dir = _root / clean_key;

            _tls.set(clean_key, from_directory(key_dir.string()));
            auto p = _tls.find(clean_key);
            return p->second;
        }

        const timeline& timeline_db::get_tl(const std::string& key) const
        {
            const auto clean_key = sanatize_key(key);
            auto t = _tls.find(clean_key);
            if(t != std::end(_tls)) return t->second;

            bf::path key_dir = _root / clean_key;

            _tls.set(clean_key, from_directory(key_dir.string()));
            auto p = _tls.find(clean_key);
            return p->second;
        }
    }
}
