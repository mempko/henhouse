#include "db/db.hpp"

#include <algorithm>
#include <functional>
#include <boost/filesystem.hpp>

#include <boost/regex.hpp>

namespace fs = boost::filesystem;

namespace henhouse::db
{
    namespace 
    {
        const int MAX_DIR_LENGTH = 8;
        const int MAX_DIR_SPLIT_LENGTH = MAX_DIR_LENGTH * 4;
        const offset_type NO_OFFSET = 0;

        fs::path get_key_dir(const fs::path& root, const stde::string_view& key)
        {
            REQUIRE(!key.empty());
            fs::path p = root;

            for(std::size_t i = 0; i < key.size() && i < MAX_DIR_SPLIT_LENGTH; i += MAX_DIR_LENGTH)
            {
                const auto d = key.substr(i, MAX_DIR_LENGTH);
                p.append(d.begin(), d.end());
            }

            if(key.size() > MAX_DIR_SPLIT_LENGTH)
            {
                const auto d = key.substr(MAX_DIR_SPLIT_LENGTH, key.size() - MAX_DIR_SPLIT_LENGTH);
                p.append(d.begin(), d.end());
            }

            return p;
        }
    }

    void sanatize_key(std::string& res, const stde::string_view& key)
    {
        res.assign(key.data(), key.size());
        std::replace_if(std::begin(res), std::end(res),
                [](char c)
                {
                return !((c >= '0' && c <= '9') ||
                        (c >= 'A' && c <= 'Z') ||
                        (c >= 'a' && c <= 'z'));
                }, '_');
    }

    summary_result timeline_db::summary(const stde::string_view& key) const
    {
        const auto& tl = get_tl(key);
        return tl.summary();
    }

    get_result timeline_db::get(const stde::string_view& key, time_type t) const 
    {
        const auto& tl = get_tl(key);
        return tl.get(t, NO_OFFSET);
    }

    bool timeline_db::put(const stde::string_view& key, time_type t, count_type count)
    {
        auto& tl = get_tl(key);
        return tl.put(t, count);
    }

    diff_result timeline_db::diff(const stde::string_view& key, time_type a, time_type b, const offset_type index_offset) const
    {
        const auto& tl = get_tl(key);
        return tl.diff(a, b, index_offset);
    }

    std::size_t timeline_db::key_index_size(const stde::string_view& key) const
    {
        const auto& tl = get_tl(key);
        return tl.index.size();
    }

    std::size_t timeline_db::key_data_size(const stde::string_view& key) const
    {
        const auto& tl = get_tl(key);
        return tl.data.size();
    }

    timeline& timeline_db::get_tl(const stde::string_view& key)
    {
        REQUIRE_FALSE(key.empty());

        const auto h = std::hash<stde::string_view>{}(key);

        const auto t = _tls.find(h);
        if(t != std::end(_tls)) return t->second;

        const auto key_dir = get_key_dir(_root, key);

        if(!fs::exists(key_dir)) fs::create_directories(key_dir);

        _tls.set(h, from_directory(key_dir.string(), _new_tl_resolution));
        auto p = _tls.find(h);

        return p->second;
    }

    const timeline& timeline_db::get_tl(const stde::string_view& key) const
    {
        REQUIRE_FALSE(key.empty());

        const auto h = std::hash<stde::string_view>{}(key);

        const auto t = _tls.find(h);
        if(t != std::end(_tls)) return t->second;

        const auto key_dir = get_key_dir(_root, key);

        if(!fs::exists(key_dir)) fs::create_directories(key_dir);

        _tls.set(h, from_directory(key_dir.string(), _new_tl_resolution));
        auto p = _tls.find(h);
        return p->second;
    }
}
