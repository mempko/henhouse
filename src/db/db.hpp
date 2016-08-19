#ifndef FLYFISH_DB_H
#define FLYFISH_DB_H

#include "db/timeline.hpp"

#include <folly/Synchronized.h>
#include <folly/EvictingCacheMap.h>

namespace henhouse
{
    namespace db
    {
        using safe_timeline = folly::Synchronized<timeline>;
        using timeline_cache = folly::EvictingCacheMap<std::string, safe_timeline>;
        using timeline_cache_ptr = std::unique_ptr<timeline_cache>;
        using safe_timeline_cache = folly::Synchronized<timeline_cache_ptr>;
        using locked_cache = safe_timeline_cache::LockedPtr;

        class timeline_db 
        {
            public:
                timeline_db(const std::string& root, std::size_t cache_size) : 
                    _root{root}, _tls{std::make_unique<timeline_cache>(cache_size)} {} 

            public:

                get_result get(const std::string& key, time_type t) const;
                bool put(const std::string& key, time_type t, count_type c);
                diff_result diff(const std::string& key, time_type a, time_type b) const;
                std::size_t key_index_size(const std::string& key) const;
                std::size_t key_data_size(const std::string& key) const;

            private:

                safe_timeline get_tl(timeline_cache&, const std::string& key);
                safe_timeline get_tl(timeline_cache&, const std::string& key) const;

            private:
                bf::path _root;
                mutable safe_timeline_cache _tls;
        };
    }
}
#endif
