#ifndef FLYFISH_DB_H
#define FLYFISH_DB_H

#include "db/timeline.hpp"

#include <folly/EvictingCacheMap.h>

namespace henhouse
{
    namespace db
    {
        using timeline_cache = folly::EvictingCacheMap<std::string, timeline>;

        class timeline_db 
        {
            public:
                timeline_db(const std::string& root, std::size_t cache_size) : 
                    _root{root}, _tls{cache_size} {} 

            public:

                get_result get(const std::string& key, time_type t) const;
                bool put(const std::string& key, time_type t, count_type c);
                diff_result diff(const std::string& key, time_type a, time_type b) const;
                std::size_t key_index_size(const std::string& key) const;
                std::size_t key_data_size(const std::string& key) const;

            private:

                timeline& get_tl(const std::string& key);
                const timeline& get_tl(const std::string& key) const;

            private:
                bf::path _root;
                mutable timeline_cache _tls;
        };
    }
}
#endif
