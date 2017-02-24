#ifndef FLYFISH_DB_H
#define FLYFISH_DB_H

#include "db/timeline.hpp"

#include <folly/EvictingCacheMap.h>
#include <folly/FBString.h>

namespace henhouse::db
{
    using timeline_cache = folly::EvictingCacheMap<folly::fbstring, timeline>;

    class timeline_db 
    {
        public:
            timeline_db(const std::string& root, const std::size_t cache_size, const time_type new_timeline_resolution) : 
                _root{root}, _new_tl_resolution{new_timeline_resolution}, _tls{cache_size} 
            {
                REQUIRE(!root.empty());
                REQUIRE_GREATER(cache_size, 0);
                REQUIRE_GREATER(new_timeline_resolution, 0);
            }

        public:

            summary_result summary(const std::string& key) const;
            get_result get(const std::string& key, time_type t) const;
            bool put(const std::string& key, time_type t, count_type c);
            diff_result diff(const std::string& key, time_type a, time_type b, const offset_type index_offset) const;
            std::size_t key_index_size(const std::string& key) const;
            std::size_t key_data_size(const std::string& key) const;

        private:

            timeline& get_tl(const std::string& key);
            const timeline& get_tl(const std::string& key) const;

        private:
            boost::filesystem::path _root;
            time_type _new_tl_resolution;
            mutable timeline_cache _tls;
    };
}
#endif
