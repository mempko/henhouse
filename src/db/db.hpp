#ifndef FLYFISH_DB_H
#define FLYFISH_DB_H

#include "db/timeline.hpp"

#include <experimental/string_view>
#include <folly/EvictingCacheMap.h>
#include <folly/FBString.h>

namespace stde = std::experimental;

namespace henhouse::db
{
    using timeline_cache = folly::EvictingCacheMap<folly::fbstring, timeline>;

    /**
     * Manages a cache of timelines based on key.
     * Note this interface is NOT thread safe.
     */
    class timeline_db 
    {
        public:
            timeline_db(const std::string& root, const std::size_t cache_size, const time_type new_timeline_resolution) : 
                _root{root}, _new_tl_resolution{new_timeline_resolution}, _tls{cache_size}
            {
                REQUIRE(!root.empty());
                REQUIRE_GREATER(cache_size, 0);
                REQUIRE_GREATER(new_timeline_resolution, 0);
                _clean_key.reserve(256);
            }

        public:

            summary_result summary(const stde::string_view& key) const;
            get_result get(const stde::string_view& key, time_type t) const;
            bool put(const stde::string_view& key, time_type t, count_type c);
            diff_result diff(const stde::string_view& key, time_type a, time_type b, const offset_type index_offset) const;
            std::size_t key_index_size(const stde::string_view& key) const;
            std::size_t key_data_size(const stde::string_view& key) const;

        private:

            timeline& get_tl(const stde::string_view& key);
            const timeline& get_tl(const stde::string_view& key) const;

        private:
            boost::filesystem::path _root;
            time_type _new_tl_resolution;
            mutable timeline_cache _tls;
            mutable std::string _clean_key;
    };
}
#endif
