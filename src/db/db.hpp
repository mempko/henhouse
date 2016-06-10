#ifndef FLYFISH_DB_H
#define FLYFISH_DB_H

#include "db/timeline.hpp"

#include <unordered_map>

namespace flyfish
{
    namespace db
    {
        using timeline_map = std::unordered_map<std::string, timeline>;

        class timeline_db 
        {
            public:
                timeline_db(const std::string& root) : _root{root} {} 

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
                mutable timeline_map _tls;
        };
    }
}
#endif
