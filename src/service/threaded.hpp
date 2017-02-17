#ifndef FLYFISH_THREADED_H
#define FLYFISH_THREADED_H

#include <iostream>
#include <thread>
#include <future>
#include <memory>

#include "db/db.hpp"

#include <folly/MPMCQueue.h>

namespace henhouse
{
    namespace threaded
    {
        enum req_type { put, get, diff, summary};
        using get_promise = std::promise<db::get_result>;
        using get_future = std::future<db::get_result>;
        using diff_promise = std::promise<db::diff_result>;
        using diff_future = std::future<db::diff_result>;
        using summary_promise = std::promise<db::summary_result>;
        using summary_future = std::future<db::summary_result>;

        struct req
        {
            req_type type;

            std::string key;
            db::time_type a;
            db::time_type b;
            db::count_type count;
            db::offset_type index_offset;

            get_promise* get_result;;
            diff_promise* diff_result;
            summary_promise* summary_result;
        };

        using req_queue= folly::MPMCQueue<req>;

        class worker  
        {
            public: 
                worker(const std::string & root, 
                        const std::size_t queue_size, 
                        const std::size_t cache_size, 
                        const db::time_type new_timeline_resolution,
                        bool* done);

                req_queue& queue() { return _queue;}
                const req_queue & queue() const { return _queue;}

                db::timeline_db& db() { return _db;}
                const db::timeline_db& db() const { return _db;}

                bool done() const { INVARIANT(_done); return *_done;}

            private:
                req_queue _queue;

                bool* _done;
                db::timeline_db _db;
        };

        using worker_ptr = std::shared_ptr<worker>;
        using workers = std::vector<worker_ptr>;
        using worker_thread_ptr = std::shared_ptr<std::thread>;
        using threads = std::vector<worker_thread_ptr>;

        class server  
        {
            public:
                server(
                        const std::size_t workers, 
                        const std::string& root, 
                        const std::size_t queue_size, 
                        const std::size_t cache_size,
                        const db::time_type new_timeline_resolution);
                ~server();

                db::summary_result summary(const std::string& key) const; 
                db::get_result get(const std::string& key, db::time_type t) const; 
                void put(const std::string& key, db::time_type t, db::count_type c);
                db::diff_result diff(const std::string& key, db::time_type a, db::time_type b, const db::offset_type index_offset) const;

                void stop();

            private:

                std::size_t worker_num(const std::string& key) const;

            private:
                std::string _root;
                workers _workers;
                threads _threads;
                bool _done;
        };
    }
}
#endif
