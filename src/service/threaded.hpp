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
        enum req_type { put, get, diff};
        using get_promise = std::promise<db::get_result>;
        using get_future = std::future<db::get_result>;
        using diff_promise = std::promise<db::diff_result>;
        using diff_future = std::future<db::diff_result>;

        struct req
        {
            req_type type;

            std::string key;
            db::time_type a;
            db::time_type b;
            db::count_type count;

            get_promise* get_result;;
            diff_promise* diff_result;
        };

        using req_queue= folly::MPMCQueue<req>;

        class worker  
        {
            public: 
                worker(const std::string & root, std::size_t queue_size, std::size_t cache_size);

                req_queue& queue() { return _queue;}
                const req_queue & queue() const { return _queue;}

                db::timeline_db& db() { return _db;}
                const db::timeline_db& db() const { return _db;}

            private:
                req_queue _queue;

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
                        std::size_t workers, 
                        const std::string& root, 
                        std::size_t queue_size, 
                        std::size_t cache_size);

                db::get_result get(const std::string& key, db::time_type t) const; 
                void put(const std::string& key, db::time_type t, db::count_type c);
                db::diff_result diff(const std::string& key, db::time_type a, db::time_type b) const;

            private:

                std::size_t worker_num(const std::string& key) const;

            private:
                std::string _root;
                workers _workers;
                threads _threads;
        };
    }
}
#endif
