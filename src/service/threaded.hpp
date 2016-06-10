#ifndef FLYFISH_THREADED_H
#define FLYFISH_THREADED_H

#include <iostream>
#include <thread>
#include <memory>

#include "db/db.hpp"

#include <folly/MPMCQueue.h>

namespace flyfish
{
    namespace threaded
    {
        struct put_req
        {
            std::string key;
            db::time_type time;
            db::count_type count;
        };

        using put_queue = folly::MPMCQueue<put_req>;

        class worker  
        {
            public: 
                worker(const std::string & root, std::size_t queue_size);

                put_queue& queue() { return _queue;}
                const put_queue & queue() const { return _queue;}

                db::timeline_db& db() { return _db;}
                const db::timeline_db& db() const { return _db;}

            private:
                put_queue _queue;

                db::timeline_db _db;
        };

        using worker_ptr = std::shared_ptr<worker>;
        using workers = std::vector<worker_ptr>;
        using worker_thread_ptr = std::shared_ptr<std::thread>;
        using threads = std::vector<worker_thread_ptr>;

        class server  
        {
            public:
                server(std::size_t workers, const std::string& root);

                db::get_result get(const std::string& key, db::time_type t) const; 
                bool put(const std::string& key, db::time_type t, db::count_type c);
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
