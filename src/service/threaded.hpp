#ifndef FLYFISH_THREADED_H
#define FLYFISH_THREADED_H

#include <experimental/string_view>
#include <iostream>
#include <thread>
#include <future>
#include <memory>
#include <boost/variant.hpp>

#include "db/db.hpp"

#include <folly/MPMCQueue.h>
#include <folly/FBString.h>

namespace stde = std::experimental;

namespace henhouse::threaded
{
    enum req_type { put, get, diff, summary};
    using get_promise = std::promise<db::get_result>;
    using get_future = std::future<db::get_result>;
    using diff_promise = std::promise<db::diff_result>;
    using diff_future = std::future<db::diff_result>;
    using summary_promise = std::promise<db::summary_result>;
    using summary_future = std::future<db::summary_result>;

    struct put_req
    {
        folly::fbstring key;
        db::time_type time;
        db::count_type count;
    };

    struct get_req
    {
        folly::fbstring key;
        db::time_type time;
        get_promise result;
    };

    struct diff_req
    {
        folly::fbstring key;
        db::time_type a;
        db::time_type b;
        db::offset_type index_offset;
        diff_promise result;
    };

    struct summary_req
    {
        stde::string_view key;
        summary_promise result;
    };

    using req = boost::variant<put_req, get_req, diff_req, summary_req>; 

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

    using worker_ptr = std::unique_ptr<worker>;
    using workers = std::vector<worker_ptr>;
    using worker_thread_ptr = std::unique_ptr<std::thread>;
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

            summary_future summary(const stde::string_view& key) const; 
            get_future get(const stde::string_view& key, db::time_type t) const; 
            void put(const stde::string_view& key, db::time_type t, db::count_type c);
            diff_future diff(const stde::string_view& key, db::time_type a, db::time_type b, const db::offset_type index_offset) const;

            void stop();

        private:

            std::size_t worker_num(const stde::string_view& key) const;

        private:
            std::string _root;
            workers _workers;
            threads _threads;
            bool _done;
    };
}
#endif
