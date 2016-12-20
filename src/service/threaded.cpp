#include "service/threaded.hpp"

namespace henhouse
{
    namespace threaded
    {
        const std::size_t QUEUE_SIZE = 1000;

        worker::worker(
                const std::string & root, 
                const std::size_t queue_size, 
                const std::size_t cache_size,
                const db::time_type new_timeline_resolution) : 
            _db{root, cache_size, new_timeline_resolution}, _queue{queue_size}
        {
            REQUIRE_GREATER(queue_size, 0);
            REQUIRE_GREATER(cache_size, 0);
            REQUIRE_GREATER(new_timeline_resolution, 0);
        }

        void req_thread(worker_ptr w) 
        {
            REQUIRE(w);

            auto& q = w->queue();
            auto& db = w->db();

            while(true)
            {
                req r;
                q.blockingRead(r);
                switch(r.type)
                {
                    case req_type::put:
                        try
                        {
                            db.put(r.key, r.a, r.count);
                        }
                        catch(std::exception& e) 
                        {
                            std::cerr << "Error putting data: " << r.key << " " << r.count 
                                << " " << r.count << ": " << e.what() << std::endl;
                        }
                        break;
                    case req_type::get:
                        try
                        {
                            REQUIRE(r.get_result);
                            r.get_result->set_value(db.get(r.key, r.a));
                        }
                        catch(std::exception& e) 
                        {
                            CHECK(r.get_result);
                            std::cerr << "Error getting data: " << r.key 
                                << " " << r.a << ": " << e.what() << std::endl;
                            r.get_result->set_value(db::get_result{});
                        }
                        break;
                    case req_type::diff:
                        try
                        {
                            REQUIRE(r.diff_result);
                            r.diff_result->set_value(db.diff(r.key, r.a, r.b, r.index_offset));
                        }
                        catch(std::exception& e) 
                        {
                            CHECK(r.diff_result);
                            std::cerr << "Error diffing data: " << r.key
                                << " (" << r.a << ", " << r.b << "): " << e.what() << std::endl;
                            r.diff_result->set_value(db::diff_result{});

                        }
                        break;
                    case req_type::summary:
                        try
                        {
                            REQUIRE(r.summary_result);
                            r.summary_result->set_value(db.summary(r.key));
                        }
                        catch(std::exception& e) 
                        {
                            CHECK(r.summary_result);
                            std::cerr << "Error summing data: " << r.key
                                << ": " << e.what() << std::endl;
                            r.summary_result->set_value(db::summary_result{});
                        }
                        break;
                    default: CHECK(false && "missed case");
                }
            }

        }

        server::server(
                const std::size_t total_workers, 
                const std::string& root, 
                const std::size_t queue_size,
                const std::size_t cache_size,
                const db::time_type new_timeline_resolution) : _root{root} 
        {
            REQUIRE_GREATER(total_workers, 0);
            REQUIRE_GREATER(queue_size, 0);
            REQUIRE_GREATER(cache_size, 0);
            REQUIRE_GREATER(new_timeline_resolution, 0);

            auto workers = total_workers;

            while(--workers)
            {

                auto w = std::make_shared<worker>(_root, queue_size, cache_size, new_timeline_resolution);
                _workers.emplace_back(w);

                auto t = std::make_shared<std::thread>(req_thread, w);
                _threads.emplace_back(t);
            }
        }

        void server::put(const std::string& key, db::time_type t, db::count_type c)
        {
            auto n = worker_num(key);

            req r;
            r.type = req_type::put;
            r.key = key;
            r.a = t;
            r.count = c;
            _workers[n]->queue().write(std::move(r));
        }

        db::summary_result server::summary(const std::string& key) const 
        {
            auto n = worker_num(key);
            req r;
            r.type = req_type::summary;
            r.key = key;
            summary_promise p;
            summary_future f = p.get_future();
            r.summary_result = &p;
            _workers[n]->queue().write(std::move(r));
            return f.get();
        }

        db::get_result server::get(const std::string& key, db::time_type t) const 
        {
            auto n = worker_num(key);

            req r;
            r.type = req_type::get;
            r.key = key;
            r.a = t;
            get_promise p;
            get_future f = p.get_future();
            r.get_result = &p;
            _workers[n]->queue().write(std::move(r));
            return f.get();
        }

        db::diff_result server::diff(const std::string& key, db::time_type a, db::time_type b, const db::offset_type index_offset) const
        {
            auto n = worker_num(key);

            req r;
            r.type = req_type::diff;
            r.key = key;
            r.a = a;
            r.b = b;
            r.index_offset = index_offset;
            diff_promise p;
            diff_future f = p.get_future();
            r.diff_result = &p;
            _workers[n]->queue().write(std::move(r));
            return f.get();
        }

        std::size_t server::worker_num(const std::string& key) const
        {
            auto h = std::hash<std::string>{}(key);
            auto n = h % _workers.size(); //exclude rank 0

            ENSURE_RANGE(n, 0, _workers.size());
            return n; 
        }

    }
}
