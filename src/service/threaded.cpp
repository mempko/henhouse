#include "service/threaded.hpp"

namespace henhouse::threaded
{
    const std::size_t QUEUE_SIZE = 1000;

    worker::worker(
            const std::string & root, 
            const std::size_t queue_size, 
            const std::size_t cache_size,
            const db::time_type new_timeline_resolution,
            bool* done) : 
        _db{root, cache_size, new_timeline_resolution}, _queue{queue_size}, _done{done}
    {
        REQUIRE(done);
        REQUIRE_GREATER(queue_size, 0);
        REQUIRE_GREATER(cache_size, 0);
        REQUIRE_GREATER(new_timeline_resolution, 0);
    }

    struct req_processeor
    {
        worker* w;

        void operator()(put_req& r)
        try
        {
            INVARIANT(w);
            REQUIRE_GREATER(r.key.size(), 0);
            w->db().put(r.key.data(), r.time, r.count);
        }
        catch(std::exception& e) 
        {
            std::cerr << "Error putting data: " << r.key << " " << r.count 
                << " " << r.count << ": " << e.what() << std::endl;
        }

        void operator()(get_req& r)
        try
        {
            INVARIANT(w);
            REQUIRE_GREATER(r.key.size(), 0);
            r.result.set_value(w->db().get(r.key, r.time));
        }
        catch(std::exception& e) 
        {
            std::cerr << "Error getting data: " << r.key 
                << " " << r.time << ": " << e.what() << std::endl;
            r.result.set_value(db::get_result{});
        }

        void operator()(diff_req& r)
        try
        {
            INVARIANT(w);
            REQUIRE_GREATER(r.key.size(), 0);
            r.result.set_value(w->db().diff(r.key, r.a, r.b, r.index_offset));
        }
        catch(std::exception& e) 
        {
            std::cerr << "Error diffing data: " << r.key
                << " (" << r.a << ", " << r.b << "): " << e.what() << std::endl;
            r.result.set_value(db::diff_result{});
        }

        void operator()(summary_req& r)
        try
        {
            INVARIANT(w);
            REQUIRE_GREATER(r.key.size(), 0);
            r.result.set_value(w->db().summary(r.key));
        }
        catch(std::exception& e) 
        {
            std::cerr << "Error summing data: " << r.key
                << ": " << e.what() << std::endl;
            r.result.set_value(db::summary_result{});
        }
    };

    void req_thread(worker* w) 
    {
        REQUIRE(w);

        req_processeor processeor{w};

        auto& q = w->queue();

        while(!w->done())
        try
        {
            req r;
            q.blockingRead(r);
            boost::apply_visitor(processeor, r);
        }
        catch (const std::exception& e)
        {
            std::cerr << "error processing request: " << e.what() << std::endl;
        }
    }

    server::server(
            const std::size_t total_workers, 
            const std::string& root, 
            const std::size_t queue_size,
            const std::size_t cache_size,
            const db::time_type new_timeline_resolution) : _root{root}, _done{false} 
    {
        REQUIRE_GREATER(total_workers, 0);
        REQUIRE_GREATER(queue_size, 0);
        REQUIRE_GREATER(cache_size, 0);
        REQUIRE_GREATER(new_timeline_resolution, 0);

        auto workers = total_workers;

        while(--workers)
        {
            auto w = std::make_unique<worker>(_root, queue_size, cache_size, new_timeline_resolution, &_done);
            auto t = std::make_unique<std::thread>(req_thread, w.get());

            _workers.emplace_back(std::move(w));
            _threads.emplace_back(std::move(t));
        }
    }

    server::~server()
    {
        stop();
    }

    void server::stop()
    {
        if(_done) return;

        _done = true;
        for(auto& t : _threads)
            t->join();
    }

    void server::put(const stde::string_view& key, db::time_type t, db::count_type c)
    {
        std::string safe_key;
        safe_key.reserve(key.size());
        db::sanatize_key(safe_key, key);

        auto n = worker_num(safe_key);

        put_req r {std::move(safe_key), t, c};
        _workers[n]->queue().write(std::move(r));
    }

    summary_future server::summary(const stde::string_view& key) const 
    {
        std::string safe_key;
        safe_key.reserve(key.size());
        db::sanatize_key(safe_key, key);

        auto n = worker_num(safe_key);
        summary_req r{std::move(safe_key)};
        summary_future f = r.result.get_future();
        _workers[n]->queue().write(std::move(r));
        return f;
    }

    get_future server::get(const stde::string_view& key, db::time_type t) const 
    {
        std::string safe_key;
        safe_key.reserve(key.size());
        db::sanatize_key(safe_key, key);

        auto n = worker_num(safe_key);

        get_req r{std::move(safe_key), t};
        get_future f = r.result.get_future();
        _workers[n]->queue().write(std::move(r));
        return f;
    }

    diff_future server::diff(const stde::string_view& key, db::time_type a, db::time_type b, const db::offset_type index_offset) const
    {
        std::string safe_key;
        safe_key.reserve(key.size());
        db::sanatize_key(safe_key, key);

        auto n = worker_num(safe_key);

        diff_req r{std::move(safe_key), a, b, index_offset};
        diff_future f = r.result.get_future();
        _workers[n]->queue().write(std::move(r));
        return f;
    }

    std::size_t server::worker_num(const stde::string_view& key) const
    {
        auto h = std::hash<stde::string_view>{}(key);
        auto n = h % _workers.size(); //exclude rank 0

        ENSURE_RANGE(n, 0, _workers.size());
        return n; 
    }
}
