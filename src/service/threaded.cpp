#include "service/threaded.hpp"

namespace henhouse
{
    namespace threaded
    {
        const std::size_t QUEUE_SIZE = 1000;

        worker::worker(const std::string & root, std::size_t queue_size) : 
            _db{root}, _queue{queue_size} {} 

        void put_thread(worker_ptr w) 
        {
            REQUIRE(w);

            auto& q = w->queue();
            auto& db = w->db();

            while(true)
            {
                put_req r;
                q.blockingRead(r);
                db.put(r.key, r.time, r.count);
            }

        }

        server::server(std::size_t workers, const std::string& root) : _root{root} 
        {
            REQUIRE_GREATER(workers, 0);

            while(--workers)
            {

                auto w = std::make_shared<worker>(_root, QUEUE_SIZE);
                _workers.emplace_back(w);

                auto t = std::make_shared<std::thread>(put_thread, w);
                _threads.emplace_back(t);
            }

        }

        db::get_result server::get(const std::string& key, db::time_type t) const 
        {
            auto n = worker_num(key);
            auto& w = _workers[worker_num(key)];

            CHECK(w);
            return w->db().get(key, t);
        }

        bool server::put(const std::string& key, db::time_type t, db::count_type c)
        {
            auto n = worker_num(key);

            put_req r{key, t, c};
            _workers[n]->queue().write(std::move(r));
        }

        db::diff_result server::diff(const std::string& key, db::time_type a, db::time_type b) const
        {
            auto& w = _workers[worker_num(key)];

            CHECK(w);
            return w->db().diff(key, a, b);
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
