#include "service/mpi.hpp"

#include <boost/serialization/string.hpp>
#include <boost/mpi/config.hpp>
#include <boost/mpi.hpp>

namespace flyfish
{
    namespace mpi
    {
        const int PUT_REQ = 1;
        const int DIFF_REQ = 2;
        const int DIFF_RES = 3;

        void worker::operator()() {
            while(true) 
            {
                auto probe = _world.probe(_requester);
                switch(probe.tag())
                {
                    case PUT_REQ: put(); break;
                    case DIFF_REQ: diff(); break;
                    default: std::cerr << "unexpected message: " << probe.tag() << std::endl;

                }
            }
        }

        void worker::put() 
        {
            put_req req;
            _world.recv(_requester, PUT_REQ, req);
            //std::cout << "(" << _world.rank() << ")" << "put: " << r.key << " " << r.time << " " << r.count << std::endl;
            _db.put(req.key, req.time, req.count);
        }

        void worker::diff()
        {
            diff_req req;
            _world.recv(_requester, DIFF_REQ, req);

            auto diff = _db.diff(req.key, req.a, req.b);

            diff_res res 
            {
                diff.sum,
                    diff.mean,
                    diff.variance,
                    diff.change,
                    diff.size
            };
            _world.send(_requester, DIFF_RES, res);
        }

        const db::data_item& server::get(const std::string& key, db::time_type t) const 
        {
            return db::data_item{};
        }

        bool server::put(const std::string& key, db::time_type t, db::count_type c)
        {
            auto w = worker(key);
            put_req r{key, t, c};
            _world.send(w, PUT_REQ, r);
        }

        db::diff_result server::diff(const std::string& key, db::time_type a, db::time_type b) const
        {
            auto w = worker(key);
            diff_req req{key, a, b};
            _world.send(w, DIFF_REQ, req);

            diff_res res;
            _world.recv(w, DIFF_RES,res);

            return db::diff_result
            {
                res.sum,
                    res.mean,
                    res.variance,
                    res.change,
                    res.size
            };
        }

        int server::worker(const std::string& key) const
        {
            auto h = std::hash<std::string>{}(key);
            return (h % (_world.size() - 1)) + 1; //exclude rank 0
        }

    }
}
