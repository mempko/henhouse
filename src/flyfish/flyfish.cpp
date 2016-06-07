#include <iostream>

#include "util/dbc.hpp"
#include "db/db.hpp"

#include <chrono>

#include <boost/serialization/string.hpp>
#include <boost/mpi/config.hpp>
#include <boost/mpi.hpp>

namespace bm = boost::mpi;

namespace flyfish
{
    struct mpi_put_req
    {
        std::string key;
        db::time_type time;
        db::count_type count;

        template<class A>
            void serialize(A& ar, const unsigned int version)
            {
                ar & key;
                ar & time;
                ar & count;
            }
    };

    struct mpi_diff_req
    {
        std::string key;
        db::time_type a;
        db::time_type b;

        template<class A>
            void serialize(A& ar, const unsigned int version)
            {
                ar & key;
                ar & a;
                ar & b;
            }
    };

    struct mpi_diff_res
    {
        db::count_type sum;
        db::count_type mean;
        db::count_type variance;
        db::change_type change;
        db::count_type size;

        template<class A>
            void serialize(A& ar, const unsigned int version)
            {
                ar & sum;
                ar & mean;
                ar & variance;
                ar & change;
                ar & size;
            }
    };

    const int PUT_REQ = 1;
    const int DIFF_REQ = 2;
    const int DIFF_RES = 3;

    class mpi_worker  
    {
        public: 
            mpi_worker(bm::communicator& world, int requester, const std::string & root) : 
                _world{world},
                _db{root}, 
                _requester{requester} {}

            void operator()() {
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

        private:

            void put() 
            {
                mpi_put_req req;
                _world.recv(_requester, PUT_REQ, req);
                //std::cout << "(" << _world.rank() << ")" << "put: " << r.key << " " << r.time << " " << r.count << std::endl;
                _db.put(req.key, req.time, req.count);
            }

            void diff()
            {
                mpi_diff_req req;
                _world.recv(_requester, DIFF_REQ, req);

                auto diff = _db.diff(req.key, req.a, req.b);

                mpi_diff_res res 
                {
                    diff.sum,
                    diff.mean,
                    diff.variance,
                    diff.change,
                    diff.size
                };
                _world.send(_requester, DIFF_RES, res);
            }

        private:
            const int _requester;
            db::timeline_db _db;
            bm::communicator& _world;
    };

    class mpi_db  
    {
        public:
            mpi_db(bm::communicator& world) : _world{world} {}

            const db::data_item& get(const std::string& key, db::time_type t) const 
            {
                return db::data_item{};
            }

            bool put(const std::string& key, db::time_type t, db::count_type c)
            {
                auto w = worker(key);
                mpi_put_req r{key, t, c};
                _world.send(w, PUT_REQ, r);
            }

            db::diff_result diff(const std::string& key, db::time_type a, db::time_type b) const
            {
                auto w = worker(key);
                mpi_diff_req req{key, a, b};
                _world.send(w, DIFF_REQ, req);

                mpi_diff_res res;
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
        private:

            int worker(const std::string& key) const
            {
                auto h = std::hash<std::string>{}(key);
                return (h % (_world.size() - 1)) + 1; //exclude rank 0
            }

        private:
            bm::communicator& _world;

    };
}

int main(int argc, char** argv)
try
{
    bm::environment env;
    bm::communicator world;

    if(world.rank() == 0) 
    {
        flyfish::mpi_db db{world};

        size_t tm = 0;

        //uncomment for a billion data points inserted.
        //Requires 2.5GB of space
        //const size_t TOTAL = 1000000000;
        //

        //100 million
        //Requires around 200MB of hardrive space.
        const int TOTAL = 1000000;
        const size_t TIME_INC = 10;
        const int CHANGE = 2;

        std::cout << std::fixed;

        std::string keys[] = {"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"};

        //insert data if the data is empty
        //otherwise skip
        auto start = std::chrono::high_resolution_clock::now();
        for(int a = 0; a < TOTAL; a++)
        {
            tm += TIME_INC;
            auto v = a % 3 ? 1 : 2;
            db.put(keys[a%8], tm, v);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
        auto sec = duration / 1000000000.0;
        std::cout <<  sec << " seconds" << std::endl;
        std::cout << (TOTAL / sec) << " puts per second" << std::endl;
        std::cout << std::endl;

        for(auto k : keys)
        {
            std::cout << k << std::endl;
            auto start = std::chrono::high_resolution_clock::now();
            auto d = db.diff(k, tm/4, tm/2);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count();
            auto sec = duration / 1000000000.0;

            std::cout << "\treq time: " << sec << "(s)" << std::endl;
            std::cout << "\tsum: " << d.sum << std::endl;
            std::cout << "\tavg: " << d.mean << std::endl;
            std::cout << "\tvariance: " << d.variance << std::endl;
            std::cout << "\tchange: " << d.change << std::endl;
            std::cout << "\tsize: " << d.size << std::endl;
        }
    }
    else
    {
        flyfish::mpi_worker worker{world, 0, "./tmp"};
        worker();
    }

    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "Error! " << e.what() << std::endl;
    return 1;
}
