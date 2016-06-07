#ifndef FLYFISH_MPI_H
#define FLYFISH_MPI_H

#include <iostream>

#include "db/db.hpp"

#include <boost/serialization/string.hpp>
#include <boost/mpi/config.hpp>
#include <boost/mpi.hpp>

namespace bm = boost::mpi;

namespace flyfish
{
    namespace mpi
    {
        struct put_req
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

        struct diff_req
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

        struct diff_res
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

        class worker  
        {
            public: 
                worker(bm::communicator& world, int requester, const std::string & root) : 
                    _world{world},
                    _db{root}, 
                    _requester{requester} {}

                void operator()();

            private:

                void put();
                void diff();

            private:
                const int _requester;
                db::timeline_db _db;
                bm::communicator& _world;
        };

        class server  
        {
            public:
                server(bm::communicator& world) : _world{world} {}

                const db::data_item& get(const std::string& key, db::time_type t) const; 
                bool put(const std::string& key, db::time_type t, db::count_type c);
                db::diff_result diff(const std::string& key, db::time_type a, db::time_type b) const;

            private:

                int worker(const std::string& key) const;

            private:
                bm::communicator& _world;
        };
    }
}
#endif
