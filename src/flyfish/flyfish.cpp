#include "service/mpi.hpp"

#include <iostream>
#include <chrono>

int main(int argc, char** argv)
try
{
    bm::environment env;
    bm::communicator world;

    if(world.rank() == 0) 
    {
        flyfish::mpi::server db{world};

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
        flyfish::mpi::worker worker{world, 0, "./tmp"};
        worker();
    }

    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "Error! " << e.what() << std::endl;
    return 1;
}
