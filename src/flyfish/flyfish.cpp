#include "service/put.hpp"
#include "service/query.hpp"

#include <iostream>
#include <chrono>
#include <sstream>
#include <vector>
#include <limits>
#include <unistd.h>

const std::size_t WORKERS = 8;
const std::size_t MAX_VALUES = 10000;

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using Protocol = proxygen::HTTPServer::Protocol;

int main(int argc, char** argv)
try
{
    const std::size_t WORKERS = sysconf(_SC_NPROCESSORS_ONLN);
    const std::string ip = "0.0.0.0";
    const std::uint16_t http_port = 7070;
    const std::uint16_t http2_port = 7071;

    flyfish::threaded::server db{WORKERS, "./tmp"};

    //setup put endpoing that mimics graphite
    wangle::ServerBootstrap<flyfish::net::put_pipeline> put_server;
    put_server.childPipeline(std::make_shared<flyfish::net::put_pipeline_factory>(db));
    put_server.bind(2003); //graphite receive port


    //setup http query interface
    std::vector<proxygen::HTTPServer::IPConfig> IPs = {
        {SocketAddress(ip, http_port), Protocol::HTTP},
        {SocketAddress(ip, http2_port), Protocol::HTTP2},
    };

    proxygen::HTTPServerOptions options;
    options.threads = WORKERS;
    options.idleTimeout = std::chrono::milliseconds(60000);
    options.shutdownOn = {SIGINT, SIGTERM};
    options.enableContentCompression = true;
    options.handlerFactories = proxygen::RequestHandlerChain()
        .addThen<flyfish::net::query_handler_factory>(db, MAX_VALUES)
        .build();

    proxygen::HTTPServer query_server(std::move(options));
    query_server.bind(IPs);


    //start services
    std::thread put_thread
    {
        [&]() { put_server.waitForStop(); }
    };

    std::thread query_thread
    {
        [&] () { query_server.start(); }
    };

    //wait forever
    put_thread.join();
    query_thread.join();

    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "Error! " << e.what() << std::endl;
    return 1;
}
