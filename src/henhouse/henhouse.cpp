#include "service/put.hpp"
#include "service/query.hpp"

#include <iostream>
#include <chrono>
#include <sstream>
#include <vector>
#include <limits>

#include <boost/program_options.hpp>

using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using Protocol = proxygen::HTTPServer::Protocol;

namespace po = boost::program_options;
namespace bf = boost::filesystem;

po::options_description create_descriptions()
{
    po::options_description d{"Options"};
    const auto workers = std::thread::hardware_concurrency();

    d.add_options()
        ("help,h", "prints help")
        ("ip", po::value<std::string>()->default_value("0.0.0.0"), "IP to bind")
        ("http_port", po::value<std::uint16_t>()->default_value(9090), "Http port")
        ("http2_port", po::value<std::uint16_t>()->default_value(9091), "Http 2.0 port")
        ("put_port", po::value<std::uint16_t>()->default_value(2003), "Data input port")
        ("data,d", po::value<std::string>()->default_value("/tmp"), "Data directory")
        ("query_workers", po::value<std::size_t>()->default_value(workers), "Query threads")
        ("db_workers", po::value<std::size_t>()->default_value(workers), "DB workers")
        ("queue_size", po::value<std::size_t>()->default_value(10000), "Input queue size")
        ("cache_size", po::value<std::size_t>()->default_value(40), 
          "Size of timeline db reference cache per worker. "
          "make this too big an you can run out of file descriptors.")
        ("resolution", po::value<henhouse::db::time_type>()->default_value(60), 
         "Minimum resolution in seconds of a timeline.")
        ("max_response_values", po::value<std::size_t>()->default_value(10000), 
         "Maximum points returned in a values response.");

    return d;
}

po::variables_map parse_options(int argc, char* argv[], po::options_description& desc)
{
    po::variables_map v;
    po::store(po::parse_command_line(argc, argv, desc), v);
    po::notify(v);

    return v;
}

int main(int argc, char** argv)
try
{
    auto description = create_descriptions();
    auto opt = parse_options(argc, argv, description);

    if(opt.count("help"))
    {
        std::cout << description << std::endl;
        return 0;
    }

    const auto ip = opt["ip"].as<std::string>();
    const auto http_port = opt["http_port"].as<std::uint16_t>();
    const auto http2_port = opt["http2_port"].as<std::uint16_t>();
    const auto put_port = opt["put_port"].as<std::uint16_t>();
    const auto query_workers = opt["query_workers"].as<std::size_t>();
    const auto db_workers = opt["db_workers"].as<std::size_t>();
    const auto data_dir = opt["data"].as<std::string>();
    const auto queue_size = opt["queue_size"].as<std::size_t>();
    const auto cache_size = opt["cache_size"].as<std::size_t>();
    const auto new_timeline_resolution = opt["resolution"].as<henhouse::db::time_type>();
    const auto max_values = opt["max_response_values"].as<std::size_t>();

    bf::create_directories(data_dir);
    henhouse::threaded::server db{db_workers, data_dir, queue_size, cache_size, new_timeline_resolution};

    std::cerr << "Started DB" << std::endl;
    std::cerr << "\tworkers: " << db_workers << std::endl;
    std::cerr << "\tqueue size: " << queue_size << std::endl;
    std::cerr << "\tcache size: " << cache_size << std::endl;
    std::cerr << "\ttimeline resolution: " << new_timeline_resolution << std::endl;

    //setup put endpoing that mimics graphite
    wangle::ServerBootstrap<henhouse::net::put_pipeline> put_server;
    put_server.childPipeline(std::make_shared<henhouse::net::put_pipeline_factory>(db));
    put_server.bind(put_port); //graphite receive port

    std::cerr << "Started Input Server" << std::endl;
    std::cerr << "\tport: " << put_port << std::endl;

    //setup http query interface
    std::vector<proxygen::HTTPServer::IPConfig> IPs = {
        {SocketAddress(ip, http_port), Protocol::HTTP},
        {SocketAddress(ip, http2_port), Protocol::HTTP2},
    };

    proxygen::HTTPServerOptions options;
    options.threads = query_workers;
    options.idleTimeout = std::chrono::milliseconds(60000);
    options.shutdownOn = {SIGINT, SIGTERM};
    options.enableContentCompression = true;
    options.handlerFactories = proxygen::RequestHandlerChain()
        .addThen<henhouse::net::query_handler_factory>(db, max_values)
        .build();

    proxygen::HTTPServer query_server{std::move(options)};
    query_server.bind(IPs);

    std::cerr << "Started Query Server" << std::endl;
    std::cerr << "\thttp port: " << http_port << std::endl;
    std::cerr << "\thttp2 port: " << http2_port << std::endl;
    std::cerr << "\tworkers: " << query_workers << std::endl;
    std::cerr << "\tcompression: " << true << std::endl;
    std::cerr << "\tmax values: " << max_values << std::endl;

    //start services
    std::thread put_thread
    {
        [&]() { put_server.waitForStop(); }
    };

    std::thread query_thread
    {
        [&] () { query_server.start(); }
    };

    std::cerr << "Started Input and Query Threads, waiting forever..." << std::endl;

    //wait forever
    put_thread.join();
    query_thread.join();

    return 0;
}
catch(std::exception& e) 
{
    std::cerr << "error, exiting: " << e.what() << std::endl;
    return 1;
}
