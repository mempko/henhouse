#ifndef FLYFISH_QUERY_SERV_H
#define FLYFISH_QUERY_SERV_H

#include "service/threaded.hpp"

#include <sstream>
#include <limits>

//for http endpoint
//
#include <folly/Memory.h>
#include <folly/Portability.h>
#include <folly/json.h>
#include <folly/io/async/EventBaseManager.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <boost/lexical_cast.hpp>

namespace hdb = henhouse::db;

namespace henhouse
{
    namespace net
    {

        const std::size_t MAX_QUERY_SIZE = 10000;
        const std::string QUERY_TOO_LARGE = 
            "query size is too large. Max query must be under 10000 values";

        const std::string SMALL_PRECISION_STEP_ERROR  = 
            "cannot go beyond second precision, for step";

        const std::string SMALL_PRECISION_SIZE_ERROR  = 
            "cannot go beyond second precision, for segment size";

        const db::offset_type NO_OFFSET = 0;

        class query_request_handler : public proxygen::RequestHandler {
            public:
                explicit query_request_handler(threaded::server& db, const std::size_t max_values) : 
                    RequestHandler{}, _db{db}, _max_values{max_values} {}

                void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override
                try
                {
                    if(headers->getPath() == "/summary") 
                        on_diff(*headers);
                    else if(headers->getPath() == "/values") 
                        on_values(*headers);
                    else
                    {
                        proxygen::ResponseBuilder{downstream_}
                            .status(404, "Not Found")
                            .sendWithEOM();
                    }

                }
                catch(std::exception& e)
                {
                    proxygen::ResponseBuilder{downstream_}
                        .status(500, e.what())
                        .sendWithEOM();
                }
                catch(...)
                {
                    proxygen::ResponseBuilder{downstream_}
                        .status(500, "Unknown Error")
                        .sendWithEOM();
                }

                void on_diff(proxygen::HTTPMessage& headers) 
                {
                    if(headers.hasQueryParam("key"))
                    {
                        auto key = headers.getQueryParam("key");
                        auto a = headers.hasQueryParam("a") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("a")) :
                            0;

                        auto b = headers.hasQueryParam("b") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("b")) :
                            std::numeric_limits<uint64_t>::max();

                        proxygen::ResponseBuilder{downstream_}
                            .status(200, "OK")
                            .body(diff(key, a, b))
                            .sendWithEOM();
                    }
                    else
                    {
                        proxygen::ResponseBuilder{downstream_}
                            .status(422, "Missing Key")
                            .sendWithEOM();
                    }
                }

                void on_values(proxygen::HTTPMessage& headers)
                {
                    if(headers.hasQueryParam("key"))
                    {
                        auto rb = proxygen::ResponseBuilder{downstream_};

                        auto key = headers.getQueryParam("key");
                        auto a = headers.hasQueryParam("a") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("a")) :
                            0;

                        auto b = headers.hasQueryParam("b") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("b")) :
                            std::numeric_limits<std::uint64_t>::max();

                        auto step = headers.hasQueryParam("step") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("step")) :
                            1;

                        auto segment_size = headers.hasQueryParam("size") ? 
                            boost::lexical_cast<std::uint64_t>(headers.getQueryParam("size")) :
                            step;

                        auto extract_func = headers.hasQueryParam("sum") ? 
                            [](const hdb::diff_result& r) { return r.sum;}:
                            (headers.hasQueryParam("variance") ?  
                             [](const hdb::diff_result& r) { return r.variance;} : 
                             [](const hdb::diff_result& r) { return r.mean;});

                        auto render_func = headers.hasQueryParam("xy") ? 
                            [](proxygen::ResponseBuilder& rb, hdb::time_type t, hdb::count_type v) -> void 
                            {
                                rb.body("{\"x\":");
                                rb.body(boost::lexical_cast<std::string>(t));
                                rb.body(",\"y\":");
                                rb.body(boost::lexical_cast<std::string>(v));
                                rb.body("}");
                            } :
                            [](proxygen::ResponseBuilder& rb, hdb::time_type t, hdb::count_type v) -> void 
                            {
                                rb.body(boost::lexical_cast<std::string>(v));
                            };


                        if(headers.hasQueryParam("csv")) 
                        {
                            values(rb, key, a, b, step, segment_size, render_func, extract_func);
                        }
                        else
                        {
                            rb.body("[");
                            values(rb, key, a, b, step, segment_size, render_func, extract_func);
                            rb.body("]");
                        }

                        rb.status(200, "OK");
                        rb.sendWithEOM();
                    }
                    else
                    {
                        proxygen::ResponseBuilder{downstream_}
                            .status(422, "Missing Key")
                            .sendWithEOM();
                    }
                }

                void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override 
                { 
                    if (_body) _body->prependChain(std::move(body));
                    else _body = std::move(body);
                }

                void onEOM() noexcept override {}
                void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override {}

                void requestComplete() noexcept override 
                { 
                    delete this;
                }

                void onError(proxygen::ProxygenError err) noexcept override 
                { 
                    delete this;
                }

            private:

                std::string diff(
                        const std::string& key, 
                        hdb::time_type a, 
                        hdb::time_type b)
                {
                    auto r = _db.diff(key, a, b, NO_OFFSET);
                    folly::dynamic o = folly::dynamic::object
                        ("sum", r.sum)
                        ("mean", r.mean)
                        ("variance", r.variance)
                        ("change", r.change)
                        ("size", r.size);
                    return folly::toJson(o);
                }

                template<class render_func, class extract_func>
                    void values(
                            proxygen::ResponseBuilder& rb,
                            const std::string& key, 
                            hdb::time_type a, 
                            hdb::time_type b,
                            hdb::time_type step,
                            hdb::time_type segment_size,
                            render_func render_value,
                            extract_func extract_value)
                    {
                        if(a > b) std::swap(a, b);
                        if(step < 1) throw std::runtime_error{SMALL_PRECISION_STEP_ERROR};
                        if(segment_size < 1) throw std::runtime_error{SMALL_PRECISION_SIZE_ERROR};

                        //output all but last
                        auto s = a - segment_size;
                        const auto e = b - step;

                        const auto query_size = (e - a) / step;
                        if(query_size > MAX_QUERY_SIZE) throw std::runtime_error{QUERY_TOO_LARGE};

                        auto prev_index_offset = 0;

                        for(; a <= e; s+=step, a+=step) 
                        {
                            const auto r = _db.diff(key, s, a, prev_index_offset);
                            prev_index_offset = r.index_offset;
                            render_value(rb, a, extract_value(r));
                            rb.body(",");
                        }

                        //output last
                        if(a <= b)
                        {
                            const auto r = _db.diff(key, s, a, prev_index_offset);
                            render_value(rb, a, extract_value(r));
                        }
                    }

            private:
                threaded::server& _db;
                const std::size_t _max_values;
                std::unique_ptr<folly::IOBuf> _body;
        };

        class query_handler_factory : public proxygen::RequestHandlerFactory 
        {
            public:
                query_handler_factory(threaded::server& db, const std::size_t max_values) : 
                    proxygen::RequestHandlerFactory{}, _db{db}, _max_values{max_values} {}

            public:

                proxygen::RequestHandler* onRequest(
                        proxygen::RequestHandler* r, 
                        proxygen::HTTPMessage* m) noexcept override 
                {
                    return new query_request_handler(_db, _max_values);
                }

                void onServerStart(folly::EventBase* evb) noexcept { } 
                void onServerStop() noexcept { }

            private:
                threaded::server& _db;
                const std::size_t _max_values;
        };
    }
}
#endif
