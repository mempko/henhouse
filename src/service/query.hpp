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

namespace flyfish
{
    namespace net
    {
        const std::string SMALL_PRECISION_ERROR  = 
            "cannot go beyond second precision, increase time or reduce amount"
            "of requested values";

        class query_request_handler : public proxygen::RequestHandler {
            public:
                explicit query_request_handler(threaded::server& db, const std::size_t max_values) : 
                    RequestHandler{}, _db{db}, _max_values{max_values} {}

                void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override
                try
                {
                    if(headers->getPath() == "/diff") 
                        on_diff(*headers);
                    else if(headers->getPath() == "/values") 
                        on_values(*headers);
                    else
                    {
                        proxygen::ResponseBuilder(downstream_)
                            .status(404, "Not Found")
                            .sendWithEOM();
                    }

                }
                catch(std::exception& e)
                {
                    proxygen::ResponseBuilder(downstream_)
                        .status(500, e.what())
                        .sendWithEOM();
                }
                catch(...)
                {
                    proxygen::ResponseBuilder(downstream_)
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

                        proxygen::ResponseBuilder(downstream_)
                            .status(200, "OK")
                            .body(diff(key, a, b))
                            .sendWithEOM();
                    }
                    else
                    {
                        proxygen::ResponseBuilder(downstream_)
                            .status(422, "Missing Key")
                            .sendWithEOM();
                    }
                }

                void on_values(proxygen::HTTPMessage& headers)
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

                        auto c = headers.hasQueryParam("values") ? 
                            boost::lexical_cast<std::size_t>(headers.getQueryParam("values")) :
                            1;

                        if(headers.hasQueryParam("csv")) 
                        {
                            proxygen::ResponseBuilder(downstream_)
                                .status(200, "OK")
                                .body(values_csv(key, a, b, c))
                                .sendWithEOM();
                        }
                        else
                        {
                            proxygen::ResponseBuilder(downstream_)
                                .status(200, "OK")
                                .body(values_json(key, a, b, c))
                                .sendWithEOM();
                        }
                    }
                    else
                    {
                        proxygen::ResponseBuilder(downstream_)
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
                        flyfish::db::time_type a, 
                        flyfish::db::time_type b)
                {
                    auto r = _db.diff(key, a, b);
                    folly::dynamic o = folly::dynamic::object
                        ("sum", r.sum)
                        ("mean", r.mean)
                        ("variance", r.variance)
                        ("change", r.change)
                        ("size", r.size);
                    return folly::toJson(o);
                }

                std::string values_json(
                        const std::string& key, 
                        flyfish::db::time_type a, 
                        flyfish::db::time_type b,
                        std::size_t c)
                {
                    c = std::max<std::size_t>(1, c);
                    c = std::min<std::size_t>(c, _max_values);
                    if(a > b) std::swap(a, b);

                    auto step = (b - a) / c;
                    if(step == 0) throw std::runtime_error{SMALL_PRECISION_ERROR};
                    

                    folly::dynamic arr = folly::dynamic::array();

                    for(auto e = a + step; a < b; a+=step,e+=step)
                    {
                        auto r = _db.diff(key, a, e);
                        arr.push_back(r.sum);
                    }
                    return folly::toJson(arr);
                }

                std::string values_csv(
                        const std::string& key, 
                        flyfish::db::time_type a, 
                        flyfish::db::time_type b,
                        std::size_t c)
                {
                    c = std::max<std::size_t>(1, c);
                    c = std::min<std::size_t>(c, _max_values);
                    if(a > b) std::swap(a, b);

                    auto step = (b - a) / c;
                    if(step == 0) throw std::runtime_error{SMALL_PRECISION_ERROR};

                    std::stringstream s;
                    auto e = a + step;
                    auto r = _db.diff(key, a, e);
                    s << r.sum;
                    for(; a < b; a+=step,e+=step)
                    {
                        r = _db.diff(key, a, e);
                        s << "," << r.sum;
                    }
                    return s.str();
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
