#ifndef FLYFISH_QUERY_SERV_H
#define FLYFISH_QUERY_SERV_H

#include "service/threaded.hpp"

#include <experimental/string_view>
#include <sstream>
#include <vector>
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
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/split.hpp>

#include <ctime>


namespace hdb = henhouse::db;
namespace ht = henhouse::threaded;
namespace ba = boost::algorithm;
namespace stde = std::experimental;

namespace henhouse::net
{
    namespace
    {
        const int PREV_INDEX_OFFSET = 0; //index offset to start diff query

        const std::size_t MAX_QUERY_SIZE = 10000;
        const std::string QUERY_TOO_LARGE = 
            "query size is too large. Max query must be under 10000 values";

        const std::string SMALL_PRECISION_STEP_ERROR  = 
            "cannot go beyond second precision, for step";

        const std::string SMALL_PRECISION_SIZE_ERROR  = 
            "cannot go beyond second precision, for segment size";

        const db::offset_type NO_OFFSET = 0;

        template<class key_func>
            void for_each_key(const stde::string_view &keys, key_func kf)
            {
                auto first_key = ba::make_split_iterator(keys, ba::first_finder(",", ba::is_equal()));
                decltype(first_key) end_key_split{};

                for(auto it = first_key; it != end_key_split; it++)
                {
                    stde::string_view::size_type size = std::distance(it->begin(), it->end());
                    if(size == 0) continue;

                    stde::string_view key{it->begin(), size};
                    kf(key);
                }
            }

        struct bad_request : public std::runtime_error 
        {
            bad_request(const std::string& error) : std::runtime_error{error}{}
        };

    }

    struct values_result
    {
        stde::string_view key;
        std::vector<ht::diff_future> results;
    };

    struct summary_result
    {
        stde::string_view key;
        ht::summary_future result;
    };

    struct diff_result
    {
        stde::string_view key;
        ht::diff_future result;
    };

    using key_values_result = std::vector<values_result>;
    using summary_results = std::vector<summary_result>;
    using diff_results = std::vector<diff_result>;

    class query_request_handler : public proxygen::RequestHandler {
        public:
            explicit query_request_handler(threaded::server& db, const std::size_t max_values) : 
                RequestHandler{}, _db{db}, _max_values{max_values} {}

            void onRequest(std::unique_ptr<proxygen::HTTPMessage> req) noexcept override
            {
                _req = std::move(req);
            }

            void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override
            {
                if (_body) _body->prependChain(std::move(body));
                else _body = std::move(body);
            }

            void onEOM() noexcept override
            try
            {
                REQUIRE(_req);

                if(_req->getPath() == "/summary")
                    on_summary(*_req);
                else if(_req->getPath() == "/diff")
                    on_diff(*_req);
                else if(_req->getPath() == "/values")
                    on_values(*_req);
                else
                {
                    proxygen::ResponseBuilder{downstream_}
                    .status(404, "Not Found")
                        .sendWithEOM();
                }

            }
            catch(bad_request& e)
            {
                proxygen::ResponseBuilder{downstream_}
                .status(400, e.what())
                    .sendWithEOM();
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

        private:

            void on_summary(proxygen::HTTPMessage& req) 
            {
                auto rb = proxygen::ResponseBuilder{downstream_};

                if(req.hasQueryParam("keys"))
                {
                    auto keys = req.getQueryParam("keys");

                    if(keys.empty()) 
                    {
                        rb.status(400, "The Keys parameter must be a comma separated list").sendWithEOM();
                        return;
                    }

                    folly::dynamic out = folly::dynamic::array();

                    summary_results results;

                    for_each_key(keys, [&](const stde::string_view & key)
                    {
                        summary_result r{key, _db.summary(key)};
                        results.emplace_back(std::move(r));
                    });

                    for(auto& r: results)
                    {
                        folly::dynamic s = folly::dynamic::object
                        ("key", r.key.to_string())
                        ("stats", summary(r.result.get()));
                        out.push_back(std::move(s));
                    }

                    rb.body(folly::toJson(out))
                        .status(200, "OK")
                        .sendWithEOM();
                }
                else
                {
                    rb.status(400, "Missing keys parameter").sendWithEOM();
                }
            }

            void on_diff(proxygen::HTTPMessage& req) 
            {
                using boost::lexical_cast;
                auto rb = proxygen::ResponseBuilder{downstream_};

                if(req.hasQueryParam("keys"))
                {
                    auto keys = req.getQueryParam("keys");

                    if(keys.empty()) 
                    {
                        rb.status(400, "The Keys parameter must be a comma separated list").sendWithEOM();
                        return;
                    }

                    auto a = req.hasQueryParam("a") ? 
                        lexical_cast<std::uint64_t>(req.getQueryParam("a")) :
                        0;

                    auto b = req.hasQueryParam("b") ? 
                        lexical_cast<std::uint64_t>(req.getQueryParam("b")) : 
                        std::time(0);

                    if(a > b) std::swap(a, b);

                    diff_results results;
                    for_each_key(keys, [&](const stde::string_view & key)
                    {
                        diff_result r{key, _db.diff(key, a, b, NO_OFFSET)};
                        results.emplace_back(std::move(r));
                    });

                    folly::dynamic out = folly::dynamic::array();

                    for(auto& r: results)
                    {
                        folly::dynamic s = folly::dynamic::object
                            ("key", r.key.to_string())
                            ("stats", diff(r.result.get()));
                        out.push_back(std::move(s));
                    }

                    rb.body(folly::toJson(out))
                        .status(200, "OK")
                        .sendWithEOM();
                }
                else
                {
                    rb.status(400, "Missing keys parameter").sendWithEOM();
                }
            }

            using extract_func_t = std::function<std::string(const hdb::diff_result& r)>;
            extract_func_t get_extract_func(proxygen::HTTPMessage& req)
            {
                using boost::lexical_cast;
                extract_func_t f = [](const hdb::diff_result& r) { return lexical_cast<std::string>(r.sum);};
                if(req.hasQueryParam("mean")) 
                    f = [](const hdb::diff_result& r) { return lexical_cast<std::string>(r.mean);};
                else if(req.hasQueryParam("var"))
                    f = [](const hdb::diff_result& r) { return lexical_cast<std::string>(r.variance);}; 
                else if(req.hasQueryParam("agg"))
                    f = [](const hdb::diff_result& r) { return lexical_cast<std::string>(r.right.integral);};

                return f;
            }

            void on_values(proxygen::HTTPMessage& req)
            {
                using boost::lexical_cast;
                auto rb = proxygen::ResponseBuilder{downstream_};

                if(req.hasQueryParam("keys"))
                {

                    auto keys = req.getQueryParam("keys");

                    if(keys.empty()) 
                    {
                        rb.status(400, "The Keys parameter must be a comma separated list").sendWithEOM();
                        return;
                    }

                    auto extract_func = get_extract_func(req);

                    bool is_csv = req.hasQueryParam("csv");

                    auto render_func = !is_csv && req.hasQueryParam("xy") ? 
                        [](proxygen::ResponseBuilder& rb, hdb::time_type t, const std::string& v) -> void 
                        {
                            rb.body("{\"x\":");
                            rb.body(lexical_cast<std::string>(t));
                            rb.body(",\"y\":");
                            rb.body(v);
                            rb.body("}");
                        } :
                    [](proxygen::ResponseBuilder& rb, hdb::time_type t, const std::string& v) -> void 
                    {
                        rb.body(v);
                    };

                    rb.status(200, "OK");

                    //first query the values asynchronously
                    key_values_result results;

                    if(_body)
                    {
                        const auto body = _body->moveToFbString();
                        const auto payload = folly::parseJson(body);

                        if(!payload.isArray() || payload.size() < 2)
                        {
                            rb.status(400, "Payload must be an array of numbers of at least two numbers").sendWithEOM();
                            return;
                        }

                        for_each_key(keys, [&](const stde::string_view& key) 
                        {
                            results.emplace_back(query_values(key, payload));
                        });
                    }
                    else
                    {
                        auto a = req.hasQueryParam("a") ?
                            lexical_cast<std::uint64_t>(req.getQueryParam("a")) :
                            0;

                        auto b = req.hasQueryParam("b") ?
                            lexical_cast<std::uint64_t>(req.getQueryParam("b")) :
                            std::time(0);

                        if(a > b) std::swap(a, b);

                        auto step = req.hasQueryParam("step") ?
                            lexical_cast<std::uint64_t>(req.getQueryParam("step")) :
                            1;

                        auto segment_size = req.hasQueryParam("size") ?
                            lexical_cast<std::uint64_t>(req.getQueryParam("size")) :
                            step;

                        for_each_key(keys, [&](const stde::string_view& key)
                        {
                            results.emplace_back(query_values(key, a, b, step, segment_size));
                        });
                    }

                    //render values as results come in
                    render_values(results, rb, keys, render_func, extract_func, is_csv);
                    rb.sendWithEOM();
                }
                else
                {
                    rb.status(400, "Missing keys parameter").sendWithEOM();
                }
            }

            void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override {}

            void requestComplete() noexcept override 
            { 
                delete this;
            }

            void onError(proxygen::ProxygenError err) noexcept override 
            { 
                delete this;
            }

            folly::dynamic diff(const db::diff_result& r)
            {

                folly::dynamic o = folly::dynamic::object
                    ("sum", r.sum)
                    ("mean", r.mean)
                    ("variance", r.variance)
                    ("points", r.size)
                    ("resolution", r.resolution)
                    ("left", 
                     folly::dynamic::object
                     ("val", r.left.value)
                     ("agg", r.left.integral))
                    ("right", 
                     folly::dynamic::object
                     ("val", r.right.value)
                     ("agg", r.right.integral));
                return o;
            }

            folly::dynamic summary(const db::summary_result& r)
            {
                folly::dynamic o = folly::dynamic::object
                    ("from", r.from)
                    ("to", r.to)
                    ("resolution", r.resolution)
                    ("sum", r.sum)
                    ("mean", r.mean)
                    ("variance", r.variance)
                    ("points", r.size);
                return o;
            }

            template<typename render_func, typename extract_func>
                void render_values(
                        key_values_result& results,
                        proxygen::ResponseBuilder& rb,
                        const std::string& keys, 
                        render_func render_value,
                        extract_func extract_value,
                        bool is_csv)
                {
                    //render the result depending on csv vs json
                    if(is_csv) 
                    {
                        for(auto& r: results)
                        {
                            rb.body(r.key.to_string());
                            rb.body(",");
                            render_key_values(rb, r, render_value, extract_value);
                            rb.body("\n");
                        };
                    }
                    else
                    {
                        rb.body("{");
                        int c = 0;
                        for(auto& r: results)
                        {
                            if(c != 0) rb.body(",");
                            c++;

                            rb.body("\"");
                            rb.body(r.key.to_string());
                            rb.body("\":[");
                            render_key_values(rb, r, render_value, extract_value);
                            rb.body("]");
                        };
                        rb.body("}");
                    }

                }

                //query even buckets based on step and segment size from a to b
                values_result query_values(
                        const stde::string_view& key, 
                        hdb::time_type a, 
                        hdb::time_type b,
                        hdb::time_type step,
                        hdb::time_type segment_size)
                {
                    if(step < 1) throw bad_request( SMALL_PRECISION_STEP_ERROR );
                    if(segment_size < 1) throw bad_request( SMALL_PRECISION_SIZE_ERROR );

                    a = std::max(segment_size, a);
                    b = std::max(step, b);

                    //output all but last
                    auto s = a - segment_size;
                    const auto e = b - step;

                    const auto query_size = (b - a) / step;
                    if(query_size > MAX_QUERY_SIZE) throw bad_request( QUERY_TOO_LARGE );

                    //create place to put future results 
                    values_result r;
                    r.key = key;
                    r.results.reserve(query_size);

                    //query db async storing the futures
                    for(; a <= e; s+=step, a+=step)
                        r.results.emplace_back(_db.diff(key, s, a, PREV_INDEX_OFFSET));

                    //query last
                    r.results.emplace_back(_db.diff(key, s, b, PREV_INDEX_OFFSET));

                    return r;
                }

                //query values based on discrete units specified in the payload
                values_result query_values(
                        const stde::string_view& key,
                        const folly::dynamic& payload)
                try
                {
                    REQUIRE(payload.isArray());
                    REQUIRE_GREATER(payload.size(), 1);

                    //create place to put future results
                    values_result r;
                    r.key = key;
                    r.results.reserve(payload.size() - 1);

                    //query db async storing the futures

                    auto s =  payload[0].getInt();
                    for(int i = 1; i < payload.size(); i++)
                    {
                        const auto e = payload[i].getInt();
                        r.results.emplace_back(_db.diff(key, s, e, PREV_INDEX_OFFSET));
                        s = e;
                    }

                    return r;
                }
                catch(...)
                {
                    throw bad_request("Expected the payload to be an array of integers");
                }

            template<typename render_func, typename extract_func>
                void render_key_values(
                        proxygen::ResponseBuilder& rb,
                        values_result& r,
                        render_func render_value,
                        extract_func extract_value)
                {
                    REQUIRE_FALSE(r.key.empty());
                    REQUIRE_FALSE(r.results.empty());

                    //process results and output to client
                    int c = 0;
                    for(size_t i = 0; i < r.results.size() - 1; i++, c++) 
                    {
                        const auto v = r.results[i].get(); 

                        render_value(rb, v.a, extract_value(v));
                        rb.body(",");
                        //50 here should roughly be 1k for xy request, though likely larger.
                        //TODO don't use RequestBuilder, do it yourself.
                        if(c % 50 == 0) rb.send();
                    }

                    //output last
                    const auto lv = r.results[r.results.size() -1].get();
                    render_value(rb, lv.a, extract_value(lv));
                }

        private:
            threaded::server& _db;
            const std::size_t _max_values;
            std::unique_ptr<folly::IOBuf> _body;
            std::unique_ptr<proxygen::HTTPMessage> _req;
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
#endif
