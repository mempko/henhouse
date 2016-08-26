#ifndef FLYFISH_PUT_SERV_H
#define FLYFISH_PUT_SERV_H

#include "service/threaded.hpp"

#include <sstream>

#include <wangle/bootstrap/ServerBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/codec/LineBasedFrameDecoder.h>
#include <wangle/codec/StringCodec.h>

namespace henhouse
{
    namespace net
    {
        typedef wangle::Pipeline<folly::IOBufQueue&, std::string> put_pipeline;

        class put_handler : public wangle::HandlerAdapter<std::string> 
        {
            public:
                put_handler(threaded::server& db) : 
                    wangle::HandlerAdapter<std::string>{}, 
                    _db{db} 
                {}

            public:
                virtual void read(Context* ctx, std::string msg) override 
                {
                    std::stringstream m{msg};

                    std::string key;
                    db::time_type t; 
                    std::int64_t c;
                    m >> key >> c >> t;

                    if(key.empty()) return;

                    _db.put(key, t, c);
                }

                virtual void readException(Context* ctx, folly::exception_wrapper e) override
                {
                    std::cerr << "put read error: " << exceptionStr(e) << std::endl;
                }

                virtual void readEOF(Context* ctx) override { close(ctx); }

            private:
                threaded::server& _db;
        };

        class put_pipeline_factory : public wangle::PipelineFactory<put_pipeline> 
        {
            public:
                put_pipeline_factory(threaded::server& db) : wangle::PipelineFactory<put_pipeline>{},
                    _db{db} {}

            public:
                put_pipeline::Ptr newPipeline(std::shared_ptr<folly::AsyncTransportWrapper> sock) 
                {
                    auto pipeline = put_pipeline::create();
                    pipeline->addBack(wangle::AsyncSocketHandler{sock});
                    pipeline->addBack(wangle::LineBasedFrameDecoder{8192});
                    pipeline->addBack(wangle::StringCodec{});
                    pipeline->addBack(put_handler{_db});
                    pipeline->finalize();
                    return pipeline;
                }

            private:
                threaded::server& _db;
        };
    }
}
#endif
