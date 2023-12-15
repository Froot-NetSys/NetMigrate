#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <sw/redis++/redis++.h>
#include <hiredis/hiredis.h>

using namespace sw::redis;

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/keyvaluestore.grpc.pb.h"
#else
#include "keyvaluestore.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using keyvaluestore::KeyValueStore;
using keyvaluestore::Response;
using keyvaluestore::Request;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;


#define MGET_BATCH 20

// Logic and data behind the server's behavior.
class KeyValueStoreServiceImpl final : public KeyValueStore::Service {
public:
    KeyValueStoreServiceImpl(std::shared_ptr<Redis> redis, std::string server_ip, uint16_t port)
        : redis(redis), server_ip(server_ip), server_port(port) {}

    ~KeyValueStoreServiceImpl() {
        server_->Shutdown();
        // Always shutdown the completion queue after the server.
        cq_->Shutdown();
    }

    void Run() {
        std::string server_address(server_ip + ":" + std::to_string(server_port));

        ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        // Register "service_" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *asynchronous* service.
        builder.RegisterService(&service_);
        // Get hold of the completion queue used for the asynchronous communication
        // with the gRPC runtime.
        cq_ = builder.AddCompletionQueue();
        // Finally assemble the server.
        server_ = builder.BuildAndStart();
        std::cout << "gRPC Server listening on " << server_address << std::endl;

        // Proceed to the server's main loop.
        HandleRpcs();
    }

private:


    // Class encompasing the state and logic needed to serve a request.
    class CallData {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallData(KeyValueStore::AsyncService* service, ServerCompletionQueue* cq, std::shared_ptr<Redis> redis)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), redis_(redis) {
            // Invoke the serving logic right away.
            Proceed();
        }

        void Proceed() {
            if (status_ == CREATE) {
                // Make this instance progress to the PROCESS state.
                status_ = PROCESS;

                // As part of the initial CREATE state, we *request* that the system
                // start processing SayHello requests. In this request, "this" acts are
                // the tag uniquely identifying the request (so that different CallData
                // instances can serve different requests concurrently), in this case
                // the memory address of this CallData instance.
                service_->RequestPriorityPull(&ctx_, &request_, &responder_, cq_, cq_,
                                        this);
            } else if (status_ == PROCESS) {
                // Spawn a new CallData instance to serve new clients while we process
                // the one for this CallData. The instance will deallocate itself as
                // part of its FINISH state.
                new CallData(service_, cq_, redis_);

                // The actual processing.
                PriorityPullProceed(request_, response_);

                // And we are done! Let the gRPC runtime know we've finished, using the
                // memory address of this instance as the uniquely identifying tag for
                // the event.
                status_ = FINISH;
                responder_.Finish(response_, Status::OK, this);
            } else {
                GPR_ASSERT(status_ == FINISH);
                // Once in the FINISH state, deallocate ourselves (CallData).
                delete this;
            }
        }

    private:

        void PriorityPullProceed(Request & request_, Response & response_)  {
            std::vector<OptionalString> values;
            std::vector<std::string> keys;
            std::vector<std::string> vals;
                
            std::copy(request_.keys().begin(), request_.keys().end(), std::back_inserter(keys));
            this->redis_->mget(keys.begin(), keys.end(), std::back_inserter(values));
                
            for (auto & val: values) {
                if (val)
                    vals.push_back(*val);
                else 
                    vals.push_back("-1");
            }
            response_.mutable_values()->Assign(vals.begin(), vals.end()); 
            this->redis_->del(keys.begin(), keys.end()); // Must delete immediately to avoid duplication
        }
        
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        KeyValueStore::AsyncService* service_;
        // The producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cq_;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        ServerContext ctx_;

        // What we get from the client.
        Request request_;
        // What we send back to the client.
        Response response_;

        // The means to get back to the client.
        ServerAsyncResponseWriter<Response> responder_;

        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
        std::shared_ptr<Redis> redis_;
    };

    // This can be run in multiple threads if needed.
    void HandleRpcs() {
        // Spawn a new CallData instance to serve new clients.
        new CallData(&service_, cq_.get(), redis);
        void* tag;  // uniquely identifies a request.
        bool ok;
        while (true) {
            // Block waiting to read the next event from the completion queue. The
            // event is uniquely identified by its tag, which in this case is the
            // memory address of a CallData instance.
            // The return value of Next should always be checked. This return value
            // tells us whether there is any kind of event or cq_ is shutting down.
            GPR_ASSERT(cq_->Next(&tag, &ok));
            GPR_ASSERT(ok);
            static_cast<CallData*>(tag)->Proceed();
        }
    }

    std::shared_ptr<Redis> redis;
    std::string server_ip;
    uint16_t server_port;

    std::unique_ptr<ServerCompletionQueue> cq_;
    KeyValueStore::AsyncService service_;
    std::unique_ptr<Server> server_;

};





class KeyValueStoreClient {
public:
    explicit KeyValueStoreClient(std::shared_ptr<Channel> channel)
        : stub_(KeyValueStore::NewStub(channel)) {}

    void PriorityPull(const std::vector<std::string>& keys, std::vector<std::string>& values) { 

        Request request;
        request.mutable_keys()->Assign(keys.begin(), keys.end());

        Response response;
        ClientContext context;
        CompletionQueue cq;
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<Response> > rpc(
            stub_->PrepareAsyncPriorityPull(&context, request, &cq));
        
        rpc->StartCall();
        
        rpc->Finish(&response, &status, (void*)1);
        void* got_tag;
        bool ok = false;
        GPR_ASSERT(cq.Next(&got_tag, &ok));
        GPR_ASSERT(got_tag == (void*)1);
        GPR_ASSERT(ok);
        if (status.ok()) {
            std::copy(response.values().begin(), response.values().end(), std::back_inserter(values));
        } else {
            std::cout << "RPC failed" << std::endl;
        }
    }

    private:
        std::unique_ptr<KeyValueStore::Stub> stub_;
};