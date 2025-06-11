#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/system/system_error.hpp>
#include <iostream>
#include <map>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"

template <>
struct std::hash<scaler::object_storage::object_t> {
    std::size_t operator()(const scaler::object_storage::object_t& x) const noexcept {
        return std::hash<std::string_view> {}({reinterpret_cast<const char*>(x.data()), x.size()});
    }
};

namespace scaler {
namespace object_storage {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

class ObjectStorageServer {
    struct Meta {
        std::shared_ptr<boost::asio::ip::tcp::socket> socket;
        ObjectRequestHeader requestHeader;
        ObjectResponseHeader responseHeader;
    };

    struct ObjectWithMeta {
        shared_object_t object;
        std::vector<Meta> metaInfo;
    };

    using reqType  = ::ObjectRequestHeader::ObjectRequestType;
    using respType = ::ObjectResponseHeader::ObjectResponseType;

    std::span<const unsigned char> getMemoryViewForResponsePayload(
        scaler::object_storage::ObjectResponseHeader& header) {
        switch (header.respType) {
            case respType::GET_O_K: return {objectIDToMeta[header.objectID].object->data(), header.payloadLength};
            case respType::SET_O_K:
            case respType::DEL_O_K:
            case respType::DEL_NOT_EXISTS:
            default: break;
        }
        return {static_cast<const unsigned char*>(nullptr), 0};
    }

#ifndef NDEBUG
public:
#endif
    bool updateRecord(
        const scaler::object_storage::ObjectRequestHeader& requestHeader,
        scaler::object_storage::ObjectResponseHeader& responseHeader,
        scaler::object_storage::payload_t payload) {
        responseHeader.objectID   = requestHeader.objectID;
        responseHeader.responseID = requestHeader.requestID;
        switch (requestHeader.reqType) {
            case reqType::SET_OBJECT: {
                auto objectHash = std::hash<object_t> {}(payload);
                if (!objectHashToObject.contains(objectHash)) {
                    objectHashToObject[objectHash] =
                        std::make_shared<scaler::object_storage::object_t>(std::move(payload));
                }
                responseHeader.respType                       = respType::SET_O_K;
                objectIDToMeta[requestHeader.objectID].object = objectHashToObject[objectHash];

                break;
            }

            case reqType::GET_OBJECT: {
                responseHeader.respType = respType::GET_O_K;
                if (objectIDToMeta[requestHeader.objectID].object)
                    responseHeader.payloadLength =
                        std::min(objectIDToMeta[requestHeader.objectID].object->size(), requestHeader.payloadLength);
                else
                    return false;
                break;
            }

            case reqType::DELETE_OBJECT: {
                responseHeader.respType =
                    objectIDToMeta[requestHeader.objectID].object ? respType::DEL_O_K : respType::DEL_NOT_EXISTS;
                auto sharedObject = objectIDToMeta[requestHeader.objectID].object;
                objectIDToMeta.erase(requestHeader.objectID);
                if (sharedObject.use_count() == 2) {
                    objectHashToObject.erase(std::hash<object_t> {}(*sharedObject));
                }
                break;
            }
        }
        return true;
    }

private:
    awaitable<void> write_once(Meta meta) {
        if (meta.requestHeader.reqType == reqType::GET_OBJECT) {
            meta.responseHeader.payloadLength =
                std::min(objectIDToMeta[meta.responseHeader.objectID].object->size(), meta.requestHeader.payloadLength);
        }

        auto payload_view = getMemoryViewForResponsePayload(meta.responseHeader);
        co_await scaler::object_storage::write_response_header(*meta.socket, meta.responseHeader, payload_view.size());
        co_await scaler::object_storage::write_response_payload(*meta.socket, payload_view);
    }

    awaitable<void> optionally_send_pending_requests(scaler::object_storage::ObjectRequestHeader requestHeader) {
        if (requestHeader.reqType == reqType::SET_OBJECT) {
            for (auto& curr_meta: objectIDToMeta[requestHeader.objectID].metaInfo) {
                try {
                    co_await write_once(std::move(curr_meta));
                } catch (boost::system::system_error& e) {
                    std::cerr << "Mostly because some connections disconnected accidentally.\n";
                }
            }
            objectIDToMeta[requestHeader.objectID].metaInfo = std::vector<Meta>();
        }
        co_return;
    }

#ifndef NDEBUG
public:
#endif
    std::map<scaler::object_storage::object_id_t, ObjectWithMeta> objectIDToMeta;
    std::map<std::size_t, shared_object_t> objectHashToObject;

public:
    awaitable<void> process_request(std::shared_ptr<tcp::socket> socket) {
        try {
            for (;;) {
                scaler::object_storage::ObjectRequestHeader requestHeader;
                co_await scaler::object_storage::read_request_header(*socket, requestHeader);

                scaler::object_storage::payload_t payload;
                co_await scaler::object_storage::read_request_payload(*socket, requestHeader, payload);

                scaler::object_storage::ObjectResponseHeader responseHeader;
                bool non_blocking_request = updateRecord(requestHeader, responseHeader, std::move(payload));

                co_await optionally_send_pending_requests(requestHeader);

                if (!non_blocking_request) {
                    objectIDToMeta[requestHeader.objectID].metaInfo.emplace_back(socket, requestHeader, responseHeader);
                    continue;
                }

                auto payload_view = getMemoryViewForResponsePayload(responseHeader);

                co_await scaler::object_storage::write_response_header(*socket, responseHeader, payload_view.size());

                co_await scaler::object_storage::write_response_payload(*socket, payload_view);
            }
        } catch (std::exception& e) {
            // TODO: Logging support
            // std::printf("process_request Exception: %s\n", e.what());
        }
    }

private:
    awaitable<void> listener(boost::asio::ip::tcp::endpoint endpoint) {
        auto executor = co_await boost::asio::this_coro::executor;
        tcp::acceptor acceptor(executor, endpoint);
        for (;;) {
            auto shared_socket = std::make_shared<tcp::socket>(executor);
            co_await acceptor.async_accept(*shared_socket, use_awaitable);
            co_spawn(executor, process_request(std::move(shared_socket)), detached);
        }
    }

public:
    void run(std::string name, std::string port) {
        try {
            boost::asio::io_context io_context(1);
            tcp::resolver resolver(io_context);
            auto res = resolver.resolve(name, port);

            boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
            signals.async_wait([&](auto, auto) { io_context.stop(); });

            co_spawn(io_context, listener(res.begin()->endpoint()), detached);

            io_context.run();
        } catch (std::exception& e) {
            std::printf("Exception: %s\n", e.what());
            std::printf("Mostly something serious happen, inspect capnp header correuption\n");
        }
    }
};

};  // namespace object_storage
};  // namespace scaler
