#include "connector.hpp"

Status connector_init(
    IoContext* ioctx, Connector* connector, Transport transport, ConnectorType type, uint8_t* identity, size_t len) {
    switch (transport) {
        case Transport::TCP:
        case Transport::InterProcess: {
            new (connector) Connector {.type = Connector::Socket, .network = nullptr};

            return network_connector_init(ioctx, &connector->network, transport, type, identity, len);
        }
        case Transport::IntraProcess: {
            new (connector) Connector {.type = Connector::IntraProcess, .intra_process = nullptr};

            return intra_process_init(ioctx, &connector->intra_process, identity, len);
        }
        default: unreachable();
    }
}

Status connector_destroy(Connector* connector) {
    switch (connector->type) {
        case Connector::Socket: {
            auto status = network_connector_destroy(connector->network);
            delete connector->network;
            return status;
        }
        case Connector::IntraProcess: {
            auto status = intra_process_destroy(connector->intra_process);
            delete connector->intra_process;
            return status;
        }
        default: unreachable();
    }
}

Status connector_connect(Connector* connector, const char* host, uint16_t port) {
    switch (connector->type) {
        case Connector::Socket: return network_connector_connect(connector->network, host, port);
        case Connector::IntraProcess: return intra_process_connect(connector->intra_process, host);
        default: unreachable();
    }
}

Status connector_bind(Connector* connector, const char* host, uint16_t port) {
    switch (connector->type) {
        case Connector::Socket: return network_connector_bind(connector->network, host, port);
        case Connector::IntraProcess: return intra_process_bind(connector->intra_process, host);
        default: unreachable();
    }
}

void connector_send_async(
    void* future, Connector* connector, uint8_t* to, size_t to_len, uint8_t* data, size_t data_len) {
    switch (connector->type) {
        case Connector::Socket:
            return network_connector_send_async(future, connector->network, to, to_len, data, data_len);
        case Connector::IntraProcess: {
            // intraprocess only support sync send, so perform the send synchronously and complete the future
            auto status = intra_process_send_sync(connector->intra_process, data, data_len);
            return future_set_status(future, &status);
        }
        default: unreachable();
    }
}

Status connector_send_sync(Connector* connector, uint8_t* to, size_t to_len, uint8_t* data, size_t data_len) {
    switch (connector->type) {
        case Connector::Socket: return network_connector_send_sync(connector->network, to, to_len, data, data_len);
        case Connector::IntraProcess: return intra_process_send_sync(connector->intra_process, data, data_len);
        default: unreachable();
    }
}

void connector_recv_async(void* future, Connector* connector) {
    switch (connector->type) {
        case Connector::Socket: return network_connector_recv_async(future, connector->network);
        case Connector::IntraProcess: return intra_process_recv_async(future, connector->intra_process);
        default: unreachable();
    }
}

Status connector_recv_sync(Connector* connector, struct Message* msg) {
    switch (connector->type) {
        case Connector::Socket: return network_connector_recv_sync(connector->network, msg);
        case Connector::IntraProcess: return intra_process_recv_sync(connector->intra_process, msg);
        default: unreachable();
    }
}
