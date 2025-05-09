#pragma once

#include "intra_process_connector.hpp"
#include "network_connector.hpp"

// rationale:
// - the interface for intraprocess and socket-based connectors needs to be unified
//
// lifetime:
// - the connector lives within the scope of the io context and thread context
//   - all connectors MUST be destroyed before the io context is destroyed
// - its lifetime begins when you call `connector_init()` and ends when you call `connector_destroy()`
// - its lifetime is flexible and determined by the library user
//
// usage:
// - the connector is the primary interface of the library
// - the connector is used to send and receive messages
// - use `connector_*()` functions to operate on the connector
//
// assumptions:
// - the user is not going to access the internal state of the connector directly
struct Connector
{
    enum Type
    {
        Socket,
        IntraProcess
    } type;

    union
    {
        IntraProcessConnector *intra_process;
        NetworkConnector *network;
    };
};

extern "C" {
Status connector_init(IoContext* ioctx, Connector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len);
Status connector_destroy(Connector *connector);
Status connector_bind(Connector *connector, const char *host, uint16_t port);
Status connector_connect(Connector *connector, const char *host, uint16_t port);
void connector_send_async(void *future, Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
Status connector_send_sync(Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_recv_async(void *future, Connector *connector);
Status connector_recv_sync(Connector *connector, Message *msg);
}
