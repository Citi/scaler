#pragma once

#include "intra_process_connector.hpp"
#include "network_connector.hpp"

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

void connector_init(Session *session, Connector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len);
void connector_destroy(Connector *connector);
Status connector_bind(Connector *connector, const char *host, uint16_t port);
void connector_connect(Connector *connector, const char *host, uint16_t port);
void connector_send_async(void *future, Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_send_sync(Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_recv_async(void *future, Connector *connector);
void connector_recv_sync(Connector *connector, Message *msg);
