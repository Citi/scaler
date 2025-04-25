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

    ~Connector() {}
};

Status connector_init(Session *session, Connector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len);
Status connector_destroy(Connector *connector, bool destruct);
Status connector_bind(Connector *connector, const char *host, uint16_t port);
Status connector_connect(Connector *connector, const char *host, uint16_t port);
void connector_send_async(void *future, Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
Status connector_send_sync(Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len);
void connector_recv_async(void *future, Connector *connector);
Status connector_recv_sync(Connector *connector, Message *msg);
