#include "connector.hpp"

void connector_init(Session *session, Connector *connector, Transport transport, ConnectorType type, uint8_t *identity, size_t len)
{
    switch (transport)
    {
    case Transport::TCP:
    case Transport::InterProcess:
    {
        new (connector) Connector{
            .type = Connector::Socket,
            .network = (NetworkConnector *)std::malloc(sizeof(NetworkConnector))};

        return network_connector_init(session, connector->network, transport, type, identity, len);
    }
    case Transport::IntraProcess:
    {
        new (connector) Connector{
            .type = Connector::IntraProcess,
            .intra_process = (IntraProcessConnector *)std::malloc(sizeof(IntraProcessConnector))};

        return intra_process_init(session, connector->intra_process, identity, len);
    }
    }
}

void connector_destroy(Connector *connector)
{
    switch (connector->type)
    {
    case Connector::Socket:
    {
        network_connector_destroy(connector->network);
        std::free(connector->network);
        return;
    }
    case Connector::IntraProcess:
    {
        intra_process_destroy(connector->intra_process);
        std::free(connector->intra_process);
        return;
    }
    }
}

void connector_connect(Connector *connector, const char *host, uint16_t port)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_connect(connector->network, host, port);
    case Connector::IntraProcess:
        return intra_process_connect(connector->intra_process, host);
    }
}

Status connector_bind(Connector *connector, const char *host, uint16_t port)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_bind(connector->network, host, port);
    case Connector::IntraProcess:
        return intra_process_bind(connector->intra_process, host);
    }
}

void connector_send_async(void *future, Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_send(future, connector->network, to, to_len, data, data_len);
    case Connector::IntraProcess:

        // intraprocess only support sync send, so perform the send synchronously and complete the future
        intra_process_send(connector->intra_process, data, data_len);
        future_set_result(future, NULL);
        return;
    }
}

void connector_send_sync(Connector *connector, uint8_t *to, size_t to_len, uint8_t *data, size_t data_len)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_send_sync(connector->network, to, to_len, data, data_len);
    case Connector::IntraProcess:
        return intra_process_send(connector->intra_process, data, data_len);
    }
}

void connector_recv_async(void *future, Connector *connector)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_recv(future, connector->network);
    case Connector::IntraProcess:
        return intra_process_recv_async(future, connector->intra_process);
    }
}

void connector_recv_sync(Connector *connector, struct Message *msg)
{
    switch (connector->type)
    {
    case Connector::Socket:
        return network_connector_recv_sync(connector->network, msg);
    case Connector::IntraProcess:
        return intra_process_recv_sync(connector->intra_process, msg);
    }
}
