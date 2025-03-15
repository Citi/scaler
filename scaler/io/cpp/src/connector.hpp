#pragma once

#include "intra_process_connector.hpp"
#include "network_connector.hpp"

struct Connector {
    enum Type {
        Socket,
        IntraProcess
    } type;

    union {
        IntraProcessConnector *intra_process;
        NetworkConnector *network;
    };
};

void connector_bind(Connector *connector, const char *addr, uint16_t port);
