#pragma once

#include "intra_process_connector.hpp"
#include "network_connector.hpp"

struct Connector {
    enum Type {
        Socket,
        IntraProcess
    } type;

    union {
        IntraProcessConnector *intra_process_connector;
        NetworkConnector *network_connector;
    };
};

// void network_connector_bind(Connector *connector);
