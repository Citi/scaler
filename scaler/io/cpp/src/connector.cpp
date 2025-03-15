#include "connector.hpp"

void connector_connect(Connector *connector, const char *addr, uint16_t port) {
    switch (connector->type) {
        case Connector::Socket:       network_connector_connect(connector->network, addr, port); break;
        case Connector::IntraProcess: intra_process_connect(connector->intra_process, addr);   break;
    }
}
