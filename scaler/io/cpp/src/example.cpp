#include "main.hpp"

#define CHECK(expr) \
    if (auto status = (expr); status.type != ErrorType::Ok) \
        panic(status.message, std::source_location::current())
        

void future_set_result(void* future, void* data) {}
void future_set_status(void* future, void* status) {}

int main() {
    Session session;
    CHECK(session_init(&session, 0));

    Connector conn_a, conn_b;
    CHECK(connector_init(&session, &conn_a, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_a", 6));
    CHECK(connector_init(&session, &conn_b, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_b", 6));

    CHECK(connector_bind(&conn_a, "conn_a", 6));
    CHECK(connector_connect(&conn_b, "conn_a", 6));

    std::string content = "Hello from conn_a";

    CHECK(connector_send_sync(&conn_a, NULL, 0, (uint8_t*)content.c_str(), content.size()));

    Message msg;
    CHECK(connector_recv_sync(&conn_b, &msg));

    std::cout << "Received message: " << msg.payload.as_string() << std::endl;

    message_destroy(&msg);

    CHECK(session_destroy(&session, false));
    CHECK(connector_destroy(&conn_a, false));
    CHECK(connector_destroy(&conn_b, false));

    return 0;
}
