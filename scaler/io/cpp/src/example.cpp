#include "main.hpp"

#define CHECK(expr)                                         \
    if (auto status = (expr); status.type != ErrorType::Ok) \
    panic(status.message, std::source_location::current())

struct Future {
    sem_t* sem;
    enum { Msg, Status_, Initial } tag;

    union {
        Message* msg;
        Status status;
    };

    void complete_msg(Message* msg) {
        tag = Msg;

        this->msg = new Message {
            .address = Bytes::clone(msg->address),
            .payload = Bytes::clone(msg->payload),
        };

        if (sem_post(sem) < 0)
            panic("failed to post semaphore");
    }

    void complete_status(Status* status) {
        tag          = Status_;
        this->status = *status;

        if (sem_post(sem) < 0)
            panic("failed to post semaphore");
    }

    void wait() {
        if (sem_wait(sem) < 0)
            panic("failed to wait on semaphore");
    }

    Future(): sem(new sem_t), tag(Initial) {
        if (sem_init(sem, 0, 0) < 0)
            panic("failed to initialize semaphore");
    };

    ~Future() {
        if (tag == Msg) {
            delete msg;
        }

        if (sem_destroy(sem) < 0)
            panic("failed to destroy semaphore");
        delete sem;
    }
};

void future_set_result(void* future, void* data) {
    auto fut = static_cast<Future*>(future);
    auto msg = static_cast<Message*>(data);

    fut->complete_msg(msg);
}

void future_set_status(void* future, void* status) {
    auto fut  = static_cast<Future*>(future);
    auto stat = static_cast<Status*>(status);

    fut->complete_status(stat);
}

void example_one() {
    Session session;
    CHECK(session_init(&session, 0));

    Connector conn_a, conn_b;
    CHECK(connector_init(&session, &conn_a, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_a", 6));
    CHECK(connector_init(&session, &conn_b, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_b", 6));

    CHECK(connector_bind(&conn_a, "conn_a", 0));
    CHECK(connector_connect(&conn_b, "conn_a", 0));

    std::string content = "Hello from conn_a; example one";

    CHECK(connector_send_sync(&conn_a, NULL, 0, (uint8_t*)content.c_str(), content.size()));

    Message msg;
    CHECK(connector_recv_sync(&conn_b, &msg));

    std::cout << "Received message: " << msg.payload.as_string() << std::endl;

    message_destroy(&msg);

    CHECK(connector_destroy(&conn_a, false));
    CHECK(connector_destroy(&conn_b, false));
    CHECK(session_destroy(&session, false));
}

void example_two() {
    Session session;
    CHECK(session_init(&session, 1));

    Connector conn_a, conn_b;
    CHECK(connector_init(&session, &conn_a, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_a", 6));
    CHECK(connector_init(&session, &conn_b, Transport::IntraProcess, ConnectorType::Pair, (uint8_t*)"conn_b", 6));

    std::string addr = "/tmp/conn_a";

    CHECK(connector_bind(&conn_a, addr.c_str(), 0));
    CHECK(connector_connect(&conn_b, addr.c_str(), 0));

    Future send, recv;
    connector_recv_async(&recv, &conn_b);

    std::string content = "Hello from conn_a; example two";
    connector_send_async(&send, &conn_a, NULL, 0, (uint8_t*)content.c_str(), content.size());

    send.wait();

    if (send.tag != Future::Status_) {
        panic("send failed: " + std::string(send.status.message));
    }

    if (send.status.type != ErrorType::Ok) {
        panic("send failed: " + std::string(send.status.message));
    }

    recv.wait();

    if (recv.tag != Future::Msg) {
        panic("recv failed: " + std::string(recv.status.message));
    }

    std::cout << "Received message: " << recv.msg->payload.as_string() << std::endl;

    CHECK(connector_destroy(&conn_a, false));
    CHECK(connector_destroy(&conn_b, false));
    CHECK(session_destroy(&session, false));
}

void example_three() {
    Session session;
    CHECK(session_init(&session, 1));

    Connector conn_a, conn_b;
    CHECK(connector_init(&session, &conn_a, Transport::TCP, ConnectorType::Pair, (uint8_t*)"conn_a", 6));
    CHECK(connector_init(&session, &conn_b, Transport::TCP, ConnectorType::Pair, (uint8_t*)"conn_b", 6));

    std::string addr = "127.0.0.1";
    uint16_t port    = 12345;

    CHECK(connector_bind(&conn_a, addr.c_str(), port));
    CHECK(connector_connect(&conn_b, addr.c_str(), port));

    Future send, recv;
    connector_recv_async(&recv, &conn_b);

    std::string content = "Hello from conn_a; example three";
    connector_send_async(&send, &conn_a, NULL, 0, (uint8_t*)content.c_str(), content.size());

    send.wait();

    if (send.tag != Future::Status_) {
        panic("send failed: " + std::string(send.status.message));
    }

    if (send.status.type != ErrorType::Ok) {
        panic("send failed: " + std::string(send.status.message));
    }

    recv.wait();

    if (recv.tag != Future::Msg) {
        panic("recv failed: " + std::string(recv.status.message));
    }

    std::cout << "Received message: " << recv.msg->payload.as_string() << std::endl;

    CHECK(connector_destroy(&conn_a, true));
    CHECK(connector_destroy(&conn_b, true));
    CHECK(session_destroy(&session, false));
}

int main() {
    example_one();
    example_two();
    example_three();

    return 0;
}
