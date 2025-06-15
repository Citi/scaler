
// C
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>

// First-party
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

int main() {
    IOContext context;
    std::shared_ptr<IOSocket> clientSocket = context.createIOSocket("ClientSocket", IOSocketType::Uninit);

    const char* ip = "127.0.0.1";  // example.com
    const int port = 8080;

    // Create a non-blocking socket using SOCK_NONBLOCK
    int sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(port);
    inet_pton(AF_INET, ip, &server_addr.sin_addr);
    clientSocket->connectTo(*(sockaddr*)&server_addr);
    sleep(10000);

    // char buf[8];
    // while (true) {
    //     socket.read("any_identity", buf, []() { printf("read completed\n"); });
    //     socket.write("reading_from_identity", buf, []() { printf("write completed\n"); });
    // }
    // printf("done");
}
