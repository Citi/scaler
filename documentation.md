# Replacing ZMQ in Scaler

Scaler relies on ZeroMQ (ZMQ) heavily for its networking.
We aim to replace Scaler's usage of ZMQ with a custom solution built in C++.

## Requirements

ZMQ sockets are a powerful abstraction and if we are to replace their usage in the Scaler, then we must implement some of its behaviours. In particular we have the following requirements:

- **Multi-protocol**: ZMQ sockets abstract over many kinds of transports: TCP, Unix, intraprocess, etc.
- **Guaranteed message delivery**: ZMQ guarantees that messages will be delivered despite network issues.
- **Reconnects**: ZMQ sockets are durable and reconnect if the underlying connection fails.
  - **Flexible connection order**: ZMQ sockets support issuing a `connect()` _before_ the remote socket has called `bind()`.
- **Multiple peers**: Unlike standard network sockets ZMQ sockets can represent a connection between multiple peers.
  - e.g., a TCP socket represents a connection between two endpoints, but a ZMQ socket can be connected to multiple endpoints at the same time.
- **Socket patterns**: ZMQ sockets come in multiple types that impact their routing behaviour, such as dealer, pub, router, etc.

In addition to implementing those ZMQ features, we also have the following requirements:

- **Async _and_ sync**: We need to support both async and sync interfaces.
- **Thread-safety**: Our ZMQ socket replacement needs to be thread-safe.
- **Multiple backends**: We need to support implementing our interface with different kinds of backends e.g. epoll, io_uring, etc.
- **Multiple consumer languages**: The library needs to be usable from multiple languages e.g. Python, C/C++, and possibly more.

## Implementation

### Structures

```c++
// rationale:
// - we need a context to manage the io threads
// - intraprocess connectors need a context to discover and communicate with each other
//
// lifetime:
// - the context is the longest lived object in the library
// - you should normally only have one context per program
// - the lifetime begins when you call `io_context_init()` and ends when you call `io_context_destroy()`
//
// usage:
// - the main usage of io context is to be passed to connector initialization
// - use `io_context_*()` functions to operate on the context
//
// assumptions:
// - the user is not going to access the internal state of the io context directly
struct IoContext {
    // the io threads
    std::vector<ThreadContext> threads;
    std::atomic_uint8_t thread_rr;

    std::vector<IntraProcessConnector*> inprocs;
    std::shared_mutex intra_process_mutex;
};

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
```
