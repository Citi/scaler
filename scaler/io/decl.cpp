// -*- C++ -*-
#include <sys/socket.h>

#include <atomic>
#include <functional>
#include <map>
#include <optional>

/* NOTE:
 * - This file is organized in a down-to-top fashion. Internal structures are listed before Interface classes.
 * - This file is also separated by abstraction layers. There are three layers,
 *    - Network layer,
 *    - Eventloop layer,
 *    - API layer.
 * - These layers are listed in this file in above order.
 * - Do NOT panic when seeing structures that are not defined. They will be defined and explained later.
 * - There are some resources online that are useful for implementing a network lib. They are listed below.
 *    - (1). https://github.com/yugabyte/libev/blob/master/ev.pod
 *    - (2). https://cvs.schmorp.de/libev/ev.html#ANATOMY_OF_A_WATCHER
 * */

// NOTE: NETWORK LAYER

// This class is an RAII class that holds a file descriptor.
// It should,
// - Hold an OPENED file descriptor.
// - Automatically close the file descriptor when destructed.
// - When destructed, or moved, set internal fd to invalid val.
//
// It should NOT,
// - be copyable.
// - provide method that operates the internal fd.
struct FileDescriptor {
    int fd;  // <-- public
    FileDescriptor(int fd);
    // int accept() { return accept(fd, ...); }

    FileDescriptor(const FileDescriptor&)            = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&&);
    FileDescriptor& operator=(FileDescriptor&&);
    ~FileDescriptor();
};

// This class, as the name suggests, is responsible for accepting
// new connections.
// It should,
// - Hold a socket that is already listening.
// - Accept new connections in a callback.
// - Handle the typical error of accept, see (1).
// - Notify the event loop when destructed.
//
// It should NOT,
// - Have a USER PROVIDED callback, as there isn't much to do besides accepting.
// - be copyable.
class TcpServer {
    EventLoopThread& eventLoop;  // eventLoop thread will call onRead that is associated w/ the eventManager
    std::unique_ptr<EventManager> eventManager;  // will copy the `onRead()` to itself
    FileDescriptor fd;
    // Implementation defined method. accept(3) should happen here.
    // This function will call user defined onAcceptReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onAcceptReturn()
    void onRead();

public:
    TcpServer(const TcpServer&)            = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    using AcceptReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    AcceptReturnCallback onAcceptReturn;
};

// This class, as the name suggests, is responsible for connecting
// new connections.
// It should do similar things as the TcpServer does, and
// - Provide retry functionality.
// - remember the current state of the fd (whether it is connected)
class TcpClient {
    EventLoopThread& eventLoop; /* shared ownership */
    std::unique_ptr<EventManager> eventManager;
    // Implementation defined method. connect(3) should happen here.
    // This function will call user defined onConnectReturn()
    // It will handle error it can handle. If it is unreasonable to
    // handle the error here, pass it to onConnectReturn()
    void onWrite();

public:
    TcpClient(const TcpClient&)            = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    using ConnectReturnCallback = std::function<void(FileDescriptor, sockaddr, int)>;
    ConnectReturnCallback onConnectReturn;

    void retry(/* Arguments */);
};

// This class wraps a tcp-connection. It does not know anything that behaves like ZMQ.
// It should,
// - remembers its current state (whether it is connected).
// - remembers its local and remote address.
// - read and write BYTES.
// - *NEW* Remember the length of last send()
// - maintains internal buffer for send and recv.
// - close reading to the remote end.
//
// It should NOT,
// - be copyable.
// - do actual send. send in this class copies msg to internal buffer.
// Rename to MessageConnectionTCP, later on be MessageConnectionIPC
class MessageConnectionTCP {
    EventLoopThread& eventLoop; /* shared ownership */
    std::unique_ptr<EventManager> eventManager;
    std::string remoteIdentity;

    using Buffer = std::vector<unsigned char>;  // placeholder, detail TBD
    // recvBuffer moved to the MessageCallback
    Buffer sendBuffer;  // < powerful bufferr that maintains a cursor
    Buffer recvBuffer;

    sockaddr_storage localAddr;
    sockaddr_storage remoteAddr;

    // - Short read disconnect:
    //   We need to give the user back the rest of the buffer
    // - Short write disconnect:
    //   We need to give the user back the rest of the buffer
    // Put in the EventManager
    void onRead();
    void onWrite();
    void onClose();
    void onError();

    // Deal with internal Buffer
    // Message de-frame should happen here
    void onReceiveBytes();
    void onSendBytes();

    void sendBytes(char* msg, size_t len);
    bool reading;  // Are we going to read? (maybe helper functions) (maybe atomic)

    enum MessageConnectionTCPState { Disconnected, Connecting, Connected } tcpConnectionState;
    // ^-- perhaps provide functions like bool disconnected()?
    using SendMessageContinuation = std::function<void()>;  // placeholder, detail TBD
    using RecvMessageContinuation = std::function<void()>;  // placeholder, detail TBD
    std::vector<size_t> sendMessageLength;
    std::vector<size_t> recvMessageDesiredLength;

public:
    MessageConnectionTCP(const MessageConnectionTCP&)            = delete;
    MessageConnectionTCP& operator=(const MessageConnectionTCP&) = delete;

    using ConnectionCallback  = std::function<void(sockaddr, sockaddr)>;
    using RemoteCloseCallback = std::function<void()>;  // placeholder, detail TBD

    void sendMessage(Message* msg, SendMessageContinuation cont);
    void recvMessage(Message* msg, RecvMessageContinuation cont);

    void shutdown();
    void forceClose();

    ConnectionCallback onConnected;
    RemoteCloseCallback onRemoteClose;
};

// NOTE: EVENTLOOP LAYER

// This class is a unified event manager. This will be the user data we passed in to
// BackendContext(e.g. epoll_ctl). TcpServer, TcpClient, and MessageConnectionTCP use this class.
//
// It should,
// - provide a unified interface to io_uring and epoll. This means we cannot use events
// directly. An ad-hoc way is to replace `int events` with `uint64_t custom_events` and
// parse it ourselve in the corresponding BackendContext. For now, let's use `int events`.
// - Hold reference to the buffer, instead of actual ones.
// - Let the user define eventCallback
//
// It should NOT,
// - know about zmq-like handle's internal. (this logic should be placed in eventCallback)
class EventManager {
    EventLoopThread& eventLoop;
    const int fd;
    // Implementation defined method, will call onRead, onWrite etc based on events
    void onEvents();

public:
    int events;
    int revents;
    void updateEvents();

    // User that registered them should have everything they need
    // In the future, we might add more onXX() methods, for now these are all we need.
    using OnEventCallback = std::function<void()>;
    OnEventCallback onRead;
    OnEventCallback onWrite;
    OnEventCallback onClose;
    OnEventCallback onError;
};

// This class defines the interface of an event loop. It allows the user to swap its
// backend context as long as they are equipped with the correct interface.
//
// Below interface will dispatch the call to its backend.
// - loop() will start looping until stopped. Imagine the main logic of event loop goes
// here.
// - stop() will send a stop signal (not necessarily a `signal(2)`) to the thread it bundled
//   with. The event loop will quit when receiving this signal.
// - execute_now() will interrupt the current wait, and execute a function.
// - execute_later() will remember a function, of which will be executed after a round of wait.
// - execute_at() will remember a function, and execute them when time arrives.
// - cancel_execution() will cancel a function execution request, if that request is not in
//   execution yet.
// - registerCallbackBeforeLoop will register an event to the backend before the eventloop start
//   to loop(). This comes handy when we want to add an accept request, for example, to the loop.
//
// There will be three channels of communication from possibly another thread (aka Python thread).
// - immediateExecutionQueue, functors pushed to this queue will wake up the wait, and let the
//   eventloop execute the functor.
// - timedExecutionQueue, functors pushed to this queue will have a timer equipped. They will wakeup
//   eventloop when the time is right, and execute that functor. This comes handy for reconnect().
// - delayedExecutionQueue, functors pushed to this queue will be executed once when wait ends.
//
// From the backend's perspective, these three queues does not have to be concurrent queue, as the
// EventLoop already sort the problem out for them. The backend's
// - immediateExecutionQueue will possibly be a std::queue
// - timedExecutionQueue will possibly be a priorityQueue with relation imposes on timer.
// - delayedExecutionQueue will possibly be a std::queue as well.
//
// We impose no constraints on what data structure the backend will be using.
template <class EventLoopBackend = EpollContext>
struct EventLoop {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD
    void loop();
    void stop();

    // send_to_connector() C API
    // []() {
    //    binding a ref to the ioSocket; (B)
    //    auto tcpconn = B.find_tcp_connection[A]
    //    tcpconn.send(message);
    //    tcpconn.setWriteCompleteCallback(resolve future);
    // };

    void executeNow(Function func);
    void executeLater(Function func, Identifier identifier);
    void executeAt(TimeStamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);
    void registerCallbackBeforeLoop(EventManager*);

    InterruptiveConcurrentQueue<FunctionType> immediateExecutionQueue;
    TimedConcurrentQueue<FunctionType> timedExecutionQueue;
    ConcurrentQueue<FunctionType> delayedExecutionQueue;

    EventLoopBackend eventLoopBackend;
};

// EpollContext, passed in as template argument of a EventLoop
struct EpollContext {
    using Function   = std::function<void()>;  // TBD
    using TimeStamp  = int;                    // TBD
    using Identifier = int;                    // TBD
    void registerCallbackBeforeLoop(EventManager*);
    void loop() {
        for (;;) {
            // EXAMPLE
            // epoll_wait;
            // for each event that is returned to the caller {
            //     cast the event back to EventManager
            //     if this event is an eventfd {
            //         func = queue front
            //         func()
            //     } else if this event is an timerfd {
            //         if it is oneshot then execute once
            //         if it is multishot then execute and rearm timer
            //     } else {
            //         eventmanager.revent = events return by epoll
            //         eventmanager.on_event() ,
            //         where as on_event is set differently for tcpserver, tcpclient, and tcpconn

            //         they are defined something like:
            //         tcpserver.on_event() {
            //             accept the socket and generate a new tcpConn or handle err
            //             this.ioSocket.addNewConn(tcpConn)
            //         }
            //         tcpclient.on_event() {
            //             connect the socket and generate a new tcpConn
            //             this.ioSocket.addNewConn(tcpConn)
            //             if there need retry {
            //                 close this socket
            //                 this.eventloop.executeAfter(the time you want from now)
            //             }
            //         }
            //         tcpConn.on_event() {
            //             read everything you can to the buffer
            //             write everything you can to write the remote end
            //             if tcpconn.ioSocket is something special, for example dealer
            //             tcpConn.ioSocket.route to corresponding tcpConn
            //         }
            //
            //     }

            // }
        }
    }
    void stop();

    void executeNow(Function func);
    void executeLater(Function func, Identifier identifier);
    void executeAt(TimeStamp, Function, Identifier identifier);
    void cancelExecution(Identifier identifier);

    // int epoll_fd;
    // int connect_timer_tfd;
    // std::map<int, EventManager*> monitoringEvent;
    // bool timer_armed;
    // // NOTE: Utility functions, may be defined otherwise
    // void ensure_timer_armed();
    // void add_epoll(int fd, uint32_t flags, EpollType type, void* data);
    // void remove_epoll(int fd);
    // EpollData* epoll_by_fd(int fd);
};

// Types are configured here
struct configuration {
    using polling_context_t = EpollContext;
};

// NOTE: API LAYER

// - This class wraps an IOSocket, which will be the primary interface user operates on.
// - User may want to call ioSocket.send(...) from python, for example.
//
// - Every instance of this class is bundled with a thread. This is a temporary solution.
//
// It should,
// - Remember the thread it is bundled with.
// - Owns the pollingContext.
// - Maintains a bunch of connections, an acceptor, and a connector. The last two are optional.
// - Remember what kind of a socket it is.
// - Provide apis for sending and receiving messages.
class IOSocket {
    EventLoopThread& eventLoopThread;
    enum SocketTypes { Binder, Sub, Pub, Dealer, Router, Pair /* etc. */ };
    SocketTypes socketTypes;

    std::optional<TcpServer> tcpServer;
    std::optional<TcpClient> tcpClient;
    std::map<int /* class FileDescriptor */, MessageConnectionTCP*> fdToConnection;
    std::map<std::string, MessageConnectionTCP*> identityToConnection;

public:
    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;

    const std::string identity;
    // string -> connection mapping
    // and connection->string mapping

    // put it into the concurrent q, which is execute_now
    void sendMessage(Message* msg, Continuation cont) {
        // EXAMPLE
        // execute_now(
        // switch (socketTypes) {
        //     case Pub:
        //         for (auto [fd, conn] &: fd_to_conn) {
        //             conn.send(msg.len, msg.size);
        //             conn.setWriteCompleteCallback(cont);
        //             eventLoopThread.getEventLoop().update_events(turn write on for this fd);
        //         }
        //         break;
        // }
        // )
    }
    void recvMessage(Message* msg);
};

class EventLoopThread {
    using PollingContext = configuration::polling_context_t;
    std::thread thread;
    std::map<std::string /* type of IOSocket's identity */, IOSocket> identityToIOSocket;
    EventLoop<PollingContext> eventLoop;

public:
    // Why not make the class a friend class of IOContext?
    // Because the removeIOSocket method is a bit trickier than addIOSocket,
    // the IOSocket that is being removed will first remove every MessageConnectionTCP
    // managed by it from the EventLoop, before it removes it self from ioSockets.
    IOSocket* addIOSocket(/* args */);
    bool removeIOSocket(IOSocket*);
    EventLoop<PollingContext>& getEventLoop();
    IOSocket* getIOSocketByIdentity(size_t identity);

    EventLoopThread(const EventLoopThread&)            = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;
};

class IOContext {
    std::vector<EventLoopThread> threads;

    // std::vector<IntraProcessTcpClient*> inprocs;
    // std::shared_mutex intra_process_mutex;

public:
    IOContext(const IOContext&)            = delete;
    IOContext& operator=(const IOContext&) = delete;
    IOContext(IOContext&&)                 = delete;
    IOContext& operator=(IOContext&&)      = delete;

    // These methods need to be thread-safe.
    IOSocket* addIOSocket(/* args */);
    // ioSocket.getEventLoop().removeIOSocket(&ioSocket);
    bool removeIOSocket(IOSocket*);
};

// NOTE: There are some structure that needs to be defined. These are,
// - Buffer. The Buffer should be a vector equipped with a cursor. It should remember
//   the user's message len and call writeCompleteCallback when it finishes writing.
// - TimedConcurrentQueue. A priority queue container that is concurrent.

// NOTE:
// Major changes since last revision:
//  - It used to be the case that each IOSocket maintains one thread. This is not feasible
//    because Object Storage Server will also use this code. Since it tends to maintain a
//    large amount of IOSocket, where each IOSocket contains only one MessageConnectionTCP, the
//    overhead will be too heavy. Plus, with this design user will not be able to control
//    the amount of thread being spawned. NOW, multiple IOSockets share the same event loop
//    thread, and the amound of event loop thread should be specified by the user.
// With respect to last week's discussion:
//  - Discussion result, renaming Acceptor -> TcpServer, Connector -> TcpClient.
//  - FileDescriptor should not have methods FOR NOW. This is because we are currently in
//    design phase, the decision should be postponed to the implementation phase.
//  - Bundled some arguments together. EventLoopThread will maintain identity to IOSockets,
//    which must present due to the need to support guaranteed message delivery. Information
//    that used to be retrieved from IOSocket  are now retrieved indirectly from
//    EventLoopThread.
//  - Do not fill in the function call back type FOR NOW. This will not affect the design
//    in a significant way.
//  - Do NOT remove EventManager type. This is because we need to remember its members'
//    states. Especially what events that user cares about. There's no other
//    good place to store, for example, `EventManager::events` if we turn EventManager
//    to a std::function. The proposed "capture this" requires every component that interact
//    with epoll to know about `events` and `revents`. Secondly, we sometime need to
//    retrieve this information outside the callback. For example, we might be interested
//    in turning off reading/writing from a connection. If we store information that used
//    to be stored in EventManager in components like MessageConnectionTCP etc. then we need to
//    find the actual object instead of simply get it from the EventManager. This essentially
//    make the epoll context know each and every component and breaks encapsulation. What we
//    can do, however, is to put the common part in EventManager, and store a std::function
//    in the EventManager and let components interact with that.
