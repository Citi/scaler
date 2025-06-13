#include "scaler/io/ymq/event_manager.h"

#include <memory>

#include "scaler/io/ymq/event_loop_thread.h"

// void EventManager::addToEventLoop() {
//     _eventLoopThread.getEventLoop().registerEventManager(*this);
// }
//
// void EventManager::removeFromEventLoop() {
//     _eventLoopThread.getEventLoop().removeEventManager(*this);
// }

EventManager::EventManager(std::shared_ptr<EventLoopThread> eventLoopThread): _eventLoopThread(eventLoopThread) {
    type = 114;
}
