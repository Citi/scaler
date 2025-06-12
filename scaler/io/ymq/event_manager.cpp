#include "scaler/io/ymq/event_manager.h"

void EventManager::addToEventLoop() {
    _eventLoopThread->registerEventManager(*this);
}

void EventManager::removeFromEventLoop() {
    _eventLoopThread->removeEventManager(*this);
}
