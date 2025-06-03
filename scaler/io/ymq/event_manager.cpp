#include "event_manager.hpp"

void EventManager::addToEventLoop() {
    thread.getEventLoop().registerEventManager(*this);
}

void EventManager::removeFromEventLoop() {
    thread.getEventLoop().removeEventManager(*this);
}
