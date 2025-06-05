#include "event_manager.h"

void EventManager::addToEventLoop() {
    thread.getEventLoop().registerEventManager(*this);
}

void EventManager::removeFromEventLoop() {
    thread.getEventLoop().removeEventManager(*this);
}
