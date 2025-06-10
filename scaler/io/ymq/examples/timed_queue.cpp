#include "scaler/io/ymq/timed_queue.h"

int main() {
    TimedQueue tq(nullptr);
    Timestamp ts;

    tq.push(ts, [] { printf("in timer\n"); });
}
