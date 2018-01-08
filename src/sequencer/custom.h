#ifndef SEQUENCER_CUSTOM_H
#define SEQUENCER_CUSTOM_H

#include "common/configuration.h"
#include "common/utils.h"
#include "common/connection.h"
#include "proto/txn.pb.h"
#include "pthread.h"

class Configuration;
class Connection;
class Storage;
class TxnProto;
class MessageProto;
class ConnectionMultiplexer;

class CustomSequencer {
public:
    CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer);

    ~CustomSequencer();

    void WaitForStart() {
        while (!started)
            ;
    }
private:
    void RunThread();

    static void *RunThreadHelper(void *arg) {
        reinterpret_cast<CustomSequencer*>(arg)->RunThread();
        return NULL;
    }

    void Synchronize();

    Configuration *configuration_;
    ConnectionMultiplexer *multiplexer_;

    Connection *connection_;

    AtomicQueue<MessageProto> *message_queues;

    pthread_t thread_;

    bool destructor_invoked_ = false;
    bool started = false;
};

#endif
