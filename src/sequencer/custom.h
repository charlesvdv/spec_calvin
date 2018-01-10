#ifndef SEQUENCER_CUSTOM_H
#define SEQUENCER_CUSTOM_H

#include "common/configuration.h"
#include "common/utils.h"
#include "common/connection.h"
#include "sequencer/utils.h"
#include "sequencer/to-multicast.h"
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

    // Propose a batch of messages which needs ordering.
    void OrderTxns(vector<TxnProto*> txns);
    AtomicQueue<TxnProto*>* GetOrderedTxns() {
        return &ordered_operations_;
    }

    // Get decided transaction, the vector is supposed to be in order.
    std::vector<TxnProto*> GetDecided();

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

    // void Synchronize();
    vector<TxnProto*> HandleReceivedOperations();

    void RunReplicationConsensus(vector<TxnProto*> txns);

    LogicalClockT GetMaxGroupExecutableClock();

    TOMulticast *genuine_;

    AtomicQueue<vector<TxnProto*>> received_operations_;
    AtomicQueue<TxnProto*> ordered_operations_;

    // The key is the transaction id.
    map<int, TxnProto*> pending_operations_;
    vector<TxnProto*> ready_operations_;
    vector<TxnProto*> executable_operations_;

    // Batch received for a specific round.
    map<int, vector<MessageProto*>> batch_messages_;

    Configuration *configuration_;
    ConnectionMultiplexer *multiplexer_;

    Connection *connection_;

    AtomicQueue<MessageProto> *message_queues;

    pthread_t thread_;

    int batch_count_;

    bool destructor_invoked_ = false;
    bool started = false;
};

#endif
