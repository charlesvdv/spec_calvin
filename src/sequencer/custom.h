#ifndef SEQUENCER_CUSTOM_H
#define SEQUENCER_CUSTOM_H

#include "common/configuration.h"
#include "common/utils.h"
#include "common/connection.h"
#include "sequencer/utils.h"
#include "sequencer/to-multicast.h"
#include "sequencer/protocol-switch.h"
#include "proto/txn.pb.h"
#include "proto/switch-info.pb.h"
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

    bool GetOrderedTxn(TxnProto **txn) {
        return ordered_operations_.Pop(txn);
    }

    void WaitForStart() {
        while (!started)
            ;
    }
    void output(DeterministicScheduler *scheduler);

private:
    void RunThread();

    static void *RunThreadHelper(void *arg) {
        reinterpret_cast<CustomSequencer*>(arg)->RunThread();
        return NULL;
    }

    void Synchronize();
    vector<TxnProto*> HandleReceivedOperations();

    LogicalClockT RunConsensus(vector<TxnProto*> batch, vector<TxnProto*> decided_ops);

    // Method used for protocol switching.
    void HandleProtocolSwitch(bool got_txns_executed);
    SwitchInfoProto::PartitionType GetPartitionType();
    void SendSwitchMsg(SwitchInfoProto *payload, int partition_id = -1);
    ProtocolSwitchInfo *protocol_switch_info_;

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
    Connection *switch_connection_;

    AtomicQueue<MessageProto> *message_queues;

    pthread_t thread_;

    int batch_count_;

    bool destructor_invoked_ = false;
    bool started = false;

    int start_time_;
};

class CustomSequencerSchedulerInterface {
public:
    CustomSequencerSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer,
        Client *client, bool enable_adaptive_switching = true);
    ~CustomSequencerSchedulerInterface();

    void output(DeterministicScheduler *scheduler);
private:
    void RunClient();

    static void *RunClientHelper(void *arg) {
        reinterpret_cast<CustomSequencerSchedulerInterface*>(arg)->RunClient();
        return NULL;
    }

    CustomSequencer *sequencer_;

    Client *client_;

    Connection *connection_;
    pthread_t thread_;

    bool destructor_invoked_;
    int max_batch_size = atoi(ConfigReader::Value("max_batch_size").c_str());

    Configuration *configuration_;

    bool enable_adaptive_switching_;
};

#endif
