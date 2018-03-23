#ifndef SEQUENCER_CUSTOM_H
#define SEQUENCER_CUSTOM_H

#include "common/configuration.h"
#include "common/utils.h"
#include "common/connection.h"
#include "sequencer/utils.h"
#include "sequencer/to-multicast.h"
#include "sequencer/protocol-switch.h"
#include "sequencer/sequencer.h"
#include "proto/txn.pb.h"
#include "proto/switch-info.pb.h"
#include "pthread.h"
#include "common/client.h"
#include "scheduler/deterministic_scheduler.h"
// #include "scheduler/scheduler.h"

class Configuration;
class Connection;
class Storage;
class TxnProto;
class MessageProto;
class ConnectionMultiplexer;

class CustomSequencer: public AbstractSequencer {
public:
    CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client);

    ~CustomSequencer();

    // Propose a batch of messages which needs ordering.
    // void OrderTxns(vector<TxnProto*> txns);

    // bool GetOrderedTxn(TxnProto **txn) {
        // return ordered_operations_.Pop(txn);
    // }

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
    bool GetBatch(vector<TxnProto*> *batch);
    void ExecuteTxns(vector<TxnProto*> &txns);
    void OptimizeProtocols(int round_delta);

    LogicalClockT RunConsensus(vector<TxnProto*> batch);

    // Method used for protocol switching.
    void HandleProtocolSwitch(bool got_txns_executed);
    SwitchInfoProto::PartitionType GetPartitionType();
    void SendSwitchMsg(SwitchInfoProto *payload, int partition_id = -1);
    void LaunchPartitionMapping(vector<int> required_partition_mapping);

    ProtocolSwitchInfo *protocol_switch_info_;

    TOMulticast *genuine_;

    // AtomicQueue<vector<TxnProto*>> received_operations_;
    // AtomicQueue<TxnProto*> ordered_operations_;

    // The key is the transaction id.
    map<int, TxnProto*> pending_operations_;
    vector<TxnProto*> ready_operations_;
    vector<TxnProto*> executable_operations_;

    // Batch received for a specific round.
    map<int, vector<MessageProto*>> batch_messages_;

    Configuration *configuration_;
    ConnectionMultiplexer *multiplexer_;

    Connection *connection_;
    Connection *sync_connection_;
    Connection *switch_connection_;
    Connection *scheduler_connection_;

    AtomicQueue<MessageProto> *message_queues;

    pthread_t thread_;

    Client *client_;

    int batch_count_;
    // Different from batch_count_ because batch count should provide a inter-partition
    // consensus when the scheduler should only be constant on a partition level
    // (when we switch, we still need a steadly increasing batch_count and not a jump).
    int scheduler_batch_count_;

    bool destructor_invoked_ = false;
    bool started = false;

    int start_time_;

    int switching_done = 0;

    double epoch_start_;
    int max_batch_size_ = atoi(ConfigReader::Value("max_batch_size").c_str());
    double epoch_duration_ = stof(ConfigReader::Value("batch_duration").c_str());

    bool enable_adaptive_switching_ = atoi(ConfigReader::Value("enable_adaptive_switching").c_str());

    std::ofstream order_file_;

    // Count touched partitions to get statistics for optimizing dispatching.
    map<int, int> touched_partitions_count_;
};

#endif
