#ifndef _DB_SEQUENCER_TO_MULTICAST_H_
#define _DB_SEQUENCER_TO_MULTICAST_H_

#include "common/configuration.h"
#include "common/utils.h"
#include "paxos/paxos.h"
#include "proto/txn.pb.h"
#include "sequencer/sequencer.h"
#include <queue>
#include <utility>
#include <vector>
#include "pthread.h"
#include <cstdlib>
#include <set>
#include <string>
#include <functional>
#include <limits>

using std::pair;
using std::vector;
using std::set;
using std::priority_queue;

class Configuration;
class Connection;
class TxnProto;
class ConnectionMultiplexer;

typedef unsigned long LogicalClockT;

enum class TOMulticastState {
    // Wait R-MULTICAST.
    WaitingReliableMulticast,
    // state = q0.
    GroupTimestamp,
    // state = q1.
    InterPartitionVote,
    // state = q2.
    ReplicaSynchronisation,
    // state = q3.
    // WaitingDecide,
};

class CompareTxn {
public:
    bool operator()(TxnProto *a, TxnProto *b) {
        return a->logical_clock() > b->logical_clock();
    }
};

class TOMulticast {
public:
    TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer);

    void Send(TxnProto *message);
    vector<TxnProto*> GetDecided();

    size_t GetPendingQueueSize() {
        return pending_operations_.Size();
    }

    ~TOMulticast();
private:
    void RunThread();

    static void *RunThreadHelper(void *arg) {
        reinterpret_cast<TOMulticast*>(arg)->RunThread();
        return NULL;
    }

    // Receive messages received skeen channel.
    void ReceiveMessages();

    // Helper function to dispatch any transactions with R-MULTICAST.
    void DispatchOperationWithReliableMulticast(TxnProto *txn);

    // Get all nodes involved in a transactions (intra+inter partitions).
    vector<int> GetInvolvedNodes(TxnProto *txn);

    // Get all partitions involved in a transactions.
    vector<int> GetInvolvedPartitions(TxnProto *txn);

    // Get the smallest logical clock transactions which is not yet
    // decided.
    LogicalClockT GetMinimumPendingClock();

    // Helper to run consensus inside a partition.
    LogicalClockT RunTimestampingConsensus(TxnProto *txn);
    void RunClockSynchronisationConsensus(LogicalClockT clock);

    void UpdateClockVote(int txn_id, int partition_id, LogicalClockT vote);

    // Next logical clock that will be assigned.
    LogicalClockT logical_clock_;

    // Store transactions that are currently being ordered by the protocol.
    AtomicQueue<pair<TxnProto*, TOMulticastState>> pending_operations_;

    // The key is the transaction id
    // Used to save transactions waiting to be receive the vote from
    // all involved partitions.
    map<int, TxnProto*> waiting_vote_operations_;

    // Contains for each transaction id, a map containing
    // the vote of each partition involved in the MPO.
    map<int, map<int, LogicalClockT>> clock_votes_;

    // Store decided operation waiting to be TO-DELIVERED.
    priority_queue<TxnProto*, vector<TxnProto*>, CompareTxn> decided_operations_;
    pthread_mutex_t decided_mutex_;

    Configuration *configuration_;
    ConnectionMultiplexer *multiplexer_;

    Connection *skeen_connection_;

    pthread_t thread_;

    bool destructor_invoked_;

    Paxos *paxos_;
};

class TOMulticastSchedulerInterface {
public:
    TOMulticastSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client);
    ~TOMulticastSchedulerInterface();
private:
    void RunClient();

    static void *RunClientHelper(void *arg) {
        reinterpret_cast<TOMulticastSchedulerInterface*>(arg)->RunClient();
        return NULL;
    }

    TOMulticast *multicast_;

    Client *client_;

    Connection *connection_;
    pthread_t thread_;
    bool destructor_invoked_;
    int max_batch_size = atoi(ConfigReader::Value("max_batch_size").c_str());
    Configuration *configuration_;
};

#endif
