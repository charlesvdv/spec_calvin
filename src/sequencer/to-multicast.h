#ifndef _DB_SEQUENCER_TO_MULTICAST_H_
#define _DB_SEQUENCER_TO_MULTICAST_H_

#include "common/configuration.h"
#include "common/utils.h"
#include "paxos/paxos.h"
#include "proto/txn.pb.h"
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

enum class TOMulticastState: unsigned {
    // state = q0.
    WaitingGroupTimestamp = 1,
    // state = q1.
    WaitingDispatching = 2,
    // state = q2.
    WaitingSynchronisation = 4,
    // state = q3.
    HasFinalTimestamp = 8,
};

class CompareTxn {
public:
    bool operator()(TxnProto *a, TxnProto *b) {
        return a->logical_clock() < b->logical_clock();
    }
};

class TOMulticast {
    public:
        TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer);

        void Send(TxnProto *message);
        vector<TxnProto*> GetDecided();

        ~TOMulticast();
    private:
        void RunClockHandler();

        // Helper to run RunClockHandler inside a thread.
        static void *RunClockHandlerHelper(void *arg) {
            reinterpret_cast<TOMulticast*>(arg)->RunClockHandler();
            return NULL;
        }

        void RunOperationDispatchingHandler();

        // Helper to run RunOperationDispatchingHandler inside a thread.
        static void *RunOperationDispatchingHandlerHelper(void *arg) {
            reinterpret_cast<TOMulticast*>(arg)->RunOperationDispatchingHandler();
            return NULL;
        }

        void DispatchOperationWithReliableMulticast(TxnProto *txn);
        vector<int> GetInvolvedNodes(TxnProto *txn);
        vector<int> GetInvolvedPartitions(TxnProto *txn);

        LogicalClockT GetMinimumPendingClock();

        LogicalClockT RunTimestampingConsensus(TxnProto *txn);
        void RunClockSynchronisationConsensus(LogicalClockT clock);

        void GetOperationAccordingToState(unsigned state, pair<TxnProto*, TOMulticastState> &message_info);

        LogicalClockT logical_clock_;

        // Store transactions that are currently being ordered by the protocol.
        AtomicVector<pair<TxnProto*, TOMulticastState>> pending_operations_;

        // The key is the transaction id
        // Used to save transactions waiting to be receive the vote from
        // all involved partitions.
        map<int, TxnProto*> waiting_vote_operations_;

        // Contains for each transaction id, a map containing
        // the vote of each partition involved in the MPO.
        map<int, map<int, LogicalClockT>> clock_votes_;

        // Store decided operation waiting to be TO-DELIVERED.
        priority_queue<TxnProto*, vector<TxnProto*>, CompareTxn> decided_operations_;

        Configuration *configuration_;
        ConnectionMultiplexer *multiplexer_;

        Connection *skeen_connection_;

        // Ti^(cons) threads.
        pthread_t clock_thread_;
        // Combine Ti^(m) in only one thread.
        pthread_t dispatch_thread_;

        bool destructor_invoked_;

        Paxos *paxos_;
};

#endif
