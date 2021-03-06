// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.

#ifndef SEQUENCER_CALVIN_H
#define SEQUENCER_CALVIN_H

#include "common/client.h"
#include "common/config_reader.h"
#include "common/configuration.h"
#include "common/utils.h"
#include "paxos/paxos.h"
#include "proto/txn.pb.h"
#include "pthread.h"
#include "scheduler/deterministic_scheduler.h"
#include <atomic>
#include <queue>
#include <set>
#include <string>
#include "sequencer/sequencer.h"

//#define PAXOS
//#define PREFETCHING

//#define MAX_BATCH_SIZE 56

//#define SAMPLES 100000
//#define SAMPLE_RATE 999
//#define VERBOSE_SEQUENCER

//#define LATENCY_TEST

using std::queue;
using std::set;
using std::string;

class Configuration;
class Connection;
class Storage;
class TxnProto;
class MessageProto;
class ConnectionMultiplexer;

#ifdef LATENCY_TEST
extern double sequencer_recv[SAMPLES];
// extern double paxos_begin[SAMPLES];
// extern double paxos_end[SAMPLES];
extern double sequencer_send[SAMPLES];
extern double prefetch_cold[SAMPLES];
extern double scheduler_lock[SAMPLES];
extern double worker_begin[SAMPLES];
extern double worker_end[SAMPLES];
extern double scheduler_unlock[SAMPLES];
#endif

class Sequencer: public AbstractSequencer {
  public:
    // The constructor creates background threads and starts the Sequencer's
    // main loops running.
    Sequencer(Configuration *conf, ConnectionMultiplexer *multiplexer,
              Client *client, Storage *storage, int queue_mode);

    // Halts the main loops.
    ~Sequencer();

    AtomicQueue<TxnProto *> *GetTxnsQueue() { return txns_queue_; }
    void output(DeterministicScheduler *scheduler);

    void WaitForStart() {
        while (!started)
            ;
    }

  private:
    void Synchronize();
    void GenerateLoad(double now, MessageProto &msg);
    // Sequencer's main loops:
    //
    // RunWriter:
    //  while true:
    //    Spend epoch_duration collecting client txn requests into a batch.
    //    Send batch to Paxos service.
    //
    // RunReader:
    //  while true:
    //    Spend epoch_duration collecting client txn requests into a batch.
    //
    // Executes in a background thread created and started by the constructor.
    void RunReader();

    // Functions to start the Multiplexor's main loops, called in new pthreads
    // by the Sequencer's constructor.
    static void *RunSequencerReader(void *arg);

    // Sets '*nodes' to contain the node_id of every node participating in
    // 'txn'.
    void FindParticipatingNodes(const TxnProto &txn, set<int> *nodes);

    // Length of time spent collecting client requests before they are ordered,
    // batched, and sent out to schedulers.
    double epoch_duration_;
    double epoch_start_;
    int batch_count_;

    // Configuration specifying node & system settings.
    Configuration *configuration_;

    // Connection for sending and receiving protocol messages.
    Connection *connection_;

    ConnectionMultiplexer *multiplexer_;

    // Client from which to get incoming txns.
    Client *client_;

    // Pointer to this node's storage object, for prefetching.
    Storage *storage_;

    // Separate pthread contexts in which to run the sequencer's main loops.
    pthread_t reader_thread_;

    // False until the deconstructor is called. As soon as it is set to true,
    // the main loop sees it and stops.
    bool deconstructor_invoked_;

    // Queue for sending batches from writer to reader if not in paxos mode.
    AtomicQueue<MessageProto *> batch_queue_;
    pthread_mutex_t mutex_;

    AtomicQueue<MessageProto> *message_queues;
    AtomicQueue<string> *paxos_queues;

    int max_batch_size = atoi(ConfigReader::Value("max_batch_size").c_str());
    int dependent_percent =
        atoi(ConfigReader::Value("dependent_percent").c_str());
    bool do_paxos;

    int queue_mode_;
    int fetched_txn_num_;

    AtomicQueue<TxnProto *> *txns_queue_;
    bool started = false;
    Paxos *paxos;
};

#endif
