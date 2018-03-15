// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>

#include <deque>
//#define LATENCY_SIZE 10000
//#define SAMPLE_RATE 50
#define THROUGHPUT_SIZE 500

#include "common/configuration.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/scheduler.h"
#include "common/client.h"

using std::deque;
using std::pair;
using std::set;

namespace zmq {
class socket_t;
class message_t;
} // namespace zmq
using zmq::socket_t;

// class Configuration;
class Connection;
class DeterministicLockManager;
class Storage;
class TxnProto;
// class Client;

// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
  public:
    DeterministicScheduler(Configuration *conf, Connection *batch_connection,
                           Storage *storage, const Application *application,
                           AtomicQueue<TxnProto *> *input_queue, Client *client,
                           int queue_mode, bool independent_mpo = false);
    virtual ~DeterministicScheduler();
    void StopRunning() {
        deconstructor_invoked_ = true;
        pthread_join(worker_thread_, NULL);
    }

  private:
    // Function for starting main loops in a separate pthreads.
    static void *RunWorkerThread(void *arg);

    // static void* LockManagerThread(void* arg);

    void SendTxnPtr(socket_t *socket, TxnProto *txn);
    TxnProto *GetTxnPtr(socket_t *socket, zmq::message_t *msg);

    // Configuration specifying node & system settings.
    Configuration *configuration_;

    // Thread contexts and their associated Connection objects.
    pthread_t worker_thread_;
    Connection *thread_connection_;

    // pthread_t lock_manager_thread_;
    // Connection for receiving txn batches from sequencer.
    Connection *batch_connection_;

    // Storage layer used in application execution.
    Storage *storage_;

    // Application currently being run.
    const Application *application_;

    AtomicQueue<TxnProto *> *to_lock_txns;

    // The per-node lock manager tracks what transactions have temporary
    // ownership of what database objects, allowing the scheduler to track LOCAL
    // conflicts and enforce equivalence to transaction orders.
    DeterministicLockManager *lock_manager_;

    // Queue of transaction ids of transactions that have acquired all locks
    // that they have requested.
    std::deque<TxnProto *> *ready_txns_;

    // Sockets for communication between main scheduler thread and worker
    // threads.
    //  socket_t* requests_out_;
    //  socket_t* requests_in_;
    //  socket_t* responses_out_[NUM_THREADS];
    //  socket_t* responses_in_;

    AtomicQueue<MessageProto> *message_queue;

    // Client
    Client *client_;

    int queue_mode_;
    int num_threads;
    int64 committed;
    int pending_txns;
    bool independent_mpo_;

  public:
    bool deconstructor_invoked_ = false;
    double throughput[THROUGHPUT_SIZE];
    double abort[THROUGHPUT_SIZE];
};
#endif // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
