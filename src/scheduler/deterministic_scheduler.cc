// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//
// TODO(scw): replace iostream with cstdio

#include "scheduler/deterministic_scheduler.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sched.h>
#include <string>
#include <tr1/unordered_map>
#include <utility>

#include "applications/application.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/connection.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"

#include "../backend/storage_manager.h"
#include "applications/tpcc.h"
#include "scheduler/deterministic_lock_manager.h"

// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF
#include "sequencer/sequencer.h" // COLD_CUTOFF and buffers in LATENCY_TEST

extern LatencyUtils latency_util;

using std::map;
using std::pair;
using std::string;
using std::unordered_map;
using zmq::socket_t;

static void DeleteTxnPtr(void *data, void *hint) { free(data); }

void DeterministicScheduler::SendTxnPtr(socket_t *socket, TxnProto *txn) {
    TxnProto **txn_ptr = reinterpret_cast<TxnProto **>(malloc(sizeof(txn)));
    *txn_ptr = txn;
    zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
    socket->send(msg);
}

TxnProto *DeterministicScheduler::GetTxnPtr(socket_t *socket,
                                            zmq::message_t *msg) {
    if (!socket->recv(msg, ZMQ_NOBLOCK))
        return NULL;
    TxnProto *txn = *reinterpret_cast<TxnProto **>(msg->data());
    return txn;
}

DeterministicScheduler::DeterministicScheduler(
    Configuration *conf, Connection *batch_connection, Storage *storage,
    const Application *application, AtomicQueue<TxnProto *> *input_queue,
    Client *client, int queue_mode, bool independent_mpo)
    : configuration_(conf), batch_connection_(batch_connection),
      storage_(storage), application_(application), to_lock_txns(input_queue),
      client_(client), queue_mode_(queue_mode), committed(0), independent_mpo_(independent_mpo) {

    num_threads = atoi(ConfigReader::Value("num_threads").c_str());
    message_queue = new AtomicQueue<MessageProto>();

    for (int i = 0; i < THROUGHPUT_SIZE; ++i) {
        throughput[i] = -1;
        abort[i] = -1;
    }

    Spin(2);

    pthread_attr_t attr1;
    pthread_attr_init(&attr1);

    // Start all worker threads.
    string channel("execution");
    thread_connection_ = batch_connection_->multiplexer()->NewConnection(
        channel, &message_queue);

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_create(&worker_thread_, &attr, RunWorkerThread,
                   reinterpret_cast<void *>(this));
}

void UnfetchAll(Storage *storage, TxnProto *txn) {
    for (int i = 0; i < txn->read_set_size(); i++)
        if (StringToInt(txn->read_set(i)) > COLD_CUTOFF)
            storage->Unfetch(txn->read_set(i));
    for (int i = 0; i < txn->read_write_set_size(); i++)
        if (StringToInt(txn->read_write_set(i)) > COLD_CUTOFF)
            storage->Unfetch(txn->read_write_set(i));
    for (int i = 0; i < txn->write_set_size(); i++)
        if (StringToInt(txn->write_set(i)) > COLD_CUTOFF)
            storage->Unfetch(txn->write_set(i));
}

// Returns ptr to heap-allocated
unordered_map<int, MessageProto *> batches;
MessageProto *GetBatch(int batch_id, Connection *connection,
                       DeterministicScheduler *scheduler) {
    // LOG(-1, " trying to get"<<batch_id);
    if (batches.count(batch_id) > 0) {
        // Requested batch has already been received.
        MessageProto *batch = batches[batch_id];
        LOG(-1, " got batch " << batch_id);
        batches.erase(batch_id);
        return batch;
    } else {
        MessageProto *message = new MessageProto();
        while (!scheduler->deconstructor_invoked_ &&
               connection->GetMessage(message)) {
            LOG(-1, " got batch " << batch_id);
            assert(message->type() == MessageProto::TXN_BATCH);
            if (message->batch_number() == batch_id) {
                return message;
            } else {
                batches[message->batch_number()] = message;
                message = new MessageProto();
            }
        }
        delete message;
        return NULL;
    }
}

void *DeterministicScheduler::RunWorkerThread(void *arg) {
    DeterministicScheduler *scheduler =
        reinterpret_cast<DeterministicScheduler *>(arg);

    int this_node_partition = scheduler->configuration_->this_node_partition;
    // bool is_recon = false;
    StorageManager *manager;
    TxnProto *txn = NULL;
    map<int64, vector<MessageProto>> buffered_messages;
    queue<TxnProto *> txns_queue;

    MessageProto message;
    MessageProto *batch_message = NULL;
    double time = GetTime();
    int batch_number = 0;
    int second = 0;
    int abort_number = 0;
    int last_committed = 0, now_committed = 0;

    while (!scheduler->deconstructor_invoked_) {
        bool nothing_happened = true;
        if (txn == NULL) {
            if (txns_queue.size()) {
                txn = txns_queue.front();
                txns_queue.pop();
                nothing_happened = false;
                // No remote read result found, start on next txn if one is
                // waiting. Create manager.
                LOG(txn->txn_id(), " starting txn");
                manager = new StorageManager(scheduler->configuration_,
                                             scheduler->thread_connection_,
                                             scheduler->storage_, txn);
                if (scheduler->application_->Execute(txn, manager) == SUCCESS || scheduler->independent_mpo_) {
                    LOG(txn->txn_id(),
                        " finished execution! " << txn->txn_type());
                    if (txn->writers_size() == 0 ||
                        txn->writers(0) == this_node_partition) {
                        latency_util.add_latency((GetUTime() - txn->seed()) /
                                                 1000, txn->writers_size());
                        ++scheduler->committed;
                    }
                    scheduler->client_->GotTxnExecuted(txn->txn_id());
                    delete manager;
                    delete txn;
                    txn = NULL;
                }
            } else {
                if (batch_message) {
                    nothing_happened = false;
                    for (int i = 0; i < batch_message->data_size(); i++) {
                        TxnProto *txn = new TxnProto();
                        txn->ParseFromString(batch_message->data(i));
                        LOG(txn->txn_id(), " adding txn to queue");
                        txns_queue.push(txn);
                    }
                    delete batch_message;
                    batch_message = NULL;
                    batch_number++;
                } else {
                    nothing_happened = false;
                    batch_message = GetBatch(
                        batch_number, scheduler->batch_connection_, scheduler);
                }
            }
        } else if (buffered_messages.count(txn->txn_id()) != 0) {
            nothing_happened = false;
            auto messages = buffered_messages[txn->txn_id()];
            LOG(txn->txn_id(), " received remote read. Executing messages (number): " << messages.size());
            for (auto msg: messages) {
                manager->HandleReadResult(msg);
            }
            buffered_messages.erase(txn->txn_id());
            if (scheduler->application_->Execute(txn, manager) == SUCCESS) {
                LOG(-1, " finished execution for " << txn->txn_id());
                if (txn->writers_size() == 0 ||
                    txn->writers(0) == this_node_partition) {
                    latency_util.add_latency((GetUTime() - txn->seed()) / 1000, txn->writers_size());
                    ++scheduler->committed;
                }
                scheduler->client_->GotTxnExecuted(txn->txn_id());
                delete manager;
                delete txn;
                txn = NULL;
            }
        } else if (scheduler->message_queue->Pop(&message)) {
            // If I get read_result when executing a transaction
            nothing_happened = false;
            LOG(-1, " got READ_RESULT for " << message.txn_id());
            assert(message.type() == MessageProto::READ_RESULT);
            // If we have independent MPOs, we don't care about the result received...
            if (!scheduler->independent_mpo_) {
                buffered_messages[message.txn_id()].push_back(message);
            }
        }

        // Current batch has remaining txns, grab up to 10.

        if (nothing_happened == true)
            Spin(0.001);

        // Report throughput.
        if (GetTime() > time + 1) {
            now_committed = scheduler->committed;
            double total_time = GetTime() - time;
            std::cout << "Completed "
                      << (static_cast<double>(now_committed - last_committed) /
                          total_time)
                      << " txns/sec, " << abort_number
                      << " transaction restart, " << second << "  second \n"
                      << std::flush;

            // Reset txn count.
            scheduler->throughput[second] =
                (static_cast<double>(now_committed - last_committed) /
                 total_time);
            scheduler->abort[second] = abort_number / total_time;
            time = GetTime();
            last_committed = now_committed;
            abort_number = 0;
            second++;
        }
    }
    return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
    delete thread_connection_;
    // pthread_join(lock_manager_thread_, NULL);

    std::cout << "Scheduler deleted" << std::endl;
}

// void* DeterministicScheduler::LockManagerThread(void* arg) {
//	DeterministicScheduler* scheduler =
// reinterpret_cast<DeterministicScheduler*>(arg);
//
//	// Run main loop.
//	MessageProto message;
//	MessageProto* batch_message = NULL;
//	double time = GetTime();
//	int batch_offset = 0;
//	int batch_number = 0;
//	int second = 0;
//	int abort_number = 0;
//	int last_committed = 0, now_committed = 0;
//
//  	while (!scheduler->deconstructor_invoked_) {
//      	// Have we run out of txns in our batch? Let's get some new ones.
//
//  	}
//  	return NULL;
//}
