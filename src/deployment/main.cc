// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "applications/microbenchmark.h"
//#include "applications/tpcc.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "backend/simple_storage.h"
#include "backend/fetching_storage.h"
#include "backend/collapsed_versioned_storage.h"
//#include "scheduler/serial_scheduler.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"

#define HOT 100

using namespace std;
map<Key, Key> latest_order_id_for_customer;
map<Key, int> latest_order_id_for_district;
map<Key, int> smallest_order_id_for_district;
map<Key, Key> customer_for_order;
unordered_map<Key, int> next_order_id_for_district;
map<Key, int> item_for_order_line;
map<Key, int> order_line_number;

vector<Key>* involed_customers;

pthread_mutex_t mutex_;
pthread_mutex_t mutex_for_item;

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(Configuration* config, int mp)
      : microbenchmark(config->all_nodes.size(), HOT), config_(config),
        percent_mp_(mp) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id, int seed) {
    if (config_->all_nodes.size() > 1 && rand() % 100 < percent_mp_) {
      // Multipartition txn.
      int other1;
      int other2;
      do {
        other1 = rand() % config_->all_nodes.size();
      } while (other1 == config_->this_node_id);

      do {
        other2 = rand() % config_->all_nodes.size();
      } while (other2 == config_->this_node_id || other2 == other1);
      

      *txn = microbenchmark.MicroTxnMP(txn_id, seed, config_->this_node_id, other1, other2);
    } else {
      // Single-partition txn.
      *txn = microbenchmark.MicroTxnSP(txn_id, seed, config_->this_node_id);
    }
  }

 private:
  Microbenchmark microbenchmark;
  Configuration* config_;
  int percent_mp_;
};

class TClient : public Client {
 public:
	TClient(Configuration* config, int mp)
      : microbenchmark(config->all_nodes.size(), HOT), config_(config),
        percent_mp_(mp) {
  }
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id, int seed) {
    if (config_->all_nodes.size() > 1 && rand() % 100 < percent_mp_) {
      // Multipartition txn.
      int other1;
      int other2;
      do {
        other1 = rand() % config_->all_nodes.size();
      } while (other1 == config_->this_node_id);

      do {
        other2 = rand() % config_->all_nodes.size();
      } while (other2 == config_->this_node_id || other2 == other1);


      *txn = microbenchmark.MicroTxnMP(txn_id, config_->this_node_id, seed, other1, other2);
    } else {
      // Single-partition txn.
      *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id, seed);
    }
  }

 private:
  Microbenchmark microbenchmark;
  Configuration* config_;
  int percent_mp_;
};

// TPCC load generation client.
//class TClient : public Client {
// public:
//  TClient(Configuration* config, int mp) : config_(config), percent_mp_(mp) {}
//  virtual ~TClient() {}
//  virtual void GetTxn(TxnProto** txn, int txn_id) {
//    TPCC tpcc;
//    TPCCArgs args;
//
//    args.set_system_time(GetTime());
//    if (rand() % 100 < percent_mp_)
//      args.set_multipartition(true);
//    else
//      args.set_multipartition(false);
//
//    string args_string;
//    args.SerializeToString(&args_string);
//
//    // New order txn
//   int random_txn_type = rand() % 100;
//    // New order txn
//    if (random_txn_type < 45)  {
//      *txn = tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, args_string, config_);
//    } else if(random_txn_type < 88) {
//      *txn = tpcc.NewTxn(txn_id, TPCC::PAYMENT, args_string, config_);
//    } else if(random_txn_type < 92) {
//      *txn = tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, args_string, config_);
//      args.set_multipartition(false);
//    } else if(random_txn_type < 96){
//      *txn = tpcc.NewTxn(txn_id, TPCC::DELIVERY, args_string, config_);
//      args.set_multipartition(false);
//    } else {
//      *txn = tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, args_string, config_);
//      args.set_multipartition(false);
//    }
//
//  }
//
// private:
//  Configuration* config_;
//  int percent_mp_;
//};

void stop(int sig) {
// #ifdef PAXOS
//  StopZookeeper(ZOOKEEPER_CONF);
// #endif
  exit(sig);
}

int main(int argc, char** argv) {
  // TODO(alex): Better arg checking.
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]> <percent_mp>\n",
            argv[0]);
    exit(1);
  }
  bool useFetching = false;
  if (argc > 4 && argv[4][0] == 'f')
    useFetching = true;
  // Catch ^C and kill signals and exit gracefully (for profiling).
  signal(SIGINT, &stop);
  signal(SIGTERM, &stop);

  // Build this node's configuration object.
  Configuration config(StringToInt(argv[1]), "deploy-run.conf");

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);

  // Artificial loadgen clients.
  //Client* client = (argv[2][0] == 'm') ?
  //    reinterpret_cast<Client*>(new MClient(&config, atoi(argv[3]))) :
  //    reinterpret_cast<Client*>(new TClient(&config, atoi(argv[3])));
  Client* client = reinterpret_cast<Client*>(new MClient(&config, atoi(argv[3])));

// #ifdef PAXOS
//  StartZookeeper(ZOOKEEPER_CONF);
// #endif
pthread_mutex_init(&mutex_, NULL);
pthread_mutex_init(&mutex_for_item, NULL);
involed_customers = new vector<Key>;

  Storage* storage;
  if (!useFetching) {
    storage = new SimpleStorage();
  } else {
    storage = FetchingStorage::BuildStorage();
  }
storage->Initmutex();
  //if (argv[2][0] == 'm') {
  //  Microbenchmark(config.all_nodes.size(), HOT).InitializeStorage(storage, &config);
  //} else {
    //TPCC().InitializeStorage(storage, &config);
	 Microbenchmark(config.all_nodes.size(), HOT).InitializeStorage(storage, &config);
  //}

  Connection* batch_connection = multiplexer.NewConnection("scheduler_");
  			// Initialize sequencer component and start sequencer thread running.

  int queue_mode;
  // Run scheduler in main thread.
  if (argv[2][0] == 'm') {
	queue_mode = NORMAL_QUEUE;
	cout << "Normal queue mode" << endl;

  } else if(argv[2][0] == 's'){
	queue_mode = FROM_SELF;
	cout << "Self-generation queue mode" << endl;

  } else if(argv[2][0] == 'd'){
	  queue_mode = FROM_SEQ_SINGLE;
	  cout << "Single-queue by sequencer mode" << endl;

  }  else if(argv[2][0] == 'i'){
	  queue_mode = FROM_SEQ_DIST;
	  cout << "Multiple-queue by sequencer mode" << endl;

  }
  else {
//    DeterministicScheduler scheduler(&config,
//    								 batch_connection,
//                                     storage,
//									 sequencer.GetTxnsQueue(),
//                                     new TPCC());

  }

  Sequencer sequencer(&config, multiplexer.NewConnection("sequencer"), batch_connection,
		  	  client, storage, queue_mode);

  DeterministicScheduler scheduler(&config,
  								 batch_connection,
                                   storage,
									 sequencer.GetTxnsQueue(), client,
                                   new Microbenchmark(config.all_nodes.size(), HOT), queue_mode);

  Spin(180);
  DeterministicScheduler::terminate();
  return 0;
}

