// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Author: Kun Ren (kun.ren@yale.edu)
//
// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <unistd.h>

#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "applications/distribution.h"
#include "backend/collapsed_versioned_storage.h"
#include "backend/fetching_storage.h"
#include "backend/simple_storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/tpcc_args.pb.h"
#include "scheduler/deterministic_scheduler.h"
#include "scheduler/serial_scheduler.h"
#include "sequencer/sequencer.h"
#include "sequencer/to-multicast.h"
#include "sequencer/custom.h"
#include "sequencer/calvin.h"
#include "sequencer/genepi.h"

//#define HOT 100

// map<Key, Key> latest_order_id_for_customer;
// map<Key, int> latest_order_id_for_district;
// map<Key, int> smallest_order_id_for_district;
// map<Key, Key> customer_for_order;
// unordered_map<Key, int> next_order_id_for_district;
// map<Key, int> item_for_order_line;
// map<Key, int> order_line_number;
//
// vector<Key>* involed_customers;
//
// pthread_mutex_t mutex_;
// pthread_mutex_t mutex_for_item;

int dependent_percent;
int multi_txn_num_parts;
LatencyUtils latency_util;

// Microbenchmark load generation client.
class MClient : public TxnGetterClient {
  public:
    MClient(Configuration *config, PartitionDistribution *distrib, float mp)
        : microbenchmark(config, config->num_partitions,
                         config->this_node_partition),
          config_(config), distrib_(distrib), percent_mp_(mp * 100) {}
    virtual ~MClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id) {
        if (config_->all_nodes.size() > 1 && rand() % 10000 < percent_mp_) {
            // Multi-partition txn.
            int parts[multi_txn_num_parts];
            parts[0] = config_->this_node_partition;
            auto remote_parts = distrib_->GetPartitions(multi_txn_num_parts-1);
            for (auto i = 1; i < multi_txn_num_parts; i++) {
                assert(unsigned(i) < remote_parts.size()+1);
                parts[i] = remote_parts[i-1];
            }

            *txn =
                microbenchmark.MicroTxnMP(txn_id, parts, multi_txn_num_parts);

            (*txn)->set_multipartition(true);
        } else {
            // Single-partition txn.
            *txn =
                microbenchmark.MicroTxnSP(txn_id, config_->this_node_partition);

            (*txn)->set_multipartition(false);
        }
        int64_t seed = GetUTime();
        // std::cout<<(*txn)->txn_id()<<"txn setting seed "<<seed<<std::endl;
        (*txn)->set_seed(seed);
    }

  private:
    Microbenchmark microbenchmark;
    Configuration *config_;
    PartitionDistribution *distrib_;
    int percent_mp_;
};

// TPCC load generation client.
class TClient : public TxnGetterClient {
  public:
    TClient(Configuration *config, PartitionDistribution *distrib, float mp)
        : config_(config), distrib_(distrib), percent_mp_(mp * 100) {}
    virtual ~TClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id) {
        TPCC tpcc;
        *txn = new TxnProto();

        if (rand() % 10000 < percent_mp_)
            (*txn)->set_multipartition(true);
        else
            (*txn)->set_multipartition(false);

        // New order txn
        int random_txn_type = rand() % 100;
        // New order txn
        if (random_txn_type < 45) {
            tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, distrib_, *txn);
        } else if (random_txn_type < 88) {
            tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, distrib_, *txn);
        } else if (random_txn_type < 92) {
            (*txn)->set_multipartition(false);
            tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, config_, distrib_, *txn);
        } else if (random_txn_type < 96) {
            (*txn)->set_multipartition(false);
            tpcc.NewTxn(txn_id, TPCC::DELIVERY, config_, distrib_, *txn);

        } else {
            (*txn)->set_multipartition(false);
            tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, config_, distrib_, *txn);
        }
    }

  private:
    Configuration *config_;
    PartitionDistribution *distrib_;
    int percent_mp_;
};

class OpenLoopClient: public Client {
  public:
    OpenLoopClient(TxnGetterClient *client):
        client_(client) {}

    virtual ~OpenLoopClient() {}

    virtual void GetTxn(TxnProto **txn, int txn_id) {
        client_->GetTxn(txn, txn_id);
    }

    virtual void GotTxnExecuted(int txn_id) {}
  private:
    TxnGetterClient *client_;
};

class ClosedLoopClient: public Client {
  public:
    ClosedLoopClient(TxnGetterClient *client, int max_txns):
        client_(client), max_txns_(max_txns) {}

    virtual ~ClosedLoopClient() {}

    virtual void GetTxn(TxnProto **txn, int txn_id) {
        if (current_txns_.size() >= max_txns_) {
            *txn = NULL;
            return;
        }
        client_->GetTxn(txn, txn_id);
        current_txns_.insert(txn_id);
    }

    virtual void GotTxnExecuted(int txn_id) {
        current_txns_.erase(txn_id);
    }
  private:
    TxnGetterClient *client_;
    unsigned max_txns_;
    set<int> current_txns_;
};

void stop(int sig) {
    // #ifdef PAXOS
    //  StopZookeeper(ZOOKEEPER_CONF);
    // #endif
    exit(sig);
}

int main(int argc, char **argv) {
    std::cout << "PID: " << getpid() << "\n";
    // TODO(alex): Better arg checking.
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]> <percent_mp>\n",
                argv[0]);
        exit(1);
    }

    // freopen("output.txt","w",stdout);

    ConfigReader::Initialize("myconfig.conf");
    dependent_percent =
        100 * stof(ConfigReader::Value("dependent_percent").c_str());
    multi_txn_num_parts =
        stof(ConfigReader::Value("multi_txn_num_parts").c_str());

    bool useFetching = false;
    if (argc > 4 && argv[4][0] == 'f')
        useFetching = true;
    // Catch ^C and kill signals and exit gracefully (for profiling).
    signal(SIGINT, &stop);
    signal(SIGTERM, &stop);

    // Build this node's configuration object.
    Configuration config(StringToInt(argv[1]), "deploy-run.conf");
    config.InitInfo();

    PartitionDistribution *partition_distribution;
    if (ConfigReader::Value("partition_distribution") == "zipfian") {
        partition_distribution = new ZipfianDistribution(&config, 2);
    } else if (ConfigReader::Value("partition_distribution") == "deterministic") {
        partition_distribution = new DeterministicDistribution(&config);
    } else {
        partition_distribution = new RandomDistribution(&config);
    }

    // Build connection context and start multiplexer thread running.
    ConnectionMultiplexer multiplexer(&config);

    // Artificial loadgen clients.
    TxnGetterClient *getterClient =
        (argv[2][0] == 't')
            ? reinterpret_cast<Client *>(new TClient(
                  &config,
                  partition_distribution,
                  stof(ConfigReader::Value("distribute_percent").c_str())))
            : reinterpret_cast<Client *>(new MClient(
                  &config,
                  partition_distribution,
                  stof(ConfigReader::Value("distribute_percent").c_str())));

    Client *client;
    if (ConfigReader::Value("client_loop_type") == "closed") {
        client = new ClosedLoopClient(getterClient, atoi(ConfigReader::Value("num_closed_loop_client").c_str()));
    } else {
        client = new OpenLoopClient(getterClient);
    }

    Storage *storage;
    if (!useFetching) {
        storage = new SimpleStorage();
    } else {
        storage = FetchingStorage::BuildStorage();
    }
    storage->Initmutex();
    std::cout << "General params: " << std::endl;
    std::cout << "	Distribute txn percent: "
              << ConfigReader::Value("distribute_percent") << std::endl;
    std::cout << "	Dependent txn percent: "
              << ConfigReader::Value("dependent_percent") << std::endl;
    std::cout << "	Max batch size: " << ConfigReader::Value("max_batch_size")
              << std::endl;
    std::cout << "	Num of threads: " << ConfigReader::Value("num_threads")
              << std::endl;

    if (argv[2][0] == 't') {
        std::cout << "TPC-C benchmark. No extra parameters." << std::endl;
        std::cout << "TPC-C benchmark" << std::endl;
        TPCC().InitializeStorage(storage, &config);
    } else if ((argv[2][0] == 'm')) {
        std::cout << "Micro benchmark. Parameters: " << std::endl;
        std::cout << "	Key per txn: " << ConfigReader::Value("rw_set_size")
                  << std::endl;
        std::cout << "	Per partition #keys: "
                  << ConfigReader::Value("total_key")
                  << ", index size: " << ConfigReader::Value("index_size")
                  << ", index num: " << ConfigReader::Value("index_num")
                  << std::endl;
        Microbenchmark(&config, config.num_partitions,
                       config.this_node_partition)
            .InitializeStorage(storage, &config);
    }

    int queue_mode;
    if (argv[2][1] == 'n') {
        queue_mode = NORMAL_QUEUE;
        std::cout << "Normal queue mode" << std::endl;
    } else if (argv[2][1] == 's') {
        queue_mode = SELF_QUEUE;
        std::cout << "Self-generation queue mode" << std::endl;

    } else if (argv[2][1] == 'd') {
        queue_mode = DIRECT_QUEUE;
        std::cout << "Direct queue by sequencer mode" << std::endl;
    }

    // Initialize sequencer component and start sequencer thread running.
    // Sequencer sequencer(&config, &multiplexer, client, storage, queue_mode);
    // TOMulticastSchedulerInterface *multicast = new TOMulticastSchedulerInterface(&config, &multiplexer, client);
    AbstractSequencer *sequencer;
    if (ConfigReader::Value("ordering_layer") == "x") {
        sequencer = new CustomSequencer(&config, &multiplexer, client);
    } else if (ConfigReader::Value("ordering_layer") == "calvin") {
        sequencer = new Sequencer(&config, &multiplexer, client, storage, queue_mode);
    } else if (ConfigReader::Value("ordering_layer") == "genepi") {
        sequencer = new GenepiSequencer(&config, &multiplexer, client, storage, queue_mode);
    } else {
        assert(false);
    }
    // CustomSequencerSchedulerInterface sequencer(&config, &multiplexer, client, enable_adaptive_switching);
    Connection *scheduler_connection = multiplexer.NewConnection("scheduler_");

    bool independent_mpo = atoi(ConfigReader::Value("independent_mpo").c_str());
    AtomicQueue<TxnProto*> txns_queue;
    DeterministicScheduler *scheduler;
    if (argv[2][0] == 't') {
        scheduler = new DeterministicScheduler(
            &config, scheduler_connection, storage, new TPCC(),
            &txns_queue, client, queue_mode, independent_mpo);
        // scheduler = new DeterministicScheduler(
            // &config, scheduler_connection, storage, new TPCC(),
            // sequencer.GetTxnsQueue(), client, queue_mode);
    } else {
        scheduler = new DeterministicScheduler(
            &config, scheduler_connection, storage,
            new Microbenchmark(&config, config.num_partitions,
                               config.this_node_partition),
            &txns_queue, client, queue_mode, independent_mpo);
        // scheduler = new DeterministicScheduler(
            // &config, scheduler_connection, storage,
            // new Microbenchmark(&config, config.num_partitions,
                               // config.this_node_partition),
            // sequencer.GetTxnsQueue(), client, queue_mode);
    }

    sequencer->WaitForStart();
    Spin(atoi(ConfigReader::Value("duration").c_str()));
    scheduler->StopRunning();
    sequencer->output(scheduler);

    delete sequencer;
    delete scheduler;
    delete scheduler_connection;
    delete partition_distribution;

    // delete multicast;
    Spin(1);
    return 0;
}
