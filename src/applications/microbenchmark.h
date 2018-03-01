// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "common/config_reader.h"

using std::set;
using std::string;

class StorageManager;

class Microbenchmark : public Application {
  public:
    enum TxnType { INITIALIZE = 0, MICROTXN_SP = 1, MICROTXN_MP = 2 };

    Microbenchmark(Configuration *config, int partition_count,
                   int partition_id) {
        this->config = config;
        nparts = partition_count;
        this_partition_id = partition_id;
    }

    virtual ~Microbenchmark() {}

    virtual void NewTxn(int64 txn_id, int txn_type,
                        Configuration *config = NULL,
                        PartitionDistribution *distrib = NULL, TxnProto *txn = NULL) const;
    virtual int Execute(TxnProto *txn, StorageManager *storage) const;

    TxnProto *InitializeTxn();
    TxnProto *MicroTxnSP(int64 txn_id, int part);
    TxnProto *MicroTxnMP(int64 txn_id, int *parts, int num_parts);
    TxnProto *MicroTxnDependentSP(int64 txn_id, int part);
    TxnProto *MicroTxnDependentMP(int64 txn_id, int *parts, int num_parts);

    Configuration *config;
    int nparts;
    int hot_records = atoi(ConfigReader::Value("index_size").c_str());
    int index_records = atoi(ConfigReader::Value("index_size").c_str());
    int this_partition_id;
    int kRWSetSize = atoi(ConfigReader::Value("rw_set_size").c_str());
    int indexAccessNum = atoi(ConfigReader::Value("index_num").c_str());
    int kDBSize = atoi(ConfigReader::Value("total_key").c_str());

    virtual void InitializeStorage(Storage *storage, Configuration *conf) const;

  private:
    void GetRandomKeys(set<int> *keys, int num_keys, int key_start,
                       int key_limit, int part);
    inline int RandomLocalKey(const int key_start, const int key_limit,
                              const int part) const {
        return key_start + part +
               nparts * (rand() % ((key_limit - key_start) / nparts));
    }
    inline int NotSoRandomLocalKey(const int64 rand_num, const int key_start,
                                   const int key_limit, const int part) const {
        return key_start + part +
               nparts * (abs(rand_num) % ((key_limit - key_start) / nparts));
    }
    Microbenchmark() {}
};

#endif // _DB_APPLICATIONS_MICROBENCHMARK_H_
