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

class ReconStorageManager;

class Microbenchmark : public Application {
 public:

  enum TxnType {
	  INITIALIZE=0,
	  MICROTXN_SP=1,
	  MICROTXN_MP=2,
	  MICROTXN_DEP_SP=9,
	  MICROTXN_DEP_MP=10
  };


  Microbenchmark(int nodecount, int node_id) {
    nparts = nodecount;
    this_node_id = node_id;
  }

  virtual ~Microbenchmark() {}

  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config = NULL, TxnProto* txn = NULL) const;
  virtual int Execute(TxnProto* txn, StorageManager* storage) const;
  virtual int ReconExecute(TxnProto* txn, ReconStorageManager* storage) const;

  TxnProto* InitializeTxn();
  TxnProto* MicroTxnSP(int64 txn_id, int part);
  TxnProto* MicroTxnMP(int64 txn_id, int part1, int part2, int part3);
  TxnProto* MicroTxnDependentSP(int64 txn_id, int part);
  TxnProto* MicroTxnDependentMP(int64 txn_id, int part1, int part2, int part3);

  int nparts;
  int hot_records = atoi(ConfigReader::Value("Access", "index_size").c_str());
  int index_records = atoi(ConfigReader::Value("Access", "index_size").c_str());
  int this_node_id;
  int kRWSetSize = atoi(ConfigReader::Value("Access", "rw_set_size").c_str());
  int indexAccessNum = atoi(ConfigReader::Value("Access", "index_num").c_str());
  int kDBSize = atoi(ConfigReader::Value("Access", "total_key").c_str());

  virtual void InitializeStorage(Storage* storage, Configuration* conf) const;

 private:
  void GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part);
  inline int RandomLocalKey(const int key_start, const int key_limit, const int part) const {
		return key_start + part + nparts * (rand() % ((key_limit - key_start)/nparts));
  }
  inline int NotSoRandomLocalKey(const int rand_num, const int key_start, const int key_limit, const int part) const {
		return key_start + part + nparts * (rand_num % ((key_limit - key_start)/nparts));
  }
  Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
