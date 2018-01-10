#ifndef SEQUENCER_UTILS_H
#define SEQUENCER_UTILS_H

#include "proto/txn.pb.h"
#include <set>

using std::set;

typedef unsigned long LogicalClockT;

class Utils {
public:
    Utils() {}

    static vector<int> GetInvolvedPartitions(TxnProto *txn) {
        set<int> partitions;

        auto readers = txn->readers();
        std::copy(readers.begin(), readers.end(), std::inserter(partitions, partitions.end()));
        auto writers = txn->writers();
        std::copy(writers.begin(), writers.end(), std::inserter(partitions, partitions.end()));

        return vector<int>(partitions.begin(), partitions.end());
    }

    static vector<int> GetPartitionsWithProtocol(TxnProto *txn, TxnProto::ProtocolType type) {
        vector<int> partitions;
        for (auto kv: txn->protocols()) {
        // for (auto it = txn->protocols.begin(); it != txn->protocols().end())
            if (kv.second == type) {
                partitions.push_back(kv.first);
            }
        }
        return partitions;
    }
};

#endif
