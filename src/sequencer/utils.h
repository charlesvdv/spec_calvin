#ifndef SEQUENCER_UTILS_H
#define SEQUENCER_UTILS_H

#include "proto/txn.pb.h"
#include <set>

using std::set;

typedef unsigned long LogicalClockT;

#define MAX_CLOCK uint64(UINT_MAX) // ULONG_MAX
#define MAX_CLOCK_CMP UINT_MAX

class Utils {
public:
    Utils() {}

    static bool IsReallyMultipartition(TxnProto *txn, int this_partition_id) {
        auto partitions = Utils::GetInvolvedPartitions(txn);
        if (partitions.size() == 1 && partitions[0] == this_partition_id) {
            return false;
        }
        return true;
    }

    static vector<int> GetInvolvedPartitions(TxnProto *txn) {
        set<int> partitions;

        auto readers = txn->readers();
        std::copy(readers.begin(), readers.end(), std::inserter(partitions, partitions.end()));
        auto writers = txn->writers();
        std::copy(writers.begin(), writers.end(), std::inserter(partitions, partitions.end()));

        return vector<int>(partitions.begin(), partitions.end());
    }

    // With protocol given or TRANSITION.
    static vector<int> GetPartitionsWithProtocol(TxnProto *txn, TxnProto::ProtocolType type, int this_partition) {
        vector<int> partitions;
        for (auto kv: txn->protocols()) {
            assert(kv.second != TxnProto::TRANSITION);
            if (kv.second == type && kv.first != this_partition) {
                partitions.push_back(kv.first);
            }
        }
        return partitions;
    }
};

#endif
