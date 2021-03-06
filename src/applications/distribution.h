#ifndef APPLICATIONS_SKEWNESS_H
#define APPLICATIONS_SKEWNESS_H

#include <vector>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <set>
#include <cstdlib>
#include "common/configuration.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "common/config_reader.h"

using std::vector;

class PartitionDistribution {
public:
    virtual ~PartitionDistribution() {}

    virtual vector<int> GetPartitions(unsigned num) = 0;
};

class ZipfianDistribution: public PartitionDistribution {
public:
    ZipfianDistribution(Configuration *conf, double skew=1.0):
            conf_(conf) {
        zipfian_cumul_.push_back(0);

        for (auto i = conf->num_partitions-1; i >= 0; i--) {
            if (conf_->partitions_protocol[i] == TxnProto::LOW_LATENCY) {
                partitions_.insert(partitions_.begin(), i);
            } else {
                partitions_.push_back(i);
            }
        }

        for (auto i = 0; i < conf->num_partitions; i++) {
            double cumul = 1/std::pow(i+1, skew) + zipfian_cumul_[i];
            zipfian_cumul_.push_back(cumul);
        }
        // We now have the harmonic number cumulation.
        auto harmonic_num = zipfian_cumul_[conf->num_partitions];
        for (auto &num: zipfian_cumul_) {
            num = num / harmonic_num;
        }

        srand(time(NULL));
        int low_latency_part_count = conf->GetPartitionProtocolSize(TxnProto::LOW_LATENCY);
        std::random_shuffle(partitions_.begin() + low_latency_part_count, partitions_.end());

        // Includes some warmup times.
        start_time_ = GetTime();
        switching_enabled_ = atoi(ConfigReader::Value("enable_adaptive_switching").c_str());

        for (auto it = conf_->this_node_protocol_switch.begin(); it != conf_->this_node_protocol_switch.begin(); ) {
            auto switch_info = *it;
            if (switch_info.forced) {
                switch_command_.push_back(switch_info);
                conf_->this_node_protocol_switch.erase(it);
            } else {
                it++;
            }
        }
        std::sort(switch_command_.begin(), switch_command_.end());
    }

    vector<int> GetPartitions(unsigned num) {
        set<int> partitions;
        int iter_num = 0;
        while (partitions.size() < num && partitions.size() < partitions_.size()) {
            // srand(time(NULL));
            double random_num = ((double)std::rand())/RAND_MAX;
            int index = BinarySearch(zipfian_cumul_, random_num, 0, zipfian_cumul_.size());
            partitions.insert(partitions_[index]);

            // As we are working with a biaised distribution, we may have a very small
            // chance to have some partitions. If num is high, we may loop for quite some time.
            // Stop before loosing too much time.
            if (iter_num++ > 50) {
                break;
            }
        }

        if (switching_enabled_ && !switch_command_.empty()) {
            if (GetTime() - start_time_ > switch_command_[0].time) {
                auto pos = std::find(partitions_.begin(), partitions_.end(), switch_command_[0].partition_id);
                assert(pos != partitions_.end());
                partitions_.erase(pos);
                partitions_.insert(partitions_.begin(), switch_command_[0].partition_id);
                switch_command_.erase(switch_command_.begin());
            }
            // auto last_partition = partitions_.back();
            // partitions_.pop_back();
            // partitions_.insert(partitions_.begin(), last_partition);
            // switching_time_ = GetTime() + (std::rand() % 5) + 5;
        }
        return vector<int>(partitions.begin(), partitions.end());
    }

private:
    // Return the index of the low interval.
    int BinarySearch(vector<double> arr, double value, int low, int high) {
        int i = (high-low)/2 + low;

        if ((high-low) <= 1) {
            return low;
        } else if (value < arr[i]) {
            high = i;
        } else if (value > arr[i]) {
            low = i;
        }

        return BinarySearch(arr, value, low, high);
    }

    Configuration *conf_;
    vector<int> partitions_;
    // Sum cumulated zipfian distribution.
    vector<double> zipfian_cumul_;
    bool switching_enabled_;
    double start_time_;
    vector<SwitchInfo> switch_command_;
};

class DeterministicDistribution: public PartitionDistribution {
public:
    DeterministicDistribution(Configuration *conf):
        conf_(conf) {}

    vector<int> GetPartitions(unsigned num) {
        set<int> low_latency_partitions;
        for (auto info: conf_->partitions_protocol) {
            if (info.second == TxnProto::LOW_LATENCY) {
                low_latency_partitions.insert(info.first);
            }
        }

        srand(time(NULL));
        while(low_latency_partitions.size() < num && low_latency_partitions.size() < conf_->partitions_protocol.size()) {
            low_latency_partitions.insert(rand() % conf_->num_partitions);
        }

        vector<int> result(low_latency_partitions.begin(), low_latency_partitions.end());
        std::random_shuffle(result.begin(), result.end());
        return result;
    }
private:
    Configuration *conf_;
};

class RandomDistribution: public PartitionDistribution {
public:
    RandomDistribution(Configuration *conf):
        conf_(conf) {}

    vector<int> GetPartitions(unsigned num) {
        set<int> partitions;
        while(partitions.size() < num && partitions.size() < unsigned(conf_->num_partitions)) {
            int partition_id = conf_->RandomPartition();
            if (partition_id != conf_->this_node_partition) {
                partitions.insert(partition_id);
            }
        }
        return vector<int>(partitions.begin(), partitions.end());
    }
private:
    Configuration *conf_;
};

#endif
