#ifndef SEQUENCER_PROTOCOL_SWITCH_H
#define SEQUENCER_PROTOCOL_SWITCH_H

#include "proto/switch-info.pb.h"

// The actual protocol switch is located at `src/sequencer/custom.cc`.
// Contains information used for a local node to keep information on
// which switching is currently in place.


// Round increase to propose a future round for round switching.
#define SWITCH_ROUND_DELTA 4
#define SWITCH_ROUND_WITH_MAPPING 8
#define HYBRID_SYNCED_MAX_DELTA 1

enum class ProtocolSwitchState {
    // Initialize transaction.
    WAITING_INIT,

    // LOW_LATENCY -> GENUINE

    // Enable transition step.
    SWITCH_TO_GENUINE_TRANSITION,
    // Wait to execute every txn involved with
    // switching partition.
    WAITING_LOW_LATENCY_TXN_EXECUTION,
    // Waiting round vote from the other partitions.
    WAITING_TO_GENUINE_ROUND_VOTE,
    // Waiting round execution.
    SWITCH_TO_GENUINE,

    // GENUINE -> LOW_LATENCY

    // When both genuine are synced hybrid.
    SWITCH_TO_LOW_LATENCY,
    // When one or two partitions is full
    // multicast. Wait for `genuine_executed` flag.
    // WAITING_GENUINE_EXECUTION,
    // // Just wait one round to synchronize mec before
    // // using LOW_LATENCY.
    // WAITING_MEC_SYNCHRO,
    IN_SYNC_WAIT_ROUND_TO_SWITCH,

    // Wait for synchronization of the MEC.
    MEC_SYNCHRO,

    // Wait for network survey result.
    NETWORK_MAPPING,

    // Wait for the other partition to finish its mapping.
    WAIT_NETWORK_MAPPING_RESPONSE,

    // Wait for round vote.
    WAIT_SWITCHING_ROUND_INFO,

    WAIT_ROUND_TO_SWITCH,
};

class ProtocolSwitchInfo {
public:
    ProtocolSwitchInfo() {  }

    ProtocolSwitchState state;

    // Information about the other partition.
    int partition_id = -1;
    SwitchInfoProto::PartitionType partition_type;

    int switching_round = 0;

    int init_round_num = 0;

    // Informs if MEC is synchronized with the execution (see ProtocolSwitchState::MEC_SYNCHRO).
    bool local_mec_synchro = false;
    bool remote_mec_synchro = false;

    // Key: partition id
    // Value: hop count from switching partitions.
    map<int, int> partition_mapping;
    long mapping_id;
    int partition_mapping_response_count = 0;

    // Informs if we finished or not the low latency partition mapping.
    bool local_mapping_finished = false;
    bool remote_mapping_finished = false;

    int final_round = 0;
};

#endif
