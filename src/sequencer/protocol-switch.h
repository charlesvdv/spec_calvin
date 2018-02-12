#ifndef SEQUENCER_PROTOCOL_SWITCH_H
#define SEQUENCER_PROTOCOL_SWITCH_H

#include "proto/switch-info.pb.h"

// The actual protocol switch is located at `src/sequencer/custom.cc`.
// Contains information used for a local node to keep information on
// which switching is currently in place.


// Round increase to propose a future round for round switching.
#define SWITCH_ROUND_DELTA 10
#define HYBRID_SYNCED_MAX_DELTA 3

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
    // // Wait for network survey result.
    WAITING_NETWORK_SURVEY,

    MEC_SYNCHRO,
};

class ProtocolSwitchInfo {
public:
    ProtocolSwitchInfo() {  }

    ProtocolSwitchState state;

    // Information about the other partition.
    int partition_id;
    SwitchInfoProto::PartitionType partition_type;

    int switching_round;

    // Flag that informs if a switch request has been executed.
    // bool genuine_executed = false;

    // Informs if MEC is synchronized with the execution (see ProtocolSwitchState::MEC_SYNCHRO).
    bool local_mec_synchro = false;
    bool remote_mec_synchro = false;
};

#endif
