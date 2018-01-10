#include "sequencer/custom.h"

CustomSequencer::CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer):
    configuration_(conf), multiplexer_(multiplexer) {

    genuine_ = new TOMulticast(conf, multiplexer);
    message_queues = new AtomicQueue<MessageProto>();

    connection_ = multiplexer->NewConnection("calvin", &message_queues);

    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);
}

CustomSequencer::~CustomSequencer() {
    destructor_invoked_ = true;

    delete genuine_;
    delete connection_;
}

void CustomSequencer::OrderTxns(std::vector<TxnProto*> txns) {
    received_operations_.Push(txns);
}

// void CustomSequencer::Synchronize() {
    // MessageProto synchronization_message;
    // synchronization_message.set_type(MessageProto::EMPTY);
    // synchronization_message.set_destination_channel("sequencer");
    // for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
        // synchronization_message.set_destination_node(i);
        // if (i != static_cast<uint32>(configuration_->this_node_id))
            // connection_->Send(synchronization_message);
    // }
    // uint32 synchronization_counter = 1;
    // while (synchronization_counter < configuration_->all_nodes.size()) {
        // synchronization_message.Clear();
        // if (connection_->GetMessage(&synchronization_message)) {
            // assert(synchronization_message.type() == MessageProto::EMPTY);
            // synchronization_counter++;
        // }
    // }

    // started = true;
// }

void CustomSequencer::RunThread() {
    Spin(1);

    while(!destructor_invoked_) {
        auto batch = HandleReceivedOperations();
        RunReplicationConsensus(batch);

        // Dispatch operations with genuine protocol.
        for (auto &txn: batch) {
            if (!txn->multipartition()) {
                txn->set_logical_clock(0);
                executable_operations_.push_back(txn);
            }
            vector<int> genuine_partition = Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE);
            if (genuine_partition.size() == 0) {
                // TODO: check if process leader.
                genuine_->Send(txn);
                pending_operations_.push_back(txn);
            } else {
                // Calvin only MPO doesn't have any logical clock.
                txn->set_logical_clock(0);
                ready_operations_.push_back(txn);
            }
        }

        // Used to collect operations by partitions to afterwards send
        // only one message by partitions.
        map<int, vector<TxnProto*>> txn_by_partitions;
        for (auto txn: ready_operations_) {
            for (auto part: Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY)) {
                if (txn_by_partitions.find(part) == txn_by_partitions.end()) {
                    vector<TxnProto*> txns = { txn };
                    txn_by_partitions.insert(std::make_pair(part, txns));
                } else {
                    txn_by_partitions[part].push_back(txn);
                }
            }
        }

        // mgec = MAX GROUP EXECUTABLE CLOCK.
        // auto mgec = GetMaxGroupExecutableClock();
        // for (auto kv: ready_operations_) {
            // MessageProto msg;
            // // msg.set_
        // }
    }
}

vector<TxnProto*> CustomSequencer::HandleReceivedOperations() {
    vector<TxnProto*> txns;
    vector<TxnProto*> tmp_txns;
    while (received_operations_.Pop(&tmp_txns)) {
        txns.insert(txns.end(), tmp_txns.begin(), tmp_txns.end());
    }

    for (auto &txn: txns) {
        // Backup partitions protocols inside the transaction.
        auto involved_partitions = Utils::GetInvolvedPartitions(txn);
        for (auto part: involved_partitions) {
            (*txn->mutable_protocols())[part] = configuration_->partitions_protocol[part];
        }
    }

    return txns;
}

void CustomSequencer::RunReplicationConsensus(vector<TxnProto*> txns) {
    Spin(0.1);
    // TODO: delay consensus execution.
    // for (auto txn: txns) {
        // operations_.Push(make_pair(txn, CustomSequencerState::REPLICATED));
    // }
}

LogicalClockT CustomSequencer::GetMaxGroupExecutableClock() {
    // TODO: consensus on the executable clock.
    Spin(0.1);
    return genuine_->GetMaxExecutableClock();
}
