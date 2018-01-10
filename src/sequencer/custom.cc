#include "sequencer/custom.h"

bool SortTxn(TxnProto *a, TxnProto *b) {
    if (a->logical_clock() != b->logical_clock()) {
        return a->logical_clock() < b->logical_clock();
    } else {
        return a->txn_id() < b->txn_id();
    }
}

CustomSequencer::CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer):
    configuration_(conf), multiplexer_(multiplexer) {

    genuine_ = new TOMulticast(conf, multiplexer);
    message_queues = new AtomicQueue<MessageProto>();

    connection_ = multiplexer->NewConnection("calvin", &message_queues);

    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);

    batch_count_ = 0;
}

CustomSequencer::~CustomSequencer() {
    destructor_invoked_ = true;

    delete genuine_;
    delete connection_;
}

void CustomSequencer::OrderTxns(std::vector<TxnProto*> txns) {
    received_operations_.Push(txns);
}

void CustomSequencer::RunThread() {
    Spin(1);

    while(!destructor_invoked_) {
        auto batch = HandleReceivedOperations();
        // -- 1. Replicate batch.
        RunReplicationConsensus(batch);

        // -- 2. Dispatch operations with genuine protocol.
        for (auto &txn: batch) {
            if (!txn->multipartition()) {
                txn->set_logical_clock(0);
                executable_operations_.push_back(txn);
            }
            vector<int> genuine_partition = Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE);
            if (genuine_partition.size() == 0) {
                // TODO: check if process leader.
                genuine_->Send(txn);
                pending_operations_[txn->txn_id()] = txn;
            } else {
                // Calvin only MPO doesn't have any logical clock.
                txn->set_logical_clock(0);
                ready_operations_.push_back(txn);
            }
        }

        // -- 3. Dispatch with low latency protocol.

        // -- 3.1 Get decided operations by genuine protocol.
        auto txn_decided_by_genuine = genuine_->GetDecided();
        for (auto txn: txn_decided_by_genuine) {
            // Save operation in the right queue.
            auto searched_txn = pending_operations_.find(txn->txn_id());
            if (searched_txn == pending_operations_.end()) {
                executable_operations_.push_back(txn);
            } else {
                if (Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY).size() > 0) {
                    ready_operations_.push_back(txn);
                } else {
                    executable_operations_.push_back(txn);
                }
                pending_operations_.erase(searched_txn);
            }
        }

        // -- 3.2 Get max executable clock inside a group.
        // mgec = MAX GROUP EXECUTABLE CLOCK.
        LogicalClockT mec = GetMaxGroupExecutableClock();

        // -- 3.3 Send message with low latency protocol.
        //
        // Used to collect operations by partitions to afterwards send
        // only one message by partitions.
        map<int, vector<TxnProto*>> txn_by_partitions;
        for (auto txn: ready_operations_) {
            for (auto part: Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY)) {
                txn_by_partitions[part].push_back(txn);
            }
            // Add txn inside executable operations.
            executable_operations_.push_back(txn);
        }
        for (auto kv: txn_by_partitions) {
            auto txns = kv.second;
            MessageProto msg;
            msg.set_destination_channel("calvin");
            msg.set_source_node(configuration_->this_node_id);
            msg.set_destination_node(configuration_->PartLocalNode(kv.first));
            msg.set_type(MessageProto::TXN_BATCH);
            msg.set_batch_number(batch_count_);
            msg.set_mec(mec);
            for (auto txn: txns) {
                msg.add_data(txn->SerializeAsString());
            }
            connection_->Send(msg);
        }

        // -- 4. Collect low latency protocol message.
        while (batch_messages_[batch_count_].size() < unsigned(configuration_->num_partitions)) {
            MessageProto *rcv_msg;
            if (!connection_->GetMessage(rcv_msg)) {
                Spin(0.01);
                break;
            }

            assert(rcv_msg->type() == MessageProto::TXN_BATCH);
            batch_messages_[rcv_msg->batch_number()].push_back(rcv_msg);
        }

        // -- 5. Get global max executable clock and receive message inside the execution queue.
        auto max_clock = mec;
        for (auto msg: batch_messages_[batch_count_]) {
            // Calculate global mec.
            mec = std::min(mec, msg->mec());
            max_clock = std::max(max_clock, msg->mec());

            // Add new transaction to the execution queue.
            for (auto txn_str: msg->data()) {
                TxnProto *txn = new TxnProto();
                assert(txn->ParseFromString(txn_str));
                executable_operations_.push_back(txn);
            }
        }
        batch_messages_.erase(batch_messages_.find(batch_count_));

        // -- 6. Update logical clock for terminaison.
        genuine_->SetLogicalClock(max_clock);

        // -- 7. Send executable txn to the scheduler.
        std::sort(executable_operations_.begin(), executable_operations_.end(), SortTxn);
        for (auto it = executable_operations_.begin(); it != executable_operations_.end(); it++) {
            auto txn = *it;
            if (txn->logical_clock() >= mec) {
                executable_operations_.erase(executable_operations_.begin(), it);
                break;
            }
            ordered_operations_.Push(txn);
        }

        batch_count_++;
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
