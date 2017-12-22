#include "sequencer/to-multicast.h"

TOMulticast::TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer) {
    configuration_ = conf;
    multiplexer_ = multiplexer_;
    destructor_invoked_ = false;

    // Create thread and launch them.
    pthread_create(&clock_thread_, NULL, RunClockHandlerHelper, this);
    pthread_create(&dispatch_thread_, NULL, RunOperationDispatchingHandlerHelper, this);

    // Create custom connection.
    skeen_connection_ = multiplexer_->NewConnection("skeen");

    // Create Paxos instance.
    // Connection *paxos_connection = multiplexer_->NewConnection("tomulticast-paxos");
    // paxos_ = new Paxos(conf->this_group, conf->this_node, paxos_connection,
                       // conf->this_node_partition, conf->num_partitions,
                       // &batch_queue_);
}

TOMulticast::~TOMulticast() {
    destructor_invoked_ = true;
    // delete paxos_;
}

void TOMulticast::Send(TxnProto *txn) {
    DispatchOperationWithReliableMulticast(txn);
    pending_operations_.push(std::make_pair(txn, TOMulticastState::WaitingGroupTimestamp));
}

// Get txns which are already decided.
// First txn in the vector is the newest one.
vector<TxnProto*> TOMulticast::GetDecided() {
    vector<TxnProto*> delivered;

    LogicalClockT min_pending_clock = GetMinimumPendingClock();

    while(decided_operations_.top()->logical_clock() <= min_pending_clock) {
        delivered.push_back(decided_operations_.top());
        decided_operations_.pop();
    }
    return delivered;
}

void TOMulticast::RunClockHandler() {
    while(!destructor_invoked_) {
        unsigned state_needed = (unsigned)(TOMulticastState::WaitingGroupTimestamp)
            || (unsigned)(TOMulticastState::WaitingSynchronisation);

        pair<TxnProto*, TOMulticastState> txn_info;
        GetOperationAccordingToState(state_needed, txn_info);
        auto txn = txn_info.first;
        auto txn_state = txn_info.second;

        logical_clock_++;

        // TODO: line 2.5, we assume that the message has been R-DELIVERED
        // inside this partition. But in reality, we need to wait until
        // the txn has been delivered.

        if (txn_state == TOMulticastState::WaitingGroupTimestamp) {
            LogicalClockT decided_clock = RunTimestampingConsensus(txn);
            txn->set_logical_clock(decided_clock);
            pending_operations_.push(make_pair(txn, TOMulticastState::WaitingDispatching));
        } else if (txn_state == TOMulticastState::WaitingSynchronisation) {
            RunClockSynchronisationConsensus(txn->logical_clock());
            pending_operations_.push(make_pair(txn, TOMulticastState::HasFinalTimestamp));
        }
    }
}

void TOMulticast::RunOperationDispatchingHandler() {
    while(!destructor_invoked_) {
        unsigned state_needed = (unsigned)(TOMulticastState::WaitingDispatching)
            || (unsigned)(TOMulticastState::HasFinalTimestamp);

        pair<TxnProto*, TOMulticastState> txn_info;
        GetOperationAccordingToState(state_needed, txn_info);
        auto txn = txn_info.first;
        auto txn_state = txn_info.second;

        if (txn_state == TOMulticastState::WaitingDispatching) {
            vector<int> involved_nodes = GetInvolvedNodes(txn);

            for (int node_id: involved_nodes) {
                MessageProto msg;
                msg.set_destination_channel("skeen");
                msg.set_source_node(configuration_->this_node_id);
                msg.set_destination_node(node_id);
                msg.add_data(reinterpret_cast<const char *>(txn->logical_clock()));
                msg.set_type(MessageProto::TOMULTICAST_CLOCK_VOTE);
                skeen_connection_->Send(msg);
            }
            waiting_vote_operations_.insert(std::make_pair(txn->txn_id(), txn));
        } else if (txn_state == TOMulticastState::HasFinalTimestamp) {
            decided_operations_.push(txn);
        }

        MessageProto rcv_msg;
        while(skeen_connection_->GetMessage(&rcv_msg) && !destructor_invoked_) {
            if (rcv_msg.type() == MessageProto::RMULTICAST_TXN) {
                TxnProto *txn;
                txn->ParseFromString(rcv_msg.data(0));
                pending_operations_.push(std::make_pair(txn, TOMulticastState::WaitingGroupTimestamp));
            } else if (rcv_msg.type() == MessageProto::TOMULTICAST_CLOCK_VOTE) {
                // TODO: verify if we always have txn_id in a received message or if we need to
                // set it ourself.
                assert(rcv_msg.has_txn_id());
                auto search_votes = clock_votes_.find(rcv_msg.txn_id());
                assert(search_votes != clock_votes_.end());

                map<int, LogicalClockT> votes = search_votes->second;

                // LogicalClockT partition_vote = static_cast<LogicalClockT>(rcv_msg.data(0));
                LogicalClockT partition_vote = std::stoul(rcv_msg.data(0));
                int partition_id = configuration_->NodePartition(rcv_msg.source_node());

                votes[partition_id] = partition_vote;
            }
        }

        // Check if we received all votes for an operation.
        // If we received it, we can assign the final timestamp
        // and pass to state = q2.
        for (auto txn_info: waiting_vote_operations_) {
            int txn_id = txn_info.first;
            TxnProto *txn = txn_info.second;

            auto search_txn = clock_votes_.find(txn_id);
            if (search_txn != clock_votes_.end()) {
                map<int, LogicalClockT> votes = (*search_txn).second;
                size_t partition_size = GetInvolvedPartitions(txn).size();
                // We received all the votes for this transaction.
                if (partition_size == votes.size()) {
                    LogicalClockT max_vote = 0;
                    for (auto vote: votes) {
                        max_vote = std::max(max_vote, vote.second);
                    }
                    txn->set_logical_clock(max_vote);

                    pending_operations_.push(make_pair(txn, TOMulticastState::WaitingSynchronisation));
                    clock_votes_.erase(search_txn);
                    waiting_vote_operations_.erase(txn_info.first);
                }
            }
        }
    }
}

void TOMulticast::DispatchOperationWithReliableMulticast(TxnProto *txn) {
    vector<int> involved_nodes = GetInvolvedNodes(txn);

    for (int node: involved_nodes) {
        MessageProto msg;
        msg.set_destination_channel("skeen");
        msg.set_source_node(configuration_->this_node_id);
        msg.set_destination_node(node);
        msg.add_data(txn->SerializeAsString());
        msg.set_type(MessageProto::RMULTICAST_TXN);
        skeen_connection_->Send(msg);
    }
}

vector<int> TOMulticast::GetInvolvedNodes(TxnProto *txn) {
    set<int> involved_nodes;
    for (auto partition_reader: txn->readers()) {
        auto searched_partition = configuration_->nodes_by_partition.find(partition_reader);
        if (searched_partition != configuration_->nodes_by_partition.end()) {
            auto nodes = searched_partition->second;
            std::copy(nodes.begin(), nodes.end(), std::inserter(involved_nodes, involved_nodes.end()));
        } else {
            assert(true);
        }
    }
    for (auto partition_writer: txn->writers()) {
        auto searched_partition = configuration_->nodes_by_partition.find(partition_writer);
        if (searched_partition != configuration_->nodes_by_partition.end()) {
            auto nodes = searched_partition->second;
            std::copy(nodes.begin(), nodes.end(), std::inserter(involved_nodes, involved_nodes.end()));
        } else {
            assert(true);
        }
    }
    for (auto node: configuration_->this_group) {
        if (node->node_id != configuration_->this_node_id) {
            involved_nodes.insert(node->node_id);
        }
    }
    return vector<int>(involved_nodes.begin(), involved_nodes.end());
}

vector<int> TOMulticast::GetInvolvedPartitions(TxnProto *txn) {
    set<int> partitions;

    auto readers = txn->readers();
    std::copy(readers.begin(), readers.end(), std::inserter(partitions, partitions.end()));
    auto writers = txn->writers();
    std::copy(writers.begin(), writers.end(), std::inserter(partitions, partitions.end()));

    return vector<int>(partitions.begin(), partitions.end());
}

LogicalClockT TOMulticast::GetMinimumPendingClock() {
    LogicalClockT min_clock = std::numeric_limits<LogicalClockT>::max();

    // Checking `waiting_vote_operations_` queue.
    for (auto txn_info: waiting_vote_operations_) {
        min_clock = min(txn_info.second->logical_clock(), min_clock);
    }

    // Checking `pending_operations_` queue.
    unsigned state_filter = (unsigned)(TOMulticastState::WaitingDispatching)
        || (unsigned)(TOMulticastState::WaitingSynchronisation);

    pending_operations_.lock_read();
    for (auto it = pending_operations_.unsafe_begin(); it < pending_operations_.unsafe_end(); it++) {
        auto txn_info = *it;
        if (((unsigned)txn_info.second & state_filter) > 0) {
            min_clock = min(txn_info.first->logical_clock(), min_clock);
        }
    }
    pending_operations_.unlock();

    return min_clock;
}

void TOMulticast::GetOperationAccordingToState(unsigned state, pair<TxnProto*, TOMulticastState> &txn_info) {
    while(true && !destructor_invoked_) {
        pending_operations_.lock_write();
        for (auto it = pending_operations_.unsafe_begin(); it < pending_operations_.unsafe_end(); it++) {
            txn_info = *it;
            if (((unsigned)txn_info.second & state) > 0) {
                pending_operations_.unsafe_erase(it);
                pending_operations_.unlock();
                return;
            }
        }
        pending_operations_.unlock();
        Spin(0.2);
    }
}

// Txn should be replicated in the same time.
LogicalClockT TOMulticast::RunTimestampingConsensus(TxnProto *txn) {
    // Taken from the genepi papers (tc).
    Spin(3);
    return logical_clock_ + (rand() % 15);
}

void TOMulticast::RunClockSynchronisationConsensus(LogicalClockT log_clock) {
    // Taken from the genepi papers (tc).
    Spin(3);
    logical_clock_ = std::max(logical_clock_, log_clock);
}
