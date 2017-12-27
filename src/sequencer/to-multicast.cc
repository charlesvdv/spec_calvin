#include "sequencer/to-multicast.h"

TOMulticast::TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer) {
    configuration_ = conf;
    multiplexer_ = multiplexer_;
    destructor_invoked_ = false;

    message_queues = new AtomicQueue<MessageProto>();

    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);

    // Create custom connection.
    skeen_connection_ = multiplexer_->NewConnection("skeen", &message_queues);

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
    pending_operations_.Push(std::make_pair(txn, TOMulticastState::WaitingReliableMulticast));
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

void TOMulticast::RunThread() {
    while(!destructor_invoked_) {
        ReceiveMessages();

        pair<TxnProto*, TOMulticastState> txn_info;
        if (pending_operations_.Pop(&txn_info)) {
            auto txn = txn_info.first;
            auto txn_state = txn_info.second;

            switch(txn_state) {
                case TOMulticastState::WaitingReliableMulticast:
                    DispatchOperationWithReliableMulticast(txn);
                    pending_operations_.Push(
                            std::make_pair(txn, TOMulticastState::GroupTimestamp)
                    );
                    break;
                case TOMulticastState::GroupTimestamp:
                    {
                        LogicalClockT decided_clock = RunTimestampingConsensus(txn);
                        txn->set_logical_clock(decided_clock);
                        pending_operations_.Push(make_pair(txn, TOMulticastState::InterPartitionVote));
                        break;
                    }
                case TOMulticastState::InterPartitionVote:
                    {
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
                        break;
                    }
                case TOMulticastState::ReplicaSynchronisation:
                    RunClockSynchronisationConsensus(txn->logical_clock());
                    pending_operations_.Push(make_pair(txn, TOMulticastState::WaitingDecide));
                    break;
                default:
                    LOG(logical_clock_, "Unknow TO-MULTICAST state!");
            }
        }
    }
}

void TOMulticast::ReceiveMessages() {
    MessageProto rcv_msg;
    while(skeen_connection_->GetMessage(&rcv_msg) && !destructor_invoked_) {
        if (rcv_msg.type() == MessageProto::RMULTICAST_TXN) {
            TxnProto *txn;
            txn->ParseFromString(rcv_msg.data(0));
            pending_operations_.Push(std::make_pair(txn, TOMulticastState::GroupTimestamp));
        } else if (rcv_msg.type() == MessageProto::TOMULTICAST_CLOCK_VOTE) {
            // TODO: verify if we always have txn_id in a received message or if we need to
            // set it ourself.
            LogicalClockT partition_vote = std::stoul(rcv_msg.data(0));
            int partition_id = configuration_->NodePartition(rcv_msg.source_node());

            assert(rcv_msg.has_txn_id());
            auto txn_votes = clock_votes_.find(rcv_msg.txn_id());
            if (txn_votes != clock_votes_.end()) {
                map<int, LogicalClockT> votes = txn_votes->second;
                votes[partition_id] = partition_vote;
            } else {
                map<int, LogicalClockT> votes;
                votes[partition_id] = partition_vote;
                clock_votes_[(int)rcv_msg.txn_id()] = votes;
            }
        }
    }

    // Check if we received all votes for an operation.
    // If we received it, we can assign the final timestamp
    // and pass to state = q2 (Synchronisation consensus).
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

                pending_operations_.Push(
                    make_pair(txn, TOMulticastState::ReplicaSynchronisation)
                );
                clock_votes_.erase(search_txn);
                waiting_vote_operations_.erase(txn_info.first);
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

    std::function<LogicalClockT(pair<TxnProto*, TOMulticastState>)> getter =
        [](pair<TxnProto*, TOMulticastState> a) {
            auto txn = a.first;
            auto txn_state = a.second;
            if (txn_state == TOMulticastState::InterPartitionVote ||
                    txn_state == TOMulticastState::ReplicaSynchronisation) {
                return txn->logical_clock();
            }
            return std::numeric_limits<LogicalClockT>::max();
        };
    std::function<LogicalClockT(LogicalClockT, LogicalClockT)> reducer =
        [](LogicalClockT acc, LogicalClockT a) {
            return std::min(acc, a);
        };
    return std::min(min_clock, pending_operations_.Reduce(getter, reducer));
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

TOMulticastSchedulerInterface::TOMulticastSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client) {
    multicast_ = new TOMulticast(conf, multiplexer);
    client_ = client;
    configuration_ = conf;

    connection_ = multiplexer->NewConnection("sequencer");

    destructor_invoked_ = false;

    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunClientHelper, this);
}

TOMulticastSchedulerInterface::~TOMulticastSchedulerInterface() {
    delete multicast_;
}

void TOMulticastSchedulerInterface::RunClient() {
    int batch_count_ = 0;
    while(!destructor_invoked_) {
        for (int i = 0; i < 10; i++) {
            int tx_base = configuration_->this_node_id +
                          configuration_->num_partitions * batch_count_;
            int txn_id_offset = i;
            TxnProto *txn;
            client_->GetTxn(&txn, max_batch_size * tx_base + txn_id_offset);
            multicast_->Send(txn);
        }

        auto decided = multicast_->GetDecided();
        for (auto it = decided.begin(); it < decided.end(); it++) {
            MessageProto msg;
            msg.set_destination_channel("scheduler_");
            msg.set_type(MessageProto::TXN_BATCH);
            msg.set_destination_node(configuration_->this_node_id);
            msg.set_batch_number(batch_count_);
            connection_->Send(msg);
        }
        batch_count_++;
    }
}
