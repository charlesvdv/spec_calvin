#include "sequencer/to-multicast.h"
#include <fstream>

TOMulticast::TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer) {
    configuration_ = conf;
    multiplexer_ = multiplexer;
    destructor_invoked_ = false;

    pthread_mutex_init(&decided_mutex_, NULL);
    pthread_mutex_init(&clock_mutex_, NULL);
    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);

    // Create custom connection.
    skeen_connection_ = multiplexer_->NewConnection("skeen");

    // Create Paxos instance.
    // Connection *paxos_connection = multiplexer_->NewConnection("tomulticast-paxos");
    // paxos_ = new Paxos(conf->this_group, conf->this_node, paxos_connection,
                       // conf->this_node_partition, conf->num_partitions,
                       // &batch_queue_);

    string filename = "debug-" + std::to_string(configuration_->this_node_id) + ".txt";
    debug_file_.open(filename, std::ios_base::out);
}

TOMulticast::~TOMulticast() {
    destructor_invoked_ = true;
    delete skeen_connection_;

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

    pthread_mutex_lock(&decided_mutex_);
    std::sort(decided_operations_.begin(), decided_operations_.end(), CompareTxn());
    for (auto it = decided_operations_.begin(); it < decided_operations_.end(); ) {
        auto txn = *it;
        if (txn->logical_clock() <= min_pending_clock) {
            delivered.push_back(txn);
            decided_operations_.erase(it);
        } else {
            break;
        }
    }
    // while(!decided_operations_.empty() && !destructor_invoked_) {
        // if (decided_operations_.front()->logical_clock() <= min_pending_clock) {
            // delivered.push_back(decided_operations_.front());
            // decided_operations_.erase(decided_operations_.begin());
        // } else {
            // break;
        // }
        // if (decided_operations_.top()->logical_clock() <= min_pending_clock) {
            // delivered.push_back(decided_operations_.top());
            // decided_operations_.pop();
        // } else {
            // break;
        // }
    // }
    pthread_mutex_unlock(&decided_mutex_);
    return delivered;
}

void TOMulticast::RunThread() {
    Spin(0.5);

    while(!destructor_invoked_) {
        ReceiveMessages();

        pair<TxnProto*, TOMulticastState> txn_info;
        if (pending_operations_.Pop(&txn_info)) {
            auto txn = txn_info.first;
            auto txn_state = txn_info.second;

            // std::cout << "waiting vote: " << waiting_vote_operations_.size() << "\n"
                // << "decided ops: " << decided_operations_.size() << "\n";
            switch(txn_state) {
                case TOMulticastState::WaitingReliableMulticast:
                    DispatchOperationWithReliableMulticast(txn);
                    pending_operations_.Push(
                            std::make_pair(txn, TOMulticastState::GroupTimestamp)
                    );
                    break;
                case TOMulticastState::GroupTimestamp:
                    {
                        IncrementLogicalClock();
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
                            msg.add_data(std::to_string(txn->logical_clock()));
                            msg.set_type(MessageProto::TOMULTICAST_CLOCK_VOTE);
                            msg.set_txn_id(txn->txn_id());
                            skeen_connection_->Send(msg);
                        }
                        UpdateClockVote(txn->txn_id(), configuration_->this_node_partition, txn->logical_clock());
                        waiting_vote_operations_.insert(std::make_pair(txn->txn_id(), txn));
                        break;
                    }
                case TOMulticastState::ReplicaSynchronisation:
                    {
                        IncrementLogicalClock();
                        RunClockSynchronisationConsensus(txn->logical_clock());
                        pthread_mutex_lock(&decided_mutex_);
                        // std::cout << "decided!!!\n";
                        decided_operations_.push_back(txn);
                        pthread_mutex_unlock(&decided_mutex_);
                        break;
                    }
                default:
                    LOG(GetLogicalClock(), "Unknow TO-MULTICAST state!");
            }
        } else {
            Spin(0.01);
        }
    }
}

void TOMulticast::ReceiveMessages() {
    MessageProto rcv_msg;
    while(skeen_connection_->GetMessage(&rcv_msg) && !destructor_invoked_) {
        if (rcv_msg.type() == MessageProto::RMULTICAST_TXN) {
            TxnProto *txn = new TxnProto();
            assert(rcv_msg.data_size() == 1);
            txn->ParseFromString(rcv_msg.data(0));
            pending_operations_.Push(std::make_pair(txn, TOMulticastState::GroupTimestamp));
            // debug_file_ << "txn_id: " << txn->txn_id() << "\n";
        } else if (rcv_msg.type() == MessageProto::TOMULTICAST_CLOCK_VOTE) {
            LogicalClockT partition_vote = std::stoul(rcv_msg.data(0));
            int partition_id = configuration_->NodePartition(rcv_msg.source_node());

            // debug_file_ << "got vote!!" << rcv_msg.txn_id() << "votes state:\n";
            assert(rcv_msg.has_txn_id());
            UpdateClockVote(rcv_msg.txn_id(), partition_id, partition_vote);

            // for (auto vote: clock_votes_) {
                // debug_file_ << "vote txn id " << vote.first << " " << vote.second.size() << "\n";
            // }
            // debug_file_ << "\n";
        } else {
            assert(false);
        }
    }

    // Check if we received all votes for an operation.
    // If we received it, we can assign the final timestamp
    // and pass to state = q2 (Synchronisation consensus).
    for (auto it = waiting_vote_operations_.begin(); it != waiting_vote_operations_.end(); ) {
    // for (auto txn_info: waiting_vote_operations_) {
        int txn_id = (*it).first;
        TxnProto *txn = (*it).second;

        auto search_txn = clock_votes_.find(txn_id);
        if (search_txn != clock_votes_.end()) {
            map<int, LogicalClockT> votes = (*search_txn).second;
            // +1 because GetInvolvedPartitions don't give the current partitition.
            size_t partition_size = GetInvolvedPartitions(txn).size() + 1;
            // We received all the votes for this transaction.
            // debug_file_ << "validation: " << txn_id  << " " << partition_size  << " " << votes.size() << "\n";
            if (partition_size == votes.size()) {
                // debug_file_ << "validated!! " << txn_id << "\n";
                LogicalClockT max_vote = 0;
                for (auto vote: votes) {
                    max_vote = std::max(max_vote, vote.second);
                }

                txn->set_logical_clock(max_vote);

                pending_operations_.Push(
                    make_pair(txn, TOMulticastState::ReplicaSynchronisation)
                );
                clock_votes_.erase(search_txn);
                waiting_vote_operations_.erase(it);
                continue;
            }
        }
        it++;
    }
}

void TOMulticast::UpdateClockVote(int txn_id, int partition_id, LogicalClockT vote) {
    auto txn_votes = clock_votes_.find(txn_id);
    if (txn_votes != clock_votes_.end()) {
        map<int, LogicalClockT> votes = txn_votes->second;
        // Should be the same but we don't have any consensus yet.
        // assert(partition_vote == votes[partition_id])
        votes[partition_id] = std::max(vote, votes[partition_id]);
        txn_votes->second = votes;
    } else {
        map<int, LogicalClockT> votes;
        votes[partition_id] = vote;
        clock_votes_[txn_id] = votes;
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

vector<int> TOMulticast::GetInvolvedPartitions(TxnProto *txn) {
    return Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE, configuration_->this_node_partition);
}

// No longer needs to send data to its groups because protocol above
// handles it.
vector<int> TOMulticast::GetInvolvedNodes(TxnProto *txn) {
    vector<int> nodes;

    auto partitions = GetInvolvedPartitions(txn);
    for (auto part: partitions) {
        auto part_nodes = configuration_->nodes_by_partition[part];
        nodes.insert(nodes.end(), part_nodes.begin(), part_nodes.end());
    }
    auto test = Utils::GetInvolvedPartitions(txn);

    return nodes;
}

LogicalClockT TOMulticast::GetMinimumPendingClock() {
    LogicalClockT min_clock = MAX_CLOCK;

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
            return MAX_CLOCK;
        };
    std::function<LogicalClockT(LogicalClockT, LogicalClockT)> reducer =
        [](LogicalClockT acc, LogicalClockT a) {
            return std::min(acc, a);
        };
    return std::min(min_clock, pending_operations_.Reduce(getter, reducer));
}

// Txn should be replicated in the same time.
LogicalClockT TOMulticast::RunTimestampingConsensus(TxnProto *txn) {
    //Spin(0.1);
    return GetLogicalClock() + (rand() % 15);
}

void TOMulticast::RunClockSynchronisationConsensus(LogicalClockT log_clock) {
    //Spin(0.1);
    SetLogicalClock(log_clock);
}

void TOMulticast::SetLogicalClock(LogicalClockT c) {
    pthread_mutex_lock(&clock_mutex_);
    if (c == MAX_CLOCK) {
        return;
    }
    logical_clock_ = std::max(c, logical_clock_);
    pthread_mutex_unlock(&clock_mutex_);
}

LogicalClockT TOMulticast::GetLogicalClock() {
    pthread_mutex_lock(&clock_mutex_);
    auto c = logical_clock_;
    pthread_mutex_unlock(&clock_mutex_);
    return c;
}

void TOMulticast::IncrementLogicalClock() {
    pthread_mutex_lock(&clock_mutex_);
    logical_clock_++;
    pthread_mutex_unlock(&clock_mutex_);
}

bool TOMulticast::HasTxnForPartition(int partition_id) {
    // Checking `waiting_vote_operations_` queue.
    for (auto txn_info: waiting_vote_operations_) {
        auto parts = Utils::GetInvolvedPartitions(txn_info.second);
        if (std::find(parts.begin(), parts.end(), partition_id) != parts.end()) {
            return true;
        }
    }

    std::function<bool(pair<TxnProto*, TOMulticastState>)> getter =
        [partition_id](pair<TxnProto*, TOMulticastState> a) {
            auto txn = a.first;
            auto parts = Utils::GetInvolvedPartitions(txn);
            if (std::find(parts.begin(), parts.end(), partition_id) != parts.end()) {
                return true;
            }
            return false;
        };
    std::function<bool(bool, bool)> reducer =
        [](bool acc, bool a) {
            return acc || a;
        };

    if (pending_operations_.Reduce(getter, reducer)) {
        return true;
    }

    pthread_mutex_lock(&decided_mutex_);
    for (auto txn: decided_operations_) {
        auto parts = Utils::GetInvolvedPartitions(txn);
        if (std::find(parts.begin(), parts.end(), partition_id) != parts.end()) {
            return true;
        }

    }
    pthread_mutex_unlock(&decided_mutex_);

    return false;
}

TOMulticastSchedulerInterface::TOMulticastSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client) {
    client_ = client;
    configuration_ = conf;

    connection_ = multiplexer->NewConnection("sequencer");

    destructor_invoked_ = false;

    multicast_ = new TOMulticast(conf, multiplexer);
    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunClientHelper, this);
}

TOMulticastSchedulerInterface::~TOMulticastSchedulerInterface() {
    destructor_invoked_ = true;

    // Wait that the main loop is finished before deleting
    // TOMulticast.
    Spin(1);
    delete multicast_;
    delete connection_;
}

void TOMulticastSchedulerInterface::RunClient() {
    int batch_count_ = 0;

    string filename = "order-" + std::to_string(configuration_->this_node_id) + ".txt";
    std::ofstream cache_file(filename, std::ios_base::app);

    while(!destructor_invoked_) {
        for (int i = 0; i < max_batch_size; i++) {
            Spin(0.01);
            int tx_base = configuration_->this_node_id +
                          configuration_->num_partitions * batch_count_;
            int txn_id_offset = i;
            TxnProto *txn;
            client_->GetTxn(&txn, max_batch_size * tx_base + txn_id_offset);
            multicast_->Send(txn);
        }

        std::vector<TxnProto*> decided;
        while (decided.empty()) {
            decided = multicast_->GetDecided();
            Spin(0.001);
        }

        MessageProto msg;
        msg.set_destination_channel("scheduler_");
        msg.set_type(MessageProto::TXN_BATCH);
        msg.set_destination_node(configuration_->this_node_id);
        msg.set_batch_number(batch_count_);
        for (auto it = decided.begin(); it < decided.end(); it++) {
            cache_file << "txn_id: " << (*it)->txn_id() << " log_clock: " << (*it)->logical_clock() << "\n";
            msg.add_data((*it)->SerializeAsString());
        }
        connection_->Send(msg);
        batch_count_++;
    }
}
