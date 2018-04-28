#include "sequencer/to-multicast.h"
#include <fstream>

extern LatencyUtils latency_util;

TOMulticast::TOMulticast(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client) {
    configuration_ = conf;
    multiplexer_ = multiplexer;
    standalone_ = client != NULL;
    client_ = client;
    destructor_invoked_ = false;

    pthread_mutex_init(&decided_mutex_, NULL);
    pthread_mutex_init(&clock_mutex_, NULL);
    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);

    // Create custom connection.
    message_queues = new AtomicQueue<MessageProto>();
    skeen_connection_ = multiplexer_->NewConnection("skeen", &message_queues);
    if (standalone_) {
        sync_connection_ = multiplexer_->NewConnection("sync-multicast");
        sequencer_connection_ = multiplexer_->NewConnection("sequencer");
    }

    // Create Paxos instance.
    // Connection *paxos_connection = multiplexer_->NewConnection("tomulticast-paxos");
    // paxos_ = new Paxos(conf->this_group, conf->this_node, paxos_connection,
                       // conf->this_node_partition, conf->num_partitions,
                       // &batch_queue_);

    string filename = "debug-" + std::to_string(configuration_->this_node_id) + ".txt";
    debug_file_.open(filename, std::ios_base::out);

    filename = "order-" + std::to_string(configuration_->this_node_id) + ".txt";
    order_file_.open(filename, std::ios_base::out);
}

TOMulticast::~TOMulticast() {
    destructor_invoked_ = true;

    Spin(0.5);

    debug_file_.close();
    order_file_.close();

    delete skeen_connection_;
    if (standalone_) {
        delete sequencer_connection_;
        // Hack: process lock when sync_connection_ is deleted...
        // delete sync_connection_;
    }
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
        if (txn->logical_clock() < min_pending_clock) {
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
    if (standalone_) {
        Synchronize();
    }

    epoch_start_ = GetTime();
    batch_count_ = -1;
    int scheduler_batch_count_ = 0;

    while(!destructor_invoked_) {
        if (standalone_) {
            ExecuteTxns(scheduler_batch_count_);
            vector<TxnProto*> batch;
            auto pending_size = pending_operations_.Size() + waiting_vote_operations_.size() + decided_operations_.size();
            if (pending_size < 100 && GetBatch(&batch)) {
                std::cout << "pending size: " << pending_size << "\n" << std::flush;
                for (auto txn: batch) {
                    // std::cout << "adding txn!!\n";
                    pending_operations_.Push(std::make_pair(txn, TOMulticastState::WaitingReliableMulticast));
                }
            }
        }
        ReceiveMessages();

        pair<TxnProto*, TOMulticastState> txn_info;
        if (pending_operations_.Pop(&txn_info)) {
            auto txn = txn_info.first;
            auto txn_state = txn_info.second;

            // std::cout << "pending ops: " << pending_operations_.Size() << "\n"
                // << "waiting vote: " << waiting_vote_operations_.size() << "\n"
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
            // Spin(0.001);
        }
    }
}

void TOMulticast::ReceiveMessages() {
    MessageProto rcv_msg;
    while(skeen_connection_->GetMessage(&rcv_msg) && !destructor_invoked_) {
        // std::cout << "got message\n" << std::flush;
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
        // votes[partition_id] = std::max(vote, votes[partition_id]);
        votes[partition_id] = vote;
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
        // std::cout << node << "," << std::flush;
        MessageProto msg;
        msg.set_destination_channel("skeen");
        msg.set_source_node(configuration_->this_node_id);
        msg.set_destination_node(node);
        msg.add_data(txn->SerializeAsString());
        msg.set_type(MessageProto::RMULTICAST_TXN);
        skeen_connection_->Send(msg);
    }
    // std::cout << "\n" << std::flush;
}

vector<int> TOMulticast::GetInvolvedPartitions(TxnProto *txn) {
    if (standalone_) {
        auto partitions = Utils::GetInvolvedPartitions(txn);
        auto this_partition_find = std::find(partitions.begin(), partitions.end(), configuration_->this_node_partition);
        assert(this_partition_find != partitions.end());
        partitions.erase(this_partition_find);
        return partitions;
    } else {
        return Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE, configuration_->this_node_partition);
    }
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
    // auto test = Utils::GetInvolvedPartitions(txn);

    return nodes;
}

LogicalClockT TOMulticast::GetMinimumPendingClock() {
    LogicalClockT min_clock = logical_clock_;

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
    auto this_partition_id = configuration_->this_node_partition;

    // Avoid useless replication.
    std::function<bool(TxnProto *txn)> testLowLatency =
        [partition_id, this_partition_id](TxnProto *txn) {
            auto protocols = txn->protocols();
            if (protocols.find(partition_id) != protocols.end()) {
                if (protocols[partition_id] == TxnProto::LOW_LATENCY && txn->source_partition() == this_partition_id) {
                    return true;
                }
            }
            return false;
        };

    bool hasLowLatency = false;

    // Checking `waiting_vote_operations_` queue.
    for (auto txn_info: waiting_vote_operations_) {
        hasLowLatency = hasLowLatency || testLowLatency(txn_info.second);
    }

    std::function<bool(pair<TxnProto*, TOMulticastState>)> getter =
        [testLowLatency](pair<TxnProto*, TOMulticastState> a) {
            auto test = testLowLatency(a.first);
            // if (test) {
                // std::cout << "test: " << a.first->txn_id() << "\n" << std::flush;
            // }
            return test;
        };
    std::function<bool(bool, bool)> reducer =
        [](bool acc, bool a) {
            return acc || a;
        };
    auto hack = false;
    hasLowLatency = hasLowLatency || pending_operations_.Reduce(getter, reducer, &hack);

    pthread_mutex_lock(&decided_mutex_);
    for (auto txn: decided_operations_) {
        hasLowLatency = hasLowLatency || testLowLatency(txn);
    }
    pthread_mutex_unlock(&decided_mutex_);

    return hasLowLatency;
}

void TOMulticast::Synchronize() {
    MessageProto synchronization_message;
    synchronization_message.set_type(MessageProto::EMPTY);
    synchronization_message.set_destination_channel("sync-multicast");
    for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
        synchronization_message.set_destination_node(i);
        if (i != static_cast<uint32>(configuration_->this_node_id))
            sync_connection_->Send(synchronization_message);
    }
    uint32 synchronization_counter = 1;
    while (synchronization_counter < configuration_->all_nodes.size()) {
        synchronization_message.Clear();
        if (sync_connection_->GetMessage(&synchronization_message)) {
            assert(synchronization_message.type() == MessageProto::EMPTY);
            synchronization_counter++;
        }
    }

    std::cout << "synchronized\n";
    delete sync_connection_;

    started = true;
}

void TOMulticast::output(DeterministicScheduler *scheduler) {
    destructor_invoked_ = true;
    // pthread_join(thread_, NULL);
    ofstream myfile;
    myfile.open(IntToString(configuration_->this_node_id) + "output.txt");
    // int count = 0;
    // double abort = 0;
    myfile << "THROUGHPUT" << '\n';
    // while ((abort = scheduler->abort[count]) != -1 && count < THROUGHPUT_SIZE) {
        // myfile << scheduler->throughput[count] << ", " << abort << '\n';
        // ++count;
    // }

    myfile << "SEP LATENCY" << '\n';
    int avg_lat = latency_util.average_latency();
    myfile << latency_util.average_sp_latency() << ", "
           << latency_util.average_mp_latency() << '\n';
    myfile << "LATENCY" << '\n';
    myfile << avg_lat << ", " << latency_util.total_latency << ", "
           << latency_util.total_count << '\n';

    myfile.close();
}

bool TOMulticast::GetBatch(vector<TxnProto*> *batch) {
    double now = GetTime();
    if (now > epoch_start_ + batch_count_ * epoch_duration_) {
        int txn_id_offset = 0;
        while(!destructor_invoked_ &&
                // now < epoch_start_ + (batch_count_ + 1) * epoch_duration_ &&
                txn_id_offset < max_batch_size_) {
            int tx_base = configuration_->this_node_id +
                          configuration_->num_partitions * batch_count_;
            TxnProto *txn;
            client_->GetTxn(&txn, max_batch_size_ * tx_base + txn_id_offset);
            if (txn == NULL) {
                break;
            }
            txn->set_multipartition(Utils::IsReallyMultipartition(txn, configuration_->this_node_partition));

            // Backup partition protocols inside txn.
            // auto involved_partitions = Utils::GetInvolvedPartitions(txn);
            txn->set_source_partition(configuration_->this_node_partition);
            // for (auto part: involved_partitions) {
                // auto protocol = configuration_->partitions_protocol[part];
                // if (protocol == TxnProto::TRANSITION) {
                    // protocol = TxnProto::GENUINE;
                // }
                // (*txn->mutable_protocols())[part] = protocol;
            // }

            batch->push_back(txn);

            txn_id_offset++;
        }
        batch_count_++;
        return true;
    }
    return false;
}

void TOMulticast::ExecuteTxns(int &batch_count) {
    auto decided = GetDecided();
    if (decided.empty()) {
        return;
    }

    MessageProto msg;
    msg.set_destination_channel("scheduler_");
    msg.set_type(MessageProto::TXN_BATCH);
    msg.set_destination_node(configuration_->this_node_id);
    msg.set_batch_number(batch_count);
    for (auto it = decided.begin(); it < decided.end(); it++) {
        auto involved_partitions = Utils::GetInvolvedPartitions(*it);

        std::ostringstream ss;
        std::copy(involved_partitions.begin(), involved_partitions.end() - 1, std::ostream_iterator<int>(ss, ","));
        ss << involved_partitions.back();

        order_file_ << (*it)->txn_id() << ":" << ss.str() << ":" << (*it)->batch_number()
            << ":" << (*it)->logical_clock() << "\n" << std::flush;
        msg.add_data((*it)->SerializeAsString());
        // cache_file << "txn_id: " << (*it)->txn_id() << " log_clock: " << (*it)->logical_clock() << "\n";
        delete *it;
    }
    sequencer_connection_->Send(msg);

    batch_count++;
}

// TOMulticastSchedulerInterface::TOMulticastSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client) {
    // client_ = client;
    // configuration_ = conf;

    // connection_ = multiplexer->NewConnection("sequencer");

    // destructor_invoked_ = false;

    // multicast_ = new TOMulticast(conf, multiplexer);
    // // Create thread and launch them.
    // pthread_create(&thread_, NULL, RunClientHelper, this);
// }

// TOMulticastSchedulerInterface::~TOMulticastSchedulerInterface() {
    // destructor_invoked_ = true;

    // // Wait that the main loop is finished before deleting
    // // TOMulticast.
    // Spin(1);
    // delete multicast_;
    // delete connection_;
// }

// void TOMulticastSchedulerInterface::RunClient() {
    // int batch_count_ = 0;

    // string filename = "order-" + std::to_string(configuration_->this_node_id) + ".txt";
    // std::ofstream cache_file(filename, std::ios_base::app);

    // while(!destructor_invoked_) {
        // for (int i = 0; i < max_batch_size; i++) {
            // Spin(0.01);
            // int tx_base = configuration_->this_node_id +
                          // configuration_->num_partitions * batch_count_;
            // int txn_id_offset = i;
            // TxnProto *txn;
            // client_->GetTxn(&txn, max_batch_size * tx_base + txn_id_offset);
            // multicast_->Send(txn);
        // }

        // std::vector<TxnProto*> decided;
        // while (decided.empty() && !destructor_invoked_) {
            // decided = multicast_->GetDecided();
            // Spin(0.001);
        // }

        // MessageProto msg;
        // msg.set_destination_channel("scheduler_");
        // msg.set_type(MessageProto::TXN_BATCH);
        // msg.set_destination_node(configuration_->this_node_id);
        // msg.set_batch_number(batch_count_);
        // for (auto it = decided.begin(); it < decided.end(); it++) {
            // cache_file << "txn_id: " << (*it)->txn_id() << " log_clock: " << (*it)->logical_clock() << "\n";
            // msg.add_data((*it)->SerializeAsString());
        // }
        // connection_->Send(msg);
        // batch_count_++;
    // }
// }
