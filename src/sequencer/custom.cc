#include "sequencer/custom.h"

extern LatencyUtils latency_util;

bool SortTxn(TxnProto *a, TxnProto *b) {
    if (a->logical_clock() != b->logical_clock()) {
        return a->logical_clock() < b->logical_clock();
    } else {
        return a->txn_id() < b->txn_id();
    }
}

CustomSequencer::CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer):
    configuration_(conf), multiplexer_(multiplexer) {
    // std::cout << "builded!";

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

inline unsigned int to_uint(char ch)
{
    return static_cast<unsigned int>(static_cast<unsigned char>(ch));
}

void CustomSequencer::RunThread() {
    Spin(1);

    LogicalClockT calvin_clock_value = 0;
    LogicalClockT last_mec = 0;
    LogicalClockT mec = 0;

    while(!destructor_invoked_) {
        // std::cout << "received operation: " << received_operations_.Size() << "\n"
            // << "pending operation: " << pending_operations_.size() << "\n"
            // << "ready operation: " << ready_operations_.size() << "\n"
            // << "executable operation: " << executable_operations_.size() << "\n"
            // << "ordered operation: " << ordered_operations_.Size() << "\n" << std::flush;

        auto txn_decided_by_genuine = genuine_->GetDecided();

        auto batch = HandleReceivedOperations();
        // -- 1. Replicate batch.
        // RunReplicationConsensus(batch);
        mec = RunConsensus(batch, txn_decided_by_genuine);

        for (auto &txn: batch) {
            // std::cout << txn->txn_id() << " multipartition? " << txn->multipartition() << "\n";
            if (!txn->multipartition()) {
                txn->set_logical_clock(0);
                executable_operations_.push_back(txn);
            } else {
                vector<int> genuine_partition = Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE);
                if (genuine_partition.size() != 0) {
                    // TODO: check if process leader.
                    // std::cout << "FAIL!\n";
                    genuine_->Send(txn);
                    pending_operations_[txn->txn_id()] = txn;
                } else {
                    // Calvin only MPO doesn't have any logical clock.
                    txn->set_logical_clock(calvin_clock_value);
                    ready_operations_.push_back(txn);
                }
            }
        }

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

        map<int, vector<TxnProto*>> txn_by_partitions;
        for (auto protocol_part: configuration_->partitions_protocol) {
            if (protocol_part.second == TxnProto::LOW_LATENCY) {
                txn_by_partitions[protocol_part.first];
            }
        }
        for (auto txn: ready_operations_) {
            for (auto part: Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY)) {
                txn_by_partitions[part].push_back(txn);
            }
            // Add txn inside executable operations.
            executable_operations_.push_back(txn);
        }
        ready_operations_.clear();
        for (auto kv: txn_by_partitions) {
            vector<TxnProto*> txns = kv.second;
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

        // -- 7. Send executable txn to the scheduler.
        std::sort(executable_operations_.begin(), executable_operations_.end(), SortTxn);

        bool loop_breaked = false;
        for (auto it = executable_operations_.begin(); it != executable_operations_.end(); it++) {
            auto txn = *it;
            if (txn->logical_clock() >= last_mec) {
                loop_breaked = true;
                executable_operations_.erase(executable_operations_.begin(), it);
                break;
            }
            txn->set_batch_number(batch_count_);
            ordered_operations_.Push(txn);
        }
        if (!loop_breaked) {
            executable_operations_.clear();
        }

        // -- 4. Collect low latency protocol message.
        while (batch_messages_[batch_count_].size() < unsigned(configuration_->num_partitions_low_latency)) {
            MessageProto *rcv_msg = new MessageProto();
            if (connection_->GetMessage(rcv_msg)) {
                assert(rcv_msg->type() == MessageProto::TXN_BATCH);
                batch_messages_[rcv_msg->batch_number()].push_back(rcv_msg);
            } else {
                Spin(0.01);
            }
        }

        // -- 5. Get global max executable clock and receive message inside the execution queue.
        auto max_clock = mec;
        for (auto msg: batch_messages_[batch_count_]) {
            // Calculate global mec.
            mec = std::min(mec, msg->mec());
            // std::cout << "temp mec: " << mec  << "received mec: " << msg->mec() << "\n";
            max_clock = std::max(max_clock, msg->mec());

            // Add new transaction to the execution queue.
            for (int i = 0; i < msg->data_size(); i++) {
                TxnProto *txn = new TxnProto();
                txn->ParseFromString(msg->data(i));
                executable_operations_.push_back(txn);
            }
        }
        batch_messages_.erase(batch_messages_.find(batch_count_));

        calvin_clock_value = max_clock + 1;
        // std::cout << batch_count_ << " " << mec << " " << calvin_clock_value << "\n" << std::flush;
        genuine_->SetLogicalClock(max_clock);

        last_mec = mec;

        batch_count_++;

        // // -- 2. Dispatch operations with genuine protocol.
        // for (auto &txn: batch) {
            // // std::cout << txn->txn_id() << " multipartition? " << txn->multipartition() << "\n";
            // if (!txn->multipartition()) {
                // txn->set_logical_clock(0);
                // executable_operations_.push_back(txn);
            // } else {
                // vector<int> genuine_partition = Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE);
                // if (genuine_partition.size() != 0) {
                    // // TODO: check if process leader.
                    // // std::cout << "FAIL!\n";
                    // genuine_->Send(txn);
                    // pending_operations_[txn->txn_id()] = txn;
                // } else {
                    // // Calvin only MPO doesn't have any logical clock.
                    // txn->set_logical_clock(calvin_clock_value);
                    // ready_operations_.push_back(txn);
                // }
            // }
        // }

        // // -- 3. Dispatch with low latency protocol.

        // // -- 3.1 Get decided operations by genuine protocol.
        // auto txn_decided_by_genuine = genuine_->GetDecided();
        // for (auto txn: txn_decided_by_genuine) {
            // // Save operation in the right queue.
            // auto searched_txn = pending_operations_.find(txn->txn_id());
            // if (searched_txn == pending_operations_.end()) {
                // executable_operations_.push_back(txn);
            // } else {
                // if (Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY).size() > 0) {
                    // ready_operations_.push_back(txn);
                // } else {
                    // executable_operations_.push_back(txn);
                // }
                // pending_operations_.erase(searched_txn);
            // }
        // }

        // // -- 3.2 Get max executable clock inside a group.
        // // mgec = MAX GROUP EXECUTABLE CLOCK.
        // LogicalClockT mec = GetMaxGroupExecutableClock(txn_decided_by_genuine);

        // // -- 3.3 Send message with low latency protocol.
        // //
        // // Used to collect operations by partitions to afterwards send
        // // only one message by partitions.
        // map<int, vector<TxnProto*>> txn_by_partitions;
        // for (auto protocol_part: configuration_->partitions_protocol) {
            // if (protocol_part.second == TxnProto::LOW_LATENCY) {
                // txn_by_partitions[protocol_part.first];
            // }
        // }
        // for (auto txn: ready_operations_) {
            // for (auto part: Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY)) {
                // txn_by_partitions[part].push_back(txn);
            // }
            // // Add txn inside executable operations.
            // executable_operations_.push_back(txn);
        // }
        // // std::cout << mec << "\n" << std::flush;
        // ready_operations_.clear();
        // for (auto kv: txn_by_partitions) {
            // vector<TxnProto*> txns = kv.second;
            // MessageProto msg;
            // msg.set_destination_channel("calvin");
            // msg.set_source_node(configuration_->this_node_id);
            // msg.set_destination_node(configuration_->PartLocalNode(kv.first));
            // msg.set_type(MessageProto::TXN_BATCH);
            // msg.set_batch_number(batch_count_);
            // msg.set_mec(mec);
            // for (auto txn: txns) {
                // msg.add_data(txn->SerializeAsString());
            // }
            // connection_->Send(msg);
        // }
        // // -- 4. Collect low latency protocol message.
        // while (batch_messages_[batch_count_].size() < unsigned(configuration_->num_partitions_low_latency)) {
            // MessageProto *rcv_msg = new MessageProto();
            // if (connection_->GetMessage(rcv_msg)) {
                // assert(rcv_msg->type() == MessageProto::TXN_BATCH);
                // batch_messages_[rcv_msg->batch_number()].push_back(rcv_msg);
            // } else {
                // Spin(0.01);
            // }
        // }

        // // -- 5. Get global max executable clock and receive message inside the execution queue.
        // auto max_clock = mec;
        // for (auto msg: batch_messages_[batch_count_]) {
            // // Calculate global mec.
            // mec = std::min(mec, msg->mec());
            // // std::cout << "temp mec: " << mec  << "received mec: " << msg->mec() << "\n";
            // max_clock = std::max(max_clock, msg->mec());

            // // Add new transaction to the execution queue.
            // for (int i = 0; i < msg->data_size(); i++) {
                // TxnProto *txn = new TxnProto();
                // txn->ParseFromString(msg->data(i));
                // executable_operations_.push_back(txn);
            // }
        // }
        // batch_messages_.erase(batch_messages_.find(batch_count_));


        // // -- 6. Update logical clock for terminaison.
        // calvin_clock_value = max_clock + 1;
        // // std::cout << batch_count_ << " " << mec << " " << calvin_clock_value << "\n" << std::flush;
        // genuine_->SetLogicalClock(max_clock);

        // // -- 7. Send executable txn to the scheduler.
        // std::sort(executable_operations_.begin(), executable_operations_.end(), SortTxn);

        // bool loop_breaked = false;
        // for (auto it = executable_operations_.begin(); it != executable_operations_.end(); it++) {
            // auto txn = *it;
            // if (txn->logical_clock() >= mec) {
                // loop_breaked = true;
                // executable_operations_.erase(executable_operations_.begin(), it);
                // break;
            // }
            // txn->set_batch_number(batch_count_);
            // ordered_operations_.Push(txn);
        // }
        // if (!loop_breaked) {
            // executable_operations_.clear();
        // }

        // batch_count_++;
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
            if (part != configuration_->this_node_partition) {
                // std::cout << "partition " << part << " protocol " << configuration_->partitions_protocol[part] << "\n";
                (*txn->mutable_protocols())[part] = configuration_->partitions_protocol[part];
            }
        }
    }

    return txns;
}

LogicalClockT CustomSequencer::RunConsensus(vector<TxnProto*> batch, vector<TxnProto*> decided_txns) {
    Spin(0.1);
    if (decided_txns.size() != 0) {
        return decided_txns.back()->logical_clock();
    } else {
        return genuine_->GetMaxExecutableClock();
    }
}
// void CustomSequencer::RunReplicationConsensus(vector<TxnProto*> txns) {
    // Spin(0.1);
    // // TODO: delay consensus execution.
    // // for (auto txn: txns) {
        // // operations_.Push(make_pair(txn, CustomSequencerState::REPLICATED));
    // // }
// }

// LogicalClockT CustomSequencer::GetMaxGroupExecutableClock(std::vector<TxnProto*> &txns) {
    // // TODO: consensus on the executable clock.
    // Spin(0.1);
    // if (txns.size() != 0) {
        // return txns.back()->logical_clock();
    // } else {
        // return genuine_->GetMaxExecutableClock();
    // }
// }

void CustomSequencer::output(DeterministicScheduler *scheduler) {
    destructor_invoked_ = true;
    // pthread_join(thread_, NULL);
    ofstream myfile;
    myfile.open(IntToString(configuration_->this_node_id) + "output.txt");
    int count = 0;
    double abort = 0;
    myfile << "THROUGHPUT" << '\n';
    while ((abort = scheduler->abort[count]) != -1 && count < THROUGHPUT_SIZE) {
        myfile << scheduler->throughput[count] << ", " << abort << '\n';
        ++count;
    }

    myfile << "SEP LATENCY" << '\n';
    int avg_lat = latency_util.average_latency();
    // myfile << latency_util.average_sp_latency() << ", "
           // << latency_util.average_mp_latency() << '\n';
    myfile << "LATENCY" << '\n';
    myfile << avg_lat << ", " << latency_util.total_latency << ", "
           << latency_util.total_count << '\n';

    myfile.close();
}

CustomSequencerSchedulerInterface::CustomSequencerSchedulerInterface(Configuration *conf, ConnectionMultiplexer *multiplexer, Client *client) {
    client_ = client;
    configuration_ = conf;

    connection_ = multiplexer->NewConnection("sequencer");

    destructor_invoked_ = false;

    sequencer_ = new CustomSequencer(conf, multiplexer);
    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunClientHelper, this);
}

CustomSequencerSchedulerInterface::~CustomSequencerSchedulerInterface() {
    destructor_invoked_ = true;

    // Wait that the main loop is finished before deleting
    // TOMulticast.
    Spin(1);
    delete sequencer_;
    delete connection_;
}

void CustomSequencerSchedulerInterface::RunClient() {
    int batch_count_ = 0;

    string filename = "order-" + std::to_string(configuration_->this_node_id) + ".txt";
    std::ofstream cache_file(filename, std::ios_base::app);

    while(!destructor_invoked_) {
        vector<TxnProto*> batch;
        for (int i = 0; i < max_batch_size; i++) {
            int tx_base = configuration_->this_node_id +
                          configuration_->num_partitions * batch_count_;
            int txn_id_offset = i;
            TxnProto *txn;
            client_->GetTxn(&txn, max_batch_size * tx_base + txn_id_offset);
            txn->set_multipartition(Utils::IsReallyMultipartition(txn, configuration_->this_node_partition));
            batch.push_back(txn);
        }
        sequencer_->OrderTxns(batch);

        vector<TxnProto*> ordered_txns;
        while(ordered_txns.size() == 0) {
            TxnProto *txn;
            while(sequencer_->GetOrderedTxn(&txn)) {
                ordered_txns.push_back(txn);
            }
            if (ordered_txns.size() == 0) {
                Spin(0.02);
            }
        }

        // std::cout << "!!! batch count: " << batch_count_  << " " << ordered_txns.size() << "\n";

        MessageProto msg;
        msg.set_destination_channel("scheduler_");
        msg.set_type(MessageProto::TXN_BATCH);
        msg.set_destination_node(configuration_->this_node_id);
        msg.set_batch_number(batch_count_);
        for (auto it = ordered_txns.begin(); it < ordered_txns.end(); it++) {
            // cache_file << "txn_id: " << (*it)->txn_id() << " log_clock: " << (*it)->logical_clock() << "\n";
            // Format involved partitions.
            auto nodes = Utils::GetInvolvedPartitions(*it);
            std::ostringstream ss;
            std::copy(nodes.begin(), nodes.end() - 1, std::ostream_iterator<int>(ss, ","));
            ss << nodes.back();

            cache_file << (*it)->txn_id() << ":" << ss.str() << ":" << (*it)->batch_number() << ":" << (*it)->logical_clock() << "\n" << std::flush;
            msg.add_data((*it)->SerializeAsString());
        }
        connection_->Send(msg);

        batch_count_++;
    }
}

void CustomSequencerSchedulerInterface::output(DeterministicScheduler *scheduler) {
    destructor_invoked_ = true;
    sequencer_->output(scheduler);
}
