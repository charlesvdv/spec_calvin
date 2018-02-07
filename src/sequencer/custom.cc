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
    switch_connection_ = multiplexer->NewConnection("protocol-switch");

    // Create thread and launch them.
    pthread_create(&thread_, NULL, RunThreadHelper, this);

    batch_count_ = 0;

    start_time_ = GetTime();
    protocol_switch_info_ = NULL;
}

CustomSequencer::~CustomSequencer() {
    destructor_invoked_ = true;

    delete genuine_;
    delete connection_;
    delete switch_connection_;
}

void CustomSequencer::OrderTxns(std::vector<TxnProto*> txns) {
    received_operations_.Push(txns);
}

void CustomSequencer::RunThread() {
    Spin(1);

    Synchronize();

    LogicalClockT calvin_clock_value = 0;
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
        // std::cout << batch_count_ << "local mec: " << mec << "\n";

        // -- 2. Dispatch operations with genuine protocol.
        for (auto &txn: batch) {
            // std::cout << txn->txn_id() << " multipartition? " << txn->multipartition() << "\n";
            if (!txn->multipartition()) {
                txn->set_logical_clock(0);
                executable_operations_.push_back(txn);
            } else {
                vector<int> genuine_partition = Utils::GetPartitionsWithProtocol(txn, TxnProto::GENUINE, configuration_->this_node_partition);
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

        // -- 3. Dispatch with low latency protocol.

        // -- 3.1 Get decided operations by genuine protocol.
        // auto txn_decided_by_genuine = genuine_->GetDecided();
        for (auto txn: txn_decided_by_genuine) {
            // Save operation in the right queue.
            auto searched_txn = pending_operations_.find(txn->txn_id());
            if (searched_txn == pending_operations_.end()) {
                executable_operations_.push_back(txn);
            } else {
                if (Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY, configuration_->this_node_partition).size() > 0) {
                    ready_operations_.push_back(txn);
                } else {
                    executable_operations_.push_back(txn);
                }
                pending_operations_.erase(searched_txn);
            }
        }

        // -- 3.2 Get max executable clock inside a group.
        // mgec = MAX GROUP EXECUTABLE CLOCK.
        // LogicalClockT mec = GetMaxGroupExecutableClock(txn_decided_by_genuine);

        // -- 3.3 Send message with low latency protocol.
        //
        // Used to collect operations by partitions to afterwards send
        // only one message by partitions.
        map<int, vector<TxnProto*>> txn_by_partitions;
        for (auto protocol_part: configuration_->partitions_protocol) {
            if (protocol_part.second == TxnProto::LOW_LATENCY) {
                txn_by_partitions[protocol_part.first];
            }
        }
        for (auto txn: ready_operations_) {
            for (auto part: Utils::GetPartitionsWithProtocol(txn, TxnProto::LOW_LATENCY, configuration_->this_node_partition)) {
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
        // -- 4. Collect low latency protocol message.
        while (batch_messages_[batch_count_].size() < unsigned(configuration_->GetPartitionProtocolSize(TxnProto::LOW_LATENCY))) {
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


        // -- 6. Update logical clock for terminaison.
        calvin_clock_value = max_clock + 1;
        // std::cout << batch_count_ << " " << mec << " " << calvin_clock_value << "\n";
        genuine_->SetLogicalClock(max_clock  + 1);

        // -- 7. Send executable txn to the scheduler.
        std::sort(executable_operations_.begin(), executable_operations_.end(), SortTxn);

        bool loop_breaked = false;
        for (auto it = executable_operations_.begin(); it != executable_operations_.end(); it++) {
            auto txn = *it;
            // if (txn->logical_clock() < mec || configuration_->low_latency_exclusive_node) {
            if (txn->logical_clock() < mec) {
                // Big HACK!
                if (txn->txn_purpose() == TxnProto::PROTOCOL_SWITCH) {
                    protocol_switch_info_->genuine_executed = true;
                } else {
                    txn->set_batch_number(batch_count_);
                    ordered_operations_.Push(txn);
                }
            } else {
                loop_breaked = true;
                executable_operations_.erase(executable_operations_.begin(), it);
                break;
            }
        }
        if (!loop_breaked) {
            executable_operations_.clear();
        }

        batch_count_++;

        HandleProtocolSwitch();
    }
}

void CustomSequencer::Synchronize() {
    set<int> node_ids;
    for (auto node: configuration_->all_nodes) {
        if (node.first != configuration_->this_node_id) {
            node_ids.insert(node.first);
        }
    }

    double time = 0;
    while(!node_ids.empty()) {
        if (GetTime() > time + 2) {
            MessageProto msg;
            msg.set_source_node(configuration_->this_node_id);
            msg.set_type(MessageProto::EMPTY);
            msg.set_destination_channel("calvin");
            for (auto node: node_ids) {
                if (node != configuration_->this_node_id) {
                    msg.set_destination_node(node);
                    connection_->Send(msg);
                }
            }
            time = GetTime();
        }

        MessageProto msg;
        while(connection_->GetMessage(&msg)) {
            assert(msg.type() == MessageProto::EMPTY);
            node_ids.erase(msg.source_node());
        }
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
            auto protocol = configuration_->partitions_protocol[part];
            if (protocol == TxnProto::TRANSITION) {
                // TODO.
                protocol = TxnProto::
            }
            (*txn->mutable_protocols())[part] = configuration_->partitions_protocol[part];
        }
    }

    return txns;
}

LogicalClockT CustomSequencer::RunConsensus(vector<TxnProto*> batch, vector<TxnProto*> decided_txns) {
    Spin(0.1);
    auto c = genuine_->GetMaxExecutableClock();
    if (decided_txns.size() != 0) {
        c = std::min(c, decided_txns.back()->logical_clock());
    }
    return c;
}

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

void CustomSequencer::HandleProtocolSwitch() {
    // Partition p is in TRANSITION state and we are waiting that any hybrid MPO requiring
    // genuine dispatching with p has been dispatched.
    if (protocol_switch_info_->state == ProtocolSwitchState::WAITING_LOW_LATENCY_TXN_EXECUTION) {
    }

    // if (protocol_switch_info_->state == ProtocolSwitchState::WAITING_NETWORK_SURVEY) {
    // }

    if (protocol_switch_info_->switching_round == batch_count_) {
        if (protocol_switch_info_->state == ProtocolSwitchState::SWITCH_TO_GENUINE_TRANSITION) {
            configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
            protocol_switch_info_->state = ProtocolSwitchState::WAITING_LOW_LATENCY_TXN_EXECUTION;
        } else if (protocol_switch_info_->state == ProtocolSwitchState::SWITCH_TO_LOW_LATENCY) {
            configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::LOW_LATENCY;

            delete protocol_switch_info_;
            protocol_switch_info_ = NULL;
        }
    }

    if (protocol_switch_info_->state == ProtocolSwitchState::WAITING_MEC_SYNCHRO) {
        assert(configuration_->partitions_protocol[protocol_switch_info_->partition_id] == TxnProto::TRANSITION);
        configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::LOW_LATENCY;

        delete protocol_switch_info_;
        protocol_switch_info_ = NULL;
    }

    if (protocol_switch_info_->genuine_executed) {
        assert(protocol_switch_info_->state == ProtocolSwitchState::WAITING_GENUINE_EXECUTION);
        // Set to TRANSITION to enable low latency message without creating low latency message.
        // This will allows an mec synchronisation to know which clock value should be assigned to
        // low latency only messages. Kind of HACKY.
        configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;

        protocol_switch_info_->state = ProtocolSwitchState::WAITING_MEC_SYNCHRO;
    }

    // Check if we need to initialize a protocol switch.
    // And initialize it.
    if (configuration_->this_node_protocol_switch.size()) {
        auto next_switch = configuration_->this_node_protocol_switch.top();
        if ((GetTime() - start_time_) >= next_switch.second) {
            // TODO: intra-partition replication of switch_info

            int partition_id = next_switch.first;
            SwitchInfoProto switch_info;
            switch_info.set_partition_type(GetPartitionType());
            switch_info.set_current_round(batch_count_);
            switch_info.set_switching_round(batch_count_ + SWITCH_ROUND_DELTA);
            switch_info.set_type(SwitchInfoProto::INIT_MSG);

            // Postpone switching if we are currently in one.
            // otherwise, remove switching info.
            if (protocol_switch_info_ != NULL) {
                next_switch.second += 2;
            } else {
                SendSwitchMsg(&switch_info, partition_id);

                protocol_switch_info_ = new ProtocolSwitchInfo();
                protocol_switch_info_->partition_id = partition_id;
                protocol_switch_info_->state = ProtocolSwitchState::WAITING_INIT;

                configuration_->this_node_protocol_switch.pop();
            }
        }
    }

    MessageProto msg;
    while(switch_connection_->GetMessage(&msg)) {
        assert(msg.type() == MessageProto::SWITCH_PROTOCOL);
        SwitchInfoProto switch_info;
        switch_info.ParseFromString(msg.data(0));
        auto partition_id = configuration_->NodePartition(msg.source_node());

        if (switch_info.type() == SwitchInfoProto::INIT_MSG) {
            auto max_switching_round = std::max(switch_info.switching_round(), batch_count_ + SWITCH_ROUND_DELTA);
            switch_info.set_switching_round(max_switching_round);

            // Check whether this partition is the initializer of this msg.
            auto is_initializer = true;
            if (protocol_switch_info_ == NULL) {
                is_initializer = false;
                protocol_switch_info_ = new ProtocolSwitchInfo();

                SwitchInfoProto response_info = SwitchInfoProto();
                response_info.set_partition_type(GetPartitionType());
                response_info.set_current_round(batch_count_);
                response_info.set_switching_round(max_switching_round);
                response_info.set_type(SwitchInfoProto::INIT_MSG);
                SendSwitchMsg(&response_info);
            } else if (protocol_switch_info_->partition_id != partition_id) {
                SwitchInfoProto info = SwitchInfoProto();
                info.set_type(SwitchInfoProto::ABORT);

                SendSwitchMsg(&info);
                continue;
            }

            // General state.
            protocol_switch_info_->switching_round = switch_info.switching_round();
            protocol_switch_info_->partition_type = switch_info.partition_type();

            // Specific state/action depending on the protocol currently in use (LOW_LATENCY or GENUINE).
            if (configuration_->partitions_protocol[partition_id] == TxnProto::LOW_LATENCY) {
                protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_GENUINE_TRANSITION;
            } else {
                if (GetPartitionType() == SwitchInfoProto::FULL_GENUINE ||
                        switch_info.partition_type() == SwitchInfoProto::FULL_GENUINE) {
                    if (is_initializer) {
                        TxnProto *txn = new TxnProto();
                        txn->set_txn_id(0);
                        txn->set_txn_purpose(TxnProto::PROTOCOL_SWITCH);
                        txn->add_readers(configuration_->this_node_id);
                        txn->add_readers(partition_id);
                        txn->set_multipartition(true);
                        genuine_->Send(txn);
                    }
                    protocol_switch_info_->state = ProtocolSwitchState::WAITING_GENUINE_EXECUTION;
                } else if (std::abs(batch_count_ - switch_info.current_round()) > HYBRID_SYNCED_MAX_DELTA) {
                    // In-sync partition.
                    protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_LOW_LATENCY;
                } else {
                    // Out-of-sync partition.
                    protocol_switch_info_->state = ProtocolSwitchState::WAITING_NETWORK_SURVEY;
                    // TODO: survey network...
                }
            }
        } else if (switch_info.type() == SwitchInfoProto::ABORT) {
            delete protocol_switch_info_;
            protocol_switch_info_ = NULL;

            // Reinsert transitions to retry it.
            configuration_->this_node_protocol_switch.push(std::make_pair(partition_id, GetTime() + 3));
        } else {
            assert(false);
        }
    }
}

void CustomSequencer::SendSwitchMsg(SwitchInfoProto *payload, int partition_id) {
    if (partition_id == -1) {
        assert(protocol_switch_info_ != NULL);
        partition_id = protocol_switch_info_->partition_id;
    }

    MessageProto msg;
    msg.set_source_node(configuration_->this_node_id);
    msg.set_destination_channel("protocol-switch");
    msg.set_destination_node(configuration_->PartLocalNode(partition_id));
    msg.set_type(MessageProto::SWITCH_PROTOCOL);
    msg.add_data(payload->SerializeAsString());
    switch_connection_->Send(msg);
}

SwitchInfoProto::PartitionType CustomSequencer::GetPartitionType() {
    if (configuration_->IsPartitionProtocolExclusive(TxnProto::GENUINE)) {
        return SwitchInfoProto::FULL_GENUINE;
    }
    if (configuration_->IsPartitionProtocolExclusive(TxnProto::LOW_LATENCY)) {
        return SwitchInfoProto::FULL_LOW_LATENCY;
    }
    return SwitchInfoProto::HYBRID;
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
    std::ofstream cache_file(filename, std::ios_base::out);

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
                Spin(0.001);
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
