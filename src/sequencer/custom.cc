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

    std::cout << "switching done: " << switching_done << "\n";

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
        // std::cout << "round: " << batch_count_ << "\n"
            // << "received operation: " << received_operations_.Size() << "\n"
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
            if (protocol_part.second == TxnProto::LOW_LATENCY || protocol_part.second == TxnProto::TRANSITION) {
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
        int partition_num = configuration_->GetPartitionProtocolSize(TxnProto::LOW_LATENCY) +
            configuration_->GetPartitionProtocolSize(TxnProto::TRANSITION);
        // std::cout << "partition num: " << partition_num << "\n"
                  // << "msg sent: " << txn_by_partitions.size() << "\n"
                  // << "round number: " << batch_count_ << "\n" << std::flush;
        while (batch_messages_[batch_count_].size() < unsigned(partition_num) && !destructor_invoked_) {
            MessageProto *rcv_msg = new MessageProto();
            // std::cout << "waiting...\n" << std::flush;
            if (connection_->GetMessage(rcv_msg)) {
                // We can maybe have some synchronization left.
                if (rcv_msg->type() == MessageProto::EMPTY) {
                    continue;
                }
                assert(rcv_msg->type() == MessageProto::TXN_BATCH);
                batch_messages_[rcv_msg->batch_number()].push_back(rcv_msg);
            } else {
                // Spin(0.01);
                Spin(0.5);
                // partition_num = configuration_->GetPartitionProtocolSize(TxnProto::LOW_LATENCY) +
                    // configuration_->GetPartitionProtocolSize(TxnProto::TRANSITION);
            }
        }
        // std::cout << "okokok\n" << std::flush;

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
        bool got_txns_executed = false;
        for (auto it = executable_operations_.begin(); it != executable_operations_.end(); it++) {
            auto txn = *it;
            // if (txn->logical_clock() < mec || configuration_->low_latency_exclusive_node) {
            if (txn->logical_clock() < mec) {
                txn->set_batch_number(batch_count_);
                ordered_operations_.Push(txn);
                got_txns_executed = true;
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

        HandleProtocolSwitch(got_txns_executed);
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
        txn->set_source_partition(configuration_->this_node_partition);
        for (auto part: involved_partitions) {
            auto protocol = configuration_->partitions_protocol[part];
            if (protocol == TxnProto::TRANSITION) {
                protocol = TxnProto::GENUINE;
            }
            (*txn->mutable_protocols())[part] = protocol;
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

// Doesn't currently support replications. It should be straightforward to do so but
// I don't have time to implement it properly.
void CustomSequencer::HandleProtocolSwitch(bool got_txns_executed) {
    // Check if we can update state of the protocol.
    if (protocol_switch_info_ != NULL) {
        std::cout << static_cast<int>(protocol_switch_info_->state)
            << " " << protocol_switch_info_->partition_id << "\n" << std::flush;

        // Partition p is in TRANSITION state and we are waiting that any hybrid MPO requiring
        // genuine dispatching with p has been dispatched.
        if (protocol_switch_info_->state == ProtocolSwitchState::WAITING_LOW_LATENCY_TXN_EXECUTION) {
            bool hasStillTxns = genuine_->HasTxnForPartition(protocol_switch_info_->partition_id);

            for (auto txn: executable_operations_) {
                auto protocols = txn->protocols();
                if (protocols.find(protocol_switch_info_->partition_id) != protocols.end()) {
                    auto protocol = protocols[protocol_switch_info_->partition_id];
                    if (protocol == TxnProto::LOW_LATENCY && txn->source_partition() == configuration_->this_node_partition) {
                        hasStillTxns = true;
                    }
                }
            }

            if (!hasStillTxns) {
                auto switching_round = batch_count_ + SWITCH_ROUND_DELTA;
                SwitchInfoProto switch_info = SwitchInfoProto();
                switch_info.set_partition_type(GetPartitionType());
                switch_info.set_current_round(batch_count_);
                switch_info.set_switching_round(switching_round);
                switch_info.set_type(SwitchInfoProto::GENUINE_SWITCH_ROUND_VOTE);

                SendSwitchMsg(&switch_info, protocol_switch_info_->partition_id);

                if (protocol_switch_info_->switching_round != 0) {
                    protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_GENUINE;
                } else {
                    protocol_switch_info_->state = ProtocolSwitchState::WAITING_TO_GENUINE_ROUND_VOTE;
                }
                protocol_switch_info_->switching_round =
                    std::max(protocol_switch_info_->switching_round, switching_round);
            }
        }

        // Some transactions got executed which means transaction execution is synchronized with the MEC.
        if (protocol_switch_info_->state == ProtocolSwitchState::MEC_SYNCHRO && got_txns_executed) {
            if (!protocol_switch_info_->local_mec_synchro) {
                SwitchInfoProto switch_info = SwitchInfoProto();
                switch_info.set_type(SwitchInfoProto::MEC_SYNCHRONIZED);
                SendSwitchMsg(&switch_info, protocol_switch_info_->partition_id);
            }

            protocol_switch_info_->local_mec_synchro = true;

            if (protocol_switch_info_->local_mec_synchro && protocol_switch_info_->remote_mec_synchro) {
                std::cout << "okokok\n" << std::flush;
                protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_LOW_LATENCY;
            }
        }

        // We can switch to low latency.
        if (protocol_switch_info_ != NULL && protocol_switch_info_->state == ProtocolSwitchState::SWITCH_TO_LOW_LATENCY) {
            switching_done += 1;
            configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::LOW_LATENCY;

            std::cout << "switched to low latency, current round: " << batch_count_ << "\n" << std::flush;
            switching_done += 1;
            delete protocol_switch_info_;
            protocol_switch_info_ = NULL;
        }

        if (protocol_switch_info_ != NULL && protocol_switch_info_->switching_round == batch_count_) {
            if (protocol_switch_info_->state == ProtocolSwitchState::SWITCH_TO_GENUINE_TRANSITION) {
                configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                protocol_switch_info_->state = ProtocolSwitchState::WAITING_LOW_LATENCY_TXN_EXECUTION;
                protocol_switch_info_->switching_round = 0;
            } else if (protocol_switch_info_->state == ProtocolSwitchState::SWITCH_TO_GENUINE) {
                configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::GENUINE;

                switching_done += 1;
                std::cout << "switched to genuine, current round: " << batch_count_ << "\n" << std::flush;
                delete protocol_switch_info_;
                protocol_switch_info_ = NULL;
            } else if (protocol_switch_info_->state == ProtocolSwitchState::WAIT_ROUND_TO_SWITCH) {
                batch_count_ = protocol_switch_info_->final_round;

                // Only update partition which need to switch from protocols and not every partitions inside
                // low latency mapping.
                if (protocol_switch_info_->partition_id != -1) {
                    // Force one round with no calvin partition to sync MEC.
                    std::cout << "current round: " << batch_count_ << "\n" << std::flush;
                    configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                    protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_LOW_LATENCY;
                } else {
                    std::cout << "do nothing, current round: " << batch_count_ << "\n" << std::flush;
                    delete protocol_switch_info_;
                    protocol_switch_info_ = NULL;
                }
            } else if (protocol_switch_info_->state == ProtocolSwitchState::IN_SYNC_WAIT_ROUND_TO_SWITCH) {
                configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_LOW_LATENCY;

                // std::cout << "current round: " << batch_count_ << "\n" << std::flush;
                // delete protocol_switch_info_;
                // protocol_switch_info_ = NULL;
            }
        }
    }

    // Check if we need to initialize a protocol switch.
    // And initialize it.
    if (configuration_->this_node_protocol_switch.size()) {
        auto next_switch = configuration_->this_node_protocol_switch.top();
        if ((GetTime() - start_time_) >= next_switch.time) {
            // TODO: intra-partition replication of switch_info

            // Postpone switching if we are currently in one.
            // otherwise, remove switching info.
            if (protocol_switch_info_ != NULL) {
                next_switch.time += (GetTime() - start_time_) + 1 + (rand() % 5);
            } else if (next_switch.protocol == configuration_->partitions_protocol[next_switch.partition_id]) {
                // We are already in the good protocol...
                configuration_->this_node_protocol_switch.pop();
            } else {
                SwitchInfoProto switch_info;
                switch_info.set_partition_type(GetPartitionType());
                switch_info.set_current_round(batch_count_);
                switch_info.set_switching_round(batch_count_ + SWITCH_ROUND_DELTA);
                switch_info.set_type(SwitchInfoProto::INIT_MSG);
                SendSwitchMsg(&switch_info, next_switch.partition_id);
                std::cout << "dispatched!! " << next_switch.partition_id << "\n"  << std::flush;

                protocol_switch_info_ = new ProtocolSwitchInfo();
                protocol_switch_info_->partition_id = next_switch.partition_id;
                protocol_switch_info_->state = ProtocolSwitchState::WAITING_INIT;
                protocol_switch_info_->init_round_num = batch_count_;
                protocol_switch_info_->protocol = next_switch.protocol;
                protocol_switch_info_->initiator = true;

                configuration_->this_node_protocol_switch.pop();
            }
        }
    }

    // Flags used for genuine -> low latency unsynchronized.

    // Map low latency partition.
    bool launch_switching_round_propagation = false;

    // Handle message.
    MessageProto msg;
    while(switch_connection_->GetMessage(&msg)) {
        assert(msg.type() == MessageProto::SWITCH_PROTOCOL);
        SwitchInfoProto switch_info;
        switch_info.ParseFromString(msg.data(0));
        auto partition_id = configuration_->NodePartition(msg.source_node());

        std::cout << "From: " << partition_id << "\n" << switch_info.DebugString() << "\n" << std::flush;
        if (protocol_switch_info_ == NULL && !(switch_info.type() == SwitchInfoProto::INIT_MSG ||
                    switch_info.type() == SwitchInfoProto::LOW_LATENCY_MAPPING_REQUEST)) {
            // Pass msg since we can't do anything with it.
            continue;
        }

        if (switch_info.type() == SwitchInfoProto::INIT_MSG) {
            // Check whether this partition is the initializer of this msg.
            if (protocol_switch_info_ == NULL) {
                auto max_switching_round = std::max(switch_info.switching_round(), batch_count_ + SWITCH_ROUND_DELTA);
                switch_info.set_switching_round(max_switching_round);

                protocol_switch_info_ = new ProtocolSwitchInfo();
                protocol_switch_info_->partition_id = partition_id;
                protocol_switch_info_->init_round_num = batch_count_;

                SwitchInfoProto response_info = SwitchInfoProto();
                response_info.set_partition_type(GetPartitionType());
                response_info.set_current_round(batch_count_);
                response_info.set_switching_round(max_switching_round);
                response_info.set_type(SwitchInfoProto::INIT_MSG);
                SendSwitchMsg(&response_info, partition_id);
            } else if (protocol_switch_info_->partition_id != partition_id) {
                SwitchInfoProto info = SwitchInfoProto();
                info.set_type(SwitchInfoProto::ABORT);

                SendSwitchMsg(&info, partition_id);
                continue;
            }

            // General state.
            protocol_switch_info_->switching_round = switch_info.switching_round();
            protocol_switch_info_->partition_type = switch_info.partition_type();

            std::cout << "switching round" << protocol_switch_info_->switching_round << "\n" << std::flush;
            // Specific state/action depending on the protocol currently in use (LOW_LATENCY or GENUINE).
            if (configuration_->partitions_protocol[partition_id] == TxnProto::LOW_LATENCY) {
                protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_GENUINE_TRANSITION;
            } else {
                if (GetPartitionType() == SwitchInfoProto::FULL_GENUINE &&
                        switch_info.partition_type() == SwitchInfoProto::FULL_GENUINE) {
                    // We can directly switch without any loss in consistency.
                    batch_count_ = protocol_switch_info_->switching_round;
                    configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                    protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_LOW_LATENCY;
                } else if (GetPartitionType() == SwitchInfoProto::FULL_GENUINE) {
                    assert(switch_info.partition_type() == SwitchInfoProto::HYBRID);

                    configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                    protocol_switch_info_->state = ProtocolSwitchState::MEC_SYNCHRO;

                    // Wait for the first low latency message to synchronize round number.
                    while (!destructor_invoked_) {
                        MessageProto *rcv_msg = new MessageProto();
                        if (connection_->GetMessage(rcv_msg)) {
                            assert(rcv_msg->type() == MessageProto::TXN_BATCH);
                            batch_messages_[rcv_msg->batch_number()].push_back(rcv_msg);
                            if (configuration_->NodePartition(rcv_msg->source_node()) == protocol_switch_info_->partition_id) {
                                batch_count_ = rcv_msg->batch_number();
                                break;
                            }
                        } else {
                            Spin(0.02);
                        }
                    }
                } else if (switch_info.partition_type() == SwitchInfoProto::FULL_GENUINE) {
                    assert(GetPartitionType() == SwitchInfoProto::HYBRID);
                    configuration_->partitions_protocol[protocol_switch_info_->partition_id] = TxnProto::TRANSITION;
                    protocol_switch_info_->state = ProtocolSwitchState::MEC_SYNCHRO;
                } else {
                    // std::cout << "out-of-sync !!!\n" << std::flush;
                    // Out-of-sync partition.

                    protocol_switch_info_->state = ProtocolSwitchState::NETWORK_MAPPING;
                    protocol_switch_info_->is_mapping_leader = true;

                    vector<int> partition_mapping_required;
                    for (auto protocol: configuration_->partitions_protocol) {
                        assert(protocol.second != TxnProto::TRANSITION);
                        if (protocol.second == TxnProto::LOW_LATENCY) {
                            partition_mapping_required.push_back(protocol.first);
                        }
                    }

                    // Generate unique id from the two partition id involved in the switch.
                    auto this_partition_id = configuration_->this_node_partition;
                    auto other_partition_id = protocol_switch_info_->partition_id;
                    int low = (this_partition_id < other_partition_id ? this_partition_id : other_partition_id);
                    int high = (this_partition_id < other_partition_id ? other_partition_id : this_partition_id);
                    protocol_switch_info_->mapping_id = (((long)high) << 32) + (long)low;
                    protocol_switch_info_->switching_round = 0;

                    LaunchPartitionMapping(partition_mapping_required);
                }
            }
            std::cout << "init round num: " << batch_count_ << "\n" << std::flush;
        } else if (switch_info.type() == SwitchInfoProto::ABORT) {
            if (switch_info.has_mapping_id() && switch_info.mapping_id() != protocol_switch_info_->mapping_id) {
                continue;
            }
            // Really bad if it's happening because we should only be in the mapping
            // process and not already decided.
            assert(protocol_switch_info_->state != ProtocolSwitchState::WAIT_ROUND_TO_SWITCH);
            std::cout << configuration_->this_node_partition << " aborded!\n" << std::flush;

            if (protocol_switch_info_->initiator) {
                // Reinsert transitions to retry it.
                configuration_->this_node_protocol_switch.push(
                    SwitchInfo((GetTime() + 3 + (rand() % 5)) - start_time_, protocol_switch_info_->partition_id, protocol_switch_info_->protocol)
                );
            }

            if (protocol_switch_info_->is_mapping_leader) {
                SwitchInfoProto abort_msg = SwitchInfoProto();
                abort_msg.set_type(SwitchInfoProto::ABORT);
                abort_msg.set_mapping_id(protocol_switch_info_->mapping_id);

                for (auto part: protocol_switch_info_->partitions_request_send) {
                    SendSwitchMsg(&abort_msg, part);
                }
                SendSwitchMsg(&abort_msg, protocol_switch_info_->partition_id);
            }

            delete protocol_switch_info_;
            protocol_switch_info_ = NULL;
        } else if (switch_info.type() == SwitchInfoProto::GENUINE_SWITCH_ROUND_VOTE) {
            auto switching_round = switch_info.switching_round();
            protocol_switch_info_->switching_round =
                std::max(protocol_switch_info_->switching_round, switching_round);
            if (protocol_switch_info_->switching_round != 0) {
                protocol_switch_info_->state = ProtocolSwitchState::SWITCH_TO_GENUINE;
            } else {
                protocol_switch_info_->state = ProtocolSwitchState::WAITING_TO_GENUINE_ROUND_VOTE;
            }
        } else if (switch_info.type() == SwitchInfoProto::MEC_SYNCHRONIZED) {
            protocol_switch_info_->remote_mec_synchro = true;
        } else if (switch_info.type() == SwitchInfoProto::LOW_LATENCY_MAPPING_REQUEST) {
            SwitchInfoProto response_msg = SwitchInfoProto();
            response_msg.set_mapping_already_handled(true);
            if (protocol_switch_info_ == NULL) {
                protocol_switch_info_ = new ProtocolSwitchInfo();
                protocol_switch_info_->state = ProtocolSwitchState::NETWORK_MAPPING;
                protocol_switch_info_->mapping_id = switch_info.mapping_id();
                response_msg.set_mapping_already_handled(false);
            } else if (protocol_switch_info_->mapping_id != switch_info.mapping_id()) {
                // Send an abort to the node which is sending us a message.
                SwitchInfoProto abort_msg = SwitchInfoProto();
                abort_msg.set_type(SwitchInfoProto::ABORT);
                abort_msg.set_mapping_id(switch_info.mapping_id());
                SendSwitchMsg(&abort_msg, partition_id);
                continue;
            }

            response_msg.set_type(SwitchInfoProto::LOW_LATENCY_MAPPING_RESPONSE);
            response_msg.set_mapping_id(protocol_switch_info_->mapping_id);
            if (protocol_switch_info_->state == ProtocolSwitchState::WAIT_NETWORK_MAPPING_RESPONSE &&
                    protocol_switch_info_->is_mapping_leader) {
                SendSwitchMsg(&response_msg, partition_id);
                continue;
            }
            assert(protocol_switch_info_->state == ProtocolSwitchState::NETWORK_MAPPING);

            for (auto protocol: configuration_->partitions_protocol) {
                assert(protocol.second != TxnProto::TRANSITION);
                int partition_id = protocol.first;
                if (protocol.second == TxnProto::LOW_LATENCY) {
                    response_msg.add_neighbours_partition(partition_id);
                }
            }

            SendSwitchMsg(&response_msg, partition_id);
        } else if (switch_info.type() == SwitchInfoProto::LOW_LATENCY_MAPPING_RESPONSE) {
            // assert(switch_info.mapping_id() == protocol_switch_info_->mapping_id);
            if (switch_info.mapping_id() != protocol_switch_info_->mapping_id) {
                continue;
            }
            assert(protocol_switch_info_->is_mapping_leader);

            if (switch_info.mapping_already_handled()) {
                protocol_switch_info_->partitions_request_send.erase(partition_id);
                continue;
            } else {
                protocol_switch_info_->partitions_response_received.insert(partition_id);
            }

            vector<int> partition_mapping_required;
            for (auto part: switch_info.neighbours_partition()) {
                if (protocol_switch_info_->partitions_request_send.count(part) == 0
                        && partition_id != configuration_->this_node_partition
                        && partition_id != protocol_switch_info_->partition_id) {
                    partition_mapping_required.push_back(part);
                }
            }

            LaunchPartitionMapping(partition_mapping_required);
        } else if (switch_info.type() == SwitchInfoProto::LOW_LATENCY_MAPPING_FINISHED) {
            protocol_switch_info_->remote_mapping_finished = true;
        } else if (switch_info.type() == SwitchInfoProto::LOW_LATENCY_ROUND_VOTE) {
            if (protocol_switch_info_->is_mapping_leader) {
                // We need to wait that we have the local and remote info before launching propagation.
                if (protocol_switch_info_->final_round != 0) {
                    launch_switching_round_propagation = true;
                }
            } else {
                protocol_switch_info_->state = ProtocolSwitchState::WAIT_ROUND_TO_SWITCH;
            }
            protocol_switch_info_->final_round = std::max(
                protocol_switch_info_->final_round,
                switch_info.final_round()
            );
            protocol_switch_info_->switching_round = std::max(
                protocol_switch_info_->switching_round,
                switch_info.switching_round()
            );
        } else {
            assert(false);
        }
    }

    // We have finished our mapping.
    if (protocol_switch_info_ != NULL && protocol_switch_info_->state == ProtocolSwitchState::NETWORK_MAPPING &&
            protocol_switch_info_->partitions_request_send.size() == protocol_switch_info_->partitions_response_received.size() &&
            protocol_switch_info_->is_mapping_leader) {
        protocol_switch_info_->state = ProtocolSwitchState::WAIT_NETWORK_MAPPING_RESPONSE;
        protocol_switch_info_->local_mapping_finished = true;

        SwitchInfoProto switch_info = SwitchInfoProto();
        switch_info.set_type(SwitchInfoProto::LOW_LATENCY_MAPPING_FINISHED);
        SendSwitchMsg(&switch_info, protocol_switch_info_->partition_id);
    }

    if (protocol_switch_info_ != NULL && protocol_switch_info_->state == ProtocolSwitchState::WAIT_NETWORK_MAPPING_RESPONSE &&
            protocol_switch_info_->local_mapping_finished && protocol_switch_info_->remote_mapping_finished) {
        if (protocol_switch_info_->final_round != 0) {
            launch_switching_round_propagation = true;
        }

        protocol_switch_info_->switching_round = std::max(
            protocol_switch_info_->switching_round,
            batch_count_ + SWITCH_ROUND_DELTA
        );
        protocol_switch_info_->final_round = std::max(
            protocol_switch_info_->final_round,
            batch_count_ + SWITCH_ROUND_WITH_MAPPING
        );
        SwitchInfoProto switch_info = SwitchInfoProto();
        switch_info.set_type(SwitchInfoProto::LOW_LATENCY_ROUND_VOTE);
        switch_info.set_switching_round(protocol_switch_info_->switching_round);
        switch_info.set_final_round(protocol_switch_info_->final_round);
        SendSwitchMsg(&switch_info, protocol_switch_info_->partition_id);

        // Avoid resending things...
        protocol_switch_info_->state = ProtocolSwitchState::WAIT_SWITCHING_ROUND_INFO;
    }

    if (launch_switching_round_propagation) {
        std::cout << "launch propagation!! " << protocol_switch_info_->final_round << "\n";
        protocol_switch_info_->state = ProtocolSwitchState::WAIT_ROUND_TO_SWITCH;

        SwitchInfoProto switch_info = SwitchInfoProto();
        switch_info.set_type(SwitchInfoProto::LOW_LATENCY_ROUND_VOTE);
        switch_info.set_final_round(protocol_switch_info_->final_round);
        switch_info.set_switching_round(protocol_switch_info_->switching_round);
        for (auto part_info: protocol_switch_info_->partitions_response_received) {
            SendSwitchMsg(&switch_info, part_info);
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

void CustomSequencer::LaunchPartitionMapping(vector<int> partition_mapping_required) {
    SwitchInfoProto switch_info = SwitchInfoProto();
    switch_info.set_type(SwitchInfoProto::LOW_LATENCY_MAPPING_REQUEST);
    switch_info.set_mapping_id(protocol_switch_info_->mapping_id);

    for (auto part_info: partition_mapping_required) {
        SendSwitchMsg(&switch_info, part_info);
        protocol_switch_info_->partitions_request_send.insert(part_info);
    }
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

CustomSequencerSchedulerInterface::CustomSequencerSchedulerInterface(Configuration *conf,
        ConnectionMultiplexer *multiplexer, Client *client, bool enable_adaptive_switching) {
    client_ = client;
    configuration_ = conf;

    connection_ = multiplexer->NewConnection("sequencer");

    destructor_invoked_ = false;

    enable_adaptive_switching_ = enable_adaptive_switching;
    std::cout << "adaptive switching: " << enable_adaptive_switching_ << "\n";

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

    // Variables used for automatic switching.
    const int TIME_DELTA = 5;
    map<int, int> partitions_count;
    int num_round = 0;
    // int next_round = ROUND_DELTA;
    int start_time_ = GetTime() + TIME_DELTA;

    std::function<bool(std::pair<int, int>, std::pair<int, int>)> compFunc =
        [](std::pair<int, int> a, std::pair<int, int> b) {
            return a.second < b.second;
        };

    while(!destructor_invoked_) {
        vector<TxnProto*> batch;
        for (int i = 0; i < max_batch_size; i++) {
            int tx_base = configuration_->this_node_id +
                          configuration_->num_partitions * batch_count_;
            int txn_id_offset = i;
            TxnProto *txn;
            client_->GetTxn(&txn, max_batch_size * tx_base + txn_id_offset);
            if (txn == NULL) {
                break;
            }
            txn->set_multipartition(Utils::IsReallyMultipartition(txn, configuration_->this_node_partition));
            batch.push_back(txn);
        }
        sequencer_->OrderTxns(batch);

        vector<TxnProto*> ordered_txns;
        TxnProto *txn;
        while(sequencer_->GetOrderedTxn(&txn)) {
            ordered_txns.push_back(txn);
        }

        if (ordered_txns.empty()) {
            Spin(0.01);
        } else {
            num_round++;
        }

        // while(ordered_txns.size() == 0) {
            // TxnProto *txn;
            // while(sequencer_->GetOrderedTxn(&txn)) {
                // ordered_txns.push_back(txn);
            // }
            // if (ordered_txns.size() == 0) {
                // Spin(0.001);
            // }
        // }

        // std::cout << "!!! batch count: " << batch_count_  << " " << ordered_txns.size() << "\n";

        // Used to check which partitions has been required for this round.
        set<int> partition_touched;

        MessageProto msg;
        msg.set_destination_channel("scheduler_");
        msg.set_type(MessageProto::TXN_BATCH);
        msg.set_destination_node(configuration_->this_node_id);
        msg.set_batch_number(batch_count_);
        for (auto it = ordered_txns.begin(); it < ordered_txns.end(); it++) {
            // cache_file << "txn_id: " << (*it)->txn_id() << " log_clock: " << (*it)->logical_clock() << "\n";
            // Format involved partitions.

            auto involved_partitions = Utils::GetInvolvedPartitions(*it);

            // Check which partitions was involved with this partition.
            if ((*it)->source_partition() != configuration_->this_node_partition) {
                partition_touched.insert((*it)->source_partition());
            } else {
                for (auto part: involved_partitions) {
                    if (part != configuration_->this_node_partition) {
                        partition_touched.insert(part);
                    }
                }
            }

            std::ostringstream ss;
            std::copy(involved_partitions.begin(), involved_partitions.end() - 1, std::ostream_iterator<int>(ss, ","));
            ss << involved_partitions.back();

            cache_file << (*it)->txn_id() << ":" << ss.str() << ":" << (*it)->batch_number()
                << ":" << (*it)->logical_clock() << "\n" << std::flush;
            msg.add_data((*it)->SerializeAsString());
        }
        connection_->Send(msg);

        for (auto part: partition_touched) {
            partitions_count[part]++;
        }

        // Check if we need to switch some partitions.
        if (enable_adaptive_switching_ && (GetTime() - start_time_) > TIME_DELTA) {
            std::vector<std::pair<int, int>> data(partitions_count.begin(), partitions_count.end());
            std::sort(data.begin(), data.end(), compFunc);

            // Check if we need to switch to LOW_LATENCY for some partitions.
            for (auto it = data.rbegin(); it < data.rend(); it++) {
                auto part = (*it).first;
                auto val = (*it).second;

                if (((double)val/num_round) >= 0.75) {
                    // We should switch...
                    if (configuration_->partitions_protocol[part] == TxnProto::GENUINE) {
                        configuration_->this_node_protocol_switch.push(
                            SwitchInfo(0, part, TxnProto::LOW_LATENCY)
                        );
                        break;
                    }
                } else {
                    break;
                }
            }

            // Check if we need to switch to GENUINE for some partitions.
            for (auto it = data.begin(); it < data.end(); it++) {
                auto part = (*it).first;
                auto val = (*it).second;

                if (((double)val/num_round) <= 0.25) {
                    // We should switch...
                    if (configuration_->partitions_protocol[part] == TxnProto::LOW_LATENCY) {
                        configuration_->this_node_protocol_switch.push(
                            SwitchInfo(0, part, TxnProto::GENUINE)
                        );
                        break;
                    }
                } else {
                    break;
                }
            }

            // Reset value
            partitions_count.clear();
            start_time_ = GetTime();
            num_round = 0;
        }

        batch_count_++;
    }
}

void CustomSequencerSchedulerInterface::output(DeterministicScheduler *scheduler) {
    destructor_invoked_ = true;
    sequencer_->output(scheduler);
}
