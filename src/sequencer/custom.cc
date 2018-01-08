#include "sequencer/custom.h"

CustomSequencer::CustomSequencer(Configuration *conf, ConnectionMultiplexer *multiplexer):
    configuration_(conf), multiplexer_(multiplexer) {

    message_queues = new AtomicQueue<MessageProto>();

    connection_ = multiplexer->NewConnection("sequencer", &message_queues);
}

CustomSequencer::~CustomSequencer() {
    destructor_invoked_ = true;

    delete connection_;
}

void CustomSequencer::Synchronize() {
    MessageProto synchronization_message;
    synchronization_message.set_type(MessageProto::EMPTY);
    synchronization_message.set_destination_channel("sequencer");
    for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
        synchronization_message.set_destination_node(i);
        if (i != static_cast<uint32>(configuration_->this_node_id))
            connection_->Send(synchronization_message);
    }
    uint32 synchronization_counter = 1;
    while (synchronization_counter < configuration_->all_nodes.size()) {
        synchronization_message.Clear();
        if (connection_->GetMessage(&synchronization_message)) {
            assert(synchronization_message.type() == MessageProto::EMPTY);
            synchronization_counter++;
        }
    }

    started = true;
}

void CustomSequencer::RunThread() {
    Spin(1);

    if (!configuration_->genuine_exclusive_node) {
        Synchronize();
    }
}
