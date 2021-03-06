// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "common/connection.h"

#include <cstdio>
#include <iostream>

#include "common/configuration.h"
#include "common/utils.h"

using zmq::socket_t;

ConnectionMultiplexer::ConnectionMultiplexer(Configuration *config)
    : configuration_(config), context_(1), new_connection_channel_(NULL),
      delete_connection_channel_(NULL), deconstructor_invoked_(false) {
    // Lookup port. (Pick semi-arbitrary port if node id < 0).
    if (config->this_node_id < 0)
        port_ = config->all_nodes.begin()->second->port;
    else
        port_ = config->all_nodes.find(config->this_node_id)->second->port;

    // Bind local (inproc) incoming socket.
    inproc_in_ = new socket_t(context_, ZMQ_PULL);
    inproc_in_->bind("inproc://__inproc_in_endpoint__");

    // Bind port for remote incoming socket.
    char endpoint[256];
    snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port_);
    remote_in_ = new socket_t(context_, ZMQ_PULL);
    remote_in_->bind(endpoint);

    // Wait for other nodes to bind sockets before connecting to them.
    Spin(0.1);

    send_mutex_ = new pthread_mutex_t[(int)config->all_nodes.size()];

    // Connect to remote outgoing sockets.
    for (map<int, Node *>::const_iterator it = config->all_nodes.begin();
         it != config->all_nodes.end(); ++it) {
        if (it->second->node_id != config->this_node_id) { // Only remote nodes.
            snprintf(endpoint, sizeof(endpoint), "tcp://%s:%d",
                     it->second->host.c_str(), it->second->port);
            remote_out_[it->second->node_id] = new socket_t(context_, ZMQ_PUSH);
            remote_out_[it->second->node_id]->connect(endpoint);
            pthread_mutex_init(&send_mutex_[it->second->node_id], NULL);
        }
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    // Start Multiplexer main loop running in background thread.
    pthread_create(&thread_, &attr, RunMultiplexer,
                   reinterpret_cast<void *>(this));

    // Initialize mutex for future calls to NewConnection.
    pthread_mutex_init(&new_connection_mutex_, NULL);
    pthread_mutex_init(&remote_result_mutex_, NULL);
    new_connection_channel_ = NULL;

    // Just to be safe, wait a bit longer for all other nodes to finish
    // multiplexer initialization before returning to the caller, who may start
    // sending messages immediately.
    Spin(0.1);
}

ConnectionMultiplexer::~ConnectionMultiplexer() {
    // Stop the multixplexer's main loop.
    deconstructor_invoked_ = true;
    pthread_join(thread_, NULL);

    // Close tcp sockets.
    delete remote_in_;
    for (unordered_map<int, zmq::socket_t *>::iterator it = remote_out_.begin();
         it != remote_out_.end(); ++it) {
        delete it->second;
    }

    // Close inproc sockets.
    delete inproc_in_;
    for (unordered_map<string, zmq::socket_t *>::iterator it =
             inproc_out_.begin();
         it != inproc_out_.end(); ++it) {
        delete it->second;
    }

    string prefix = "scheduler";
    for (unordered_map<string, AtomicQueue<MessageProto> *>::iterator it =
             remote_result_.begin();
         it != remote_result_.end(); ++it) {
        if (!it->first.compare(0, prefix.size(), prefix))
            delete it->second;
    }

    for (unordered_map<string, AtomicQueue<MessageProto> *>::iterator it =
             link_unlink_queue_.begin();
         it != link_unlink_queue_.end(); ++it) {
        delete it->second;
    }
    std::cout << "ConnectionMultiplexer deleted" << std::endl;
}

Connection *ConnectionMultiplexer::NewConnection(const string &channel) {
    // Disallow concurrent calls to NewConnection/~Connection.
    pthread_mutex_lock(&new_connection_mutex_);

    // Register the new connection request.
    new_connection_channel_ = &channel;

    // Wait for the Run() loop to create the Connection object. (It will reset
    // new_connection_channel_ to NULL when the new connection has been created.
    while (new_connection_channel_ != NULL) {
    }

    Connection *connection = new_connection_;
    new_connection_ = NULL;

    // Allow future calls to NewConnection/~Connection.
    pthread_mutex_unlock(&new_connection_mutex_);

    return connection;
}

Connection *
ConnectionMultiplexer::NewConnection(const string &channel,
                                     AtomicQueue<MessageProto> **aa) {
    // Disallow concurrent calls to NewConnection/~Connection.
    pthread_mutex_lock(&new_connection_mutex_);
    pthread_mutex_lock(&remote_result_mutex_);
    remote_result_[channel] = *aa;
    pthread_mutex_unlock(&remote_result_mutex_);
    // Register the new connection request.
    new_connection_channel_ = &channel;

    // Wait for the Run() loop to create the Connection object. (It will reset
    // new_connection_channel_ to NULL when the new connection has been created.
    while (new_connection_channel_ != NULL) {
    }

    Connection *connection = new_connection_;
    new_connection_ = NULL;

    // Allow future calls to NewConnection/~Connection.
    pthread_mutex_unlock(&new_connection_mutex_);
    return connection;
}

void ConnectionMultiplexer::Run() {
    MessageProto message;
    zmq::message_t msg;

    while (!deconstructor_invoked_) {
        // Serve any pending NewConnection request.
        bool nothing_happened = true;
        if (new_connection_channel_ != NULL) {
            nothing_happened = false;
            if (inproc_out_.count(*new_connection_channel_) > 0) {
                // Channel name already in use. Report an error and set
                // new_connection_ (which NewConnection() will return) to NULL.
                std::cerr << "Attempt to create channel that already exists: "
                          << (*new_connection_channel_) << "\n"
                          << std::flush;
                new_connection_ = NULL;
            } else {
                // Channel name is not already in use. Create a new Connection
                // object and connect it to this multiplexer.
                new_connection_ = new Connection();
                new_connection_->channel_ = *new_connection_channel_;
                new_connection_->multiplexer_ = this;
                char endpoint[256];
                snprintf(endpoint, sizeof(endpoint), "inproc://%s",
                         new_connection_channel_->c_str());
                inproc_out_[*new_connection_channel_] =
                    new socket_t(context_, ZMQ_PUSH);
                inproc_out_[*new_connection_channel_]->bind(endpoint);
                new_connection_->socket_in_ = new socket_t(context_, ZMQ_PULL);
                new_connection_->socket_in_->connect(endpoint);
                new_connection_->socket_out_ = new socket_t(context_, ZMQ_PUSH);
                new_connection_->socket_out_->connect(
                    "inproc://__inproc_in_endpoint__");

                // Forward on any messages sent to this channel before it
                // existed.
                vector<MessageProto>::iterator i;
                for (i = undelivered_messages_[*new_connection_channel_]
                             .begin();
                     i != undelivered_messages_[*new_connection_channel_].end();
                     ++i) {
                    LOG(-1, " sending undelivered msg: "
                                << message.type() << " for "
                                << message.destination_channel());
                    Send(*i);
                }
                undelivered_messages_.erase(*new_connection_channel_);
            }

            // Reset request variable.
            new_connection_channel_ = NULL;
        }

        // Serve any pending (valid) connection deletion request.
        if (delete_connection_channel_ != NULL &&
            inproc_out_.count(*delete_connection_channel_) > 0) {
            nothing_happened = false;
            delete inproc_out_[*delete_connection_channel_];
            inproc_out_.erase(*delete_connection_channel_);
            delete_connection_channel_ = NULL;
            // TODO(alex): Should we also be emptying deleted channels of
            // messages and storing them in 'undelivered_messages_' in case the
            // channel is reopened/relinked? Probably.
        }

        // Forward next message from a remote node (if any).
        while (remote_in_->recv(&msg, ZMQ_NOBLOCK)) {
            nothing_happened = false;
            message.ParseFromArray(msg.data(), msg.size());
            Send(message);
        }

        // Forward next message from a local component (if any), intercepting
        // local Link/UnlinkChannel requests.
        while (inproc_in_->recv(&msg, ZMQ_NOBLOCK)) {
            nothing_happened = false;
            message.ParseFromArray(msg.data(), msg.size());
            // Normal message. Forward appropriately.
            Send(message);
        }

        if (nothing_happened == true)
            Spin(0.001);
    }
}

// Function to call multiplexer->Run() in a new pthread.
void *ConnectionMultiplexer::RunMultiplexer(void *multiplexer) {
    reinterpret_cast<ConnectionMultiplexer *>(multiplexer)->Run();
    return NULL;
}

void ConnectionMultiplexer::Send(const MessageProto &message) {
    if (message.type() == MessageProto::READ_RESULT) {
        pthread_mutex_lock(&remote_result_mutex_);
        if (remote_result_.count(message.destination_channel()) > 0) {
            remote_result_[message.destination_channel()]->Push(message);
            pthread_mutex_unlock(&remote_result_mutex_);
            if (message.type() == MessageProto::READ_RESULT)
                LOG(-1, " put message into queue: "
                            << message.type() << " for "
                            << message.destination_channel());
        } else {
            pthread_mutex_unlock(&remote_result_mutex_);
            if (message.type() == MessageProto::READ_RESULT)
                LOG(-1, " put message into undelivered: "
                            << message.type() << " for "
                            << message.destination_channel());
            undelivered_messages_[message.destination_channel()].push_back(
                message);
        }
    } else {
        //|| message.type() == MessageProto::RECON_INDEX_REQUEST ||
        //message.type()
        //== MessageProto::RECON_INDEX_REPLY
        // Prepare message.
        string *message_string = new string();
        message.SerializeToString(message_string);
        zmq::message_t msg(reinterpret_cast<void *>(
                               const_cast<char *>(message_string->data())),
                           message_string->size(), DeleteString,
                           message_string);

        // Send message.
        if (message.destination_node() == configuration_->this_node_id) {
            // Message is addressed to a local channel. If channel is valid,
            // send the message on, else store it to be delivered if the channel
            // is ever created.
            if (inproc_out_.count(message.destination_channel()) > 0)
                inproc_out_[message.destination_channel()]->send(msg);
            else
                undelivered_messages_[message.destination_channel()].push_back(
                    message);
        } else {
            // Message is addressed to valid remote node. Channel validity will
            // be checked by the remote multiplexer.
            // if (message.destination_node() < 0 || message.destination_node() > 6) {
                // // TxnProto txn;
                // // txn.ParseFromString(message.data()[0]);
                // // std::cout << txn.DebugString() << "\n\n" << std::flush;
                // std::cout << message.DebugString() << "\n" << std::flush;
            // }
            // std::cout << message.destination_node() << "\n" << std::flush;
            // assert(message.destination_node() >= 0 && message.destination_node() <= 6);
            pthread_mutex_lock(&send_mutex_[message.destination_node()]);
            // LOG(0, " trying to send msg "<<message.type()<<", batch is
            // "<<message.batch_number());
            remote_out_[message.destination_node()]->send(msg);
            pthread_mutex_unlock(&send_mutex_[message.destination_node()]);
        }
    }
}

Connection::~Connection() {
    // Unlink any linked channels.
    for (set<string>::iterator it = linked_channels_.begin();
         it != linked_channels_.end(); ++it) {
        UnlinkChannel(*it);
    }

    // Disallow concurrent calls to NewConnection/~Connection.
    pthread_mutex_lock(&(multiplexer_->new_connection_mutex_));

    // Delete socket on Connection end.
    delete socket_in_;
    delete socket_out_;

    // Prompt multiplexer to delete socket on its end.
    multiplexer_->delete_connection_channel_ = &channel_;

    // Wait for the Run() loop to delete its socket for this Connection object.
    // (It will then reset delete_connection_channel_ to NULL.)
    while (multiplexer_->delete_connection_channel_ != NULL) {
    }

    // Allow future calls to NewConnection/~Connection.
    pthread_mutex_unlock(&(multiplexer_->new_connection_mutex_));
}

// Send message to multiplexer.
void Connection::Send(const MessageProto &message) {
    // Prepare message.
    // LOG(0, "Before sending msg of "<<message.type());
    string *message_string = new string();
    message.SerializeToString(message_string);
    zmq::message_t msg(
        reinterpret_cast<void *>(const_cast<char *>(message_string->data())),
        message_string->size(), DeleteString, message_string);
    // Send message.

    socket_out_->send(msg);
}

void Connection::Send1(const MessageProto &message) {
    // Prepare message.
    // LOG(0, "Before sending1 msg of "<<message.type());
    string *message_string = new string();
    message.SerializeToString(message_string);
    zmq::message_t msg(
        reinterpret_cast<void *>(const_cast<char *>(message_string->data())),
        message_string->size(), DeleteString, message_string);
    pthread_mutex_lock(&multiplexer()->send_mutex_[message.destination_node()]);
    multiplexer()->remote_out_[message.destination_node()]->send(msg);
    pthread_mutex_unlock(
        &multiplexer()->send_mutex_[message.destination_node()]);
}

void Connection::SmartSend(const MessageProto &message) {
    if (message.destination_node() ==
        multiplexer_->configuration_->this_node_id) {
        Send(message);
    } else
        Send1(message);
}

bool Connection::GetMessage(MessageProto *message) {
    zmq::message_t msg_;
    if (socket_in_->recv(&msg_, ZMQ_NOBLOCK)) {
        // Received a message.
        message->ParseFromArray(msg_.data(), msg_.size());
        return true;
    } else {
        // No message received at this time.
        return false;
    }
}

bool Connection::GetMessageBlocking(MessageProto *message,
                                    double max_wait_time) {
    double start = GetTime();
    do {
        if (GetMessage(message)) {
            // Received a message.
            return true;
        }
    } while (GetTime() < start + max_wait_time);

    // Waited for max_wait_time, but no message was received.
    return false;
}

void Connection::LinkChannel(const string &channel) {
    MessageProto m;
    m.set_type(MessageProto::LINK_CHANNEL);
    m.set_channel_request(channel);
    // LOG(-1, " calling linking channel for "<<channel<<" to "<<channel_);
    multiplexer()->link_unlink_queue_[channel_]->Push(m);
}

void Connection::UnlinkChannel(const string &channel) {
    pthread_mutex_lock(&multiplexer_->remote_result_mutex_);
    if (multiplexer_->remote_result_[channel] ==
        multiplexer_->remote_result_[channel_])
        multiplexer_->remote_result_.erase(channel);
    pthread_mutex_unlock(&multiplexer_->remote_result_mutex_);
}
