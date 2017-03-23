// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "common/configuration.h"
#include "sequencer/sequencer.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"
#include "applications/application.h"
#include <iostream>

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage,  AtomicQueue<int64_t>* abort_queue,
								 AtomicQueue<int64_t>* pend_queue)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  message_has_value_(false), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue),
	  pend_queue_(pend_queue), num_aborted(0){
	get_blocked_ = 0;
	sent_msg_ = 0;
	message_ = NULL;
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage, AtomicQueue<int64_t>* abort_queue,
								 AtomicQueue<int64_t>* pend_queue, TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), message_has_value_(false), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue),
	  pend_queue_(pend_queue), num_aborted(0){
	get_blocked_ = 0;
	sent_msg_ = 0;
	message_ = NULL;
	if (txn->txn_type() != SINGLE_PART){
		message_ = new MessageProto();
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
	}
  //MessageProto message;

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.

  //!TODO Potential bug: shouldn't break when true??
//  bool reader = false;
//  for (int i = 0; i < txn->readers_size(); i++) {
//    if (txn->readers(i) == configuration_->this_node_id)
//      reader = true;
//  }
//
//  if (reader) {
//    message.set_destination_channel(IntToString(txn->txn_id()));
//    message.set_type(MessageProto::READ_RESULT);
//
//    // Execute local reads.
//    for (int i = 0; i < txn->read_set_size(); i++) {
//      const Key& key = txn->read_set(i);
//      if (configuration_->LookupPartition(key) ==
//          configuration_->this_node_id) {
//        Value* val = actual_storage_->ReadObject(key);
//        objects_[key] = val;
//        message.add_keys(key);
//        message.add_values(val == NULL ? "" : *val);
//      }
//    }
//    for (int i = 0; i < txn->read_write_set_size(); i++) {
//      const Key& key = txn->read_write_set(i);
//      if (configuration_->LookupPartition(key) ==
//          configuration_->this_node_id) {
//        Value* val = actual_storage_->ReadObject(key);
//        objects_[key] = val;
//        message.add_keys(key);
//        message.add_values(val == NULL ? "" : *val);
//      }
//    }
//
//    // Broadcast local reads to (other) writers.
//    for (int i = 0; i < txn->writers_size(); i++) {
//      if (txn->writers(i) != configuration_->this_node_id) {
//        message.set_destination_node(txn->writers(i));
//        connection_->Send1(message);
//      }
//    }
//  }
//
//  // Note whether this node is a writer. If not, no need to do anything further.
//  writer = false;
//  for (int i = 0; i < txn->writers_size(); i++) {
//    if (txn->writers(i) == configuration_->this_node_id)
//      writer = true;
//  }

  // Scheduler is responsible for calling HandleReadResponse. We're done here.
}

void StorageManager::SetupTxn(TxnProto* txn){
	assert(txn_ == NULL);
	assert(txn->txn_type() != SINGLE_PART);

	txn_ = txn;
	message_ = new MessageProto();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
}


void StorageManager::HandleReadResult(const MessageProto& message) {
  assert(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    objects_[message.keys(i)] = val;
    //std::cout << "Hanld remote to add " << message.keys(i) << " for txn " << txn_->txn_id() << std::endl;
    remote_reads_.push_back(val);
  }
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	delete txn_;

	if (message_has_value_){
		for (int i = 0; i < txn_->writers().size(); i++) {
		  if (txn_->writers(i) != configuration_->this_node_id) {
			  message_->set_destination_node(txn_->writers(i));
			connection_->Send1(*message_);
		  }
		}
		++sent_msg_;
	}
	//std::cout << txn_->txn_id() <<" get blocked:" << get_blocked_
	//		<<", sent msg "<< sent_msg_ << std::endl;
	delete message_;
	for (vector<Value*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it)
	{
		delete *it;
	}
}


Value* StorageManager::SkipOrRead(const Key& key) {

	if (exec_counter_ == max_counter_){
		++exec_counter_;
		++max_counter_;

		// The key is replicated locally, should broadcast to all readers
			// !TODO: only send the value when all previous txns finish
			if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
				Value* val = objects_[key];
				if (val == NULL){
					val = actual_storage_->ReadObject(key, txn_id, abort_bit, num_aborted, Value* value_bit,
					AtomicQueue<int64_t>* abort_queue, AtomicQueue<int64_t>* pend_queue

					objects_[key] = val;
					//std::cout <<"here, multi part is " << txn_->txn_type() << std::endl;
					if (txn_->txn_type() == MULTI_PART){
						message_->add_keys(key);
						message_->add_values(val == NULL ? "" : *val);
						message_has_value_ = true;
					}
				}
				return val;
			}
			else // The key is not replicated locally, the writer should wait
			{
				if (objects_.count(key) > 0){
					return objects_[key];
				}
				else{ //Should be blocked
					++get_blocked_;
					// The tranasction will perform the read again
					--max_counter_;
					if (message_has_value_){
						if (Sequencer::num_lc_txns_ == txn_->local_txn_id()){
							//std::cout.precision(15);
							//std::cout << txn_->txn_id() << ": blocked and sent. " << GetTime() << std::endl;
							SendMsg();
							//std::cout << "Sent msg, as mum of lc is the same as my id" <<
							//		txn_->local_txn_id() << std::endl;
							return reinterpret_cast<Value*>(WAIT_AND_SENT);
						}
						else{
							//std::cout.precision(15);
							//std::cout << txn_->txn_id() << ": blocked but no sent. " << GetTime() << std::endl;
							//std::cout << "Not sent msg, as mum of lc is smaller than my id:"
							//		<< Sequencer::num_lc_txns_ << ", "<< txn_->local_txn_id() << std::endl;
							return reinterpret_cast<Value*>(WAIT_NOT_SENT);
						}
					}
					else{
						//std::cout.precision(15);
						//std::cout << txn_->txn_id() << ": blocked but has nothign to send. " << GetTime() << std::endl;
						//std::cout << "No value, so not sending anyway, " <<
						//		txn_->local_txn_id() << std::endl;
						return reinterpret_cast<Value*>(WAIT_AND_SENT);
					}
				}
			}
	}
	else{
		++exec_counter_;
		return reinterpret_cast<Value*>(SKIP);
	}
}

Value* StorageManager::ReadObject(const Key& key) {
	// The key is replicated locally, should broadcast to all readers
	// !TODO: only send the value when all previous txns finish
	if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
		Value* val = objects_[key];
		if (val == NULL){
			val = actual_storage_->ReadObject(key);
			objects_[key] = val;
			//std::cout <<"here, multi part is " << txn_->txn_type() << std::endl;
			if (txn_->txn_type() == MULTI_PART){
				message_->add_keys(key);
				message_->add_values(val == NULL ? "" : *val);
				message_has_value_ = true;
			}
		}
		return val;
	}
	else // The key is not replicated locally, the writer should wait
	{
		if (objects_.count(key) > 0){
			return objects_[key];
		}
		else{ //Should be blocked
			++get_blocked_;
			// The tranasction will perform the read again
			--max_counter_;
			if (message_has_value_){
				if (Sequencer::num_lc_txns_ == txn_->local_txn_id()){
					SendMsg();
					return reinterpret_cast<Value*>(WAIT_AND_SENT);
				}
				else{
					return reinterpret_cast<Value*>(WAIT_NOT_SENT);
				}
			}
			else{
				return reinterpret_cast<Value*>(WAIT_AND_SENT);
			}
		}
	}
}

void StorageManager::SendMsg(){
	++sent_msg_;
	for (int i = 0; i < txn_->writers().size(); i++) {
	  if (txn_->writers(i) != configuration_->this_node_id) {
		  //std::cout << txn_->txn_id()<< " sending reads to " << txn_->writers(i) << std::endl;
		  message_->set_destination_node(txn_->writers(i));
		  connection_->Send1(*message_);
	  }
	}
	delete message_;
	message_ = new MessageProto();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	message_has_value_ = false;
}

bool StorageManager::PutObject(const Key& key, Value* value) {
  // Write object to storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->PutObject(key, value, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}
