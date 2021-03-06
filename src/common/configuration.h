// Author: Shu-chun Weng (scweng@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Each node in the system has a Configuration, which stores the identity of
// that node, the system's current execution mode, and the set of all currently
// active nodes in the system.
//
// Config file format:
//  # (Lines starting with '#' are comments.)
//  # List all nodes in the system.
//  # Node<id>=<replica>:<partition>:<cores>:<host>:<port>
//  node13=1:3:16:4.8.15.16:1001:1002
//  node23=2:3:16:4.8.15.16:1004:1005
//
// Note: Epoch duration, application and other global global options are
//       specified as command line options at invocation time (see
//       deployment/main.cc).

#ifndef _DB_COMMON_CONFIGURATION_H_
#define _DB_COMMON_CONFIGURATION_H_

#include <stdint.h>
#include "common/config_reader.h"

#include <map>
#include <queue>
#include <string>
#include <vector>
#include <unordered_map>
#include <pthread.h>

#include "common/types.h"
#include "common/utils.h"
#include "proto/txn.pb.h"

class TxnProto;

using std::map;
using std::string;
using std::unordered_map;
using std::vector;
using std::unordered_map;
using std::priority_queue;

extern map<Key, Key> latest_order_id_for_customer;
extern map<Key, int> latest_order_id_for_district;
extern map<Key, int> smallest_order_id_for_district;
extern map<Key, Key> customer_for_order;
extern unordered_map<Key, int> next_order_id_for_district;
extern map<Key, int> item_for_order_line;
extern map<Key, int> order_line_number;

extern vector<Key> *involed_customers;

extern pthread_mutex_t mutex_;
extern pthread_mutex_t mutex_for_item;

#define ORDER_LINE_NUMBER 10

class Configuration {
public:
    Configuration(int node_id, const string &filename, string default_protocol_key);
    ~Configuration() {
        // Dump configuration of protocol.
        std::cout << "Low latency partition: ";
        for (auto protocol_info: partitions_protocol) {
            // assert(protocol_info.second != TxnProto::TRANSITION);
            if (protocol_info.second == TxnProto::LOW_LATENCY) {
                std::cout << protocol_info.first << ":";
            }
        }
        std::cout << "\n" << std::flush;
    }

    // Returns the node_id of the partition at which 'key' is stored.
    int LookupPartition(const Key &key) const;
    int LookupPartition(const int &key) const;

    // Broken...
    inline int RandomDCNode() {
        // int index = abs(rand()) % num_partitions;
        // auto node = this_group[index];
        // return node->node_id;

        // int index = abs(rand()) % this_group.size();
        // return this_group[index]->node_id;

        Node *node;
        do {
            int index = abs(rand()) % all_nodes.size();
            node = all_nodes[index];
        } while(all_nodes.size() > 1 &&
            node->partition_id == this_node_partition);
        return node->node_id;
    }

    inline int RandomPartition() { return abs(rand()) % num_partitions; }

    inline int NodePartition(int node_id) {
        return all_nodes[node_id]->partition_id;
    }

    inline int PartLocalNode(int partition_id) {
        return part_local_node[partition_id];
    }

    inline bool IsPartitionProtocolExclusive(TxnProto::ProtocolType type) {
        for (auto i: partitions_protocol) {
            if (i.second != type) {
                return false;
            }
        }
        return true;
    }

    inline int GetPartitionProtocolSize(TxnProto::ProtocolType type) {
        int size = 0;
        for (auto i: partitions_protocol) {
            if (i.second == type) {
                size++;
            }
        }
        return size;
    }

    // Dump the current config into the file in key=value format.
    // Returns true when success.
    bool WriteToFile(const string &filename) const;

    void InitInfo();

    // This node's node_id.
    int this_node_id;
    int this_node_partition;
    int this_dc_id;
    Node *this_node;
    vector<Node *> this_group;
    vector<Node *> this_dc;
    int *part_local_node;
    int num_partitions;

    map<int, Node *> all_nodes;
    map<int, vector<int>> nodes_by_partition;


    // Protocol used to communicate with this node.
    map<int, TxnProto::ProtocolType> partitions_protocol;

    // Pair with <time to switch, partition to switch>
    vector<SwitchInfo> this_node_protocol_switch;

  private:
    // TODO(alex): Comments.
    void ProcessConfigLine(char key[], char value[]);
    int ReadFromFile(const string &filename);

    string default_protocol_key_;
};

#endif // _DB_COMMON_CONFIGURATION_H_
