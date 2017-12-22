// Author: Shu-chun Weng (scweng@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "common/configuration.h"

#include <netdb.h>
#include <netinet/in.h>

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <string>

#include "common/utils.h"

using std::string;

Configuration::Configuration(int node_id, const string &filename)
    : this_node_id(node_id) {
    if (ReadFromFile(filename)) // Reading from file failed.
        exit(0);
}

void Configuration::InitInfo() {
    this_node_partition = all_nodes[this_node_id]->partition_id;
    this_dc_id = all_nodes[this_node_id]->replica_id;
    this_node = all_nodes[this_node_id];
    set<int> all_partitions;
    for (uint i = 0; i < all_nodes.size(); ++i) {
        if (all_nodes[i]->partition_id == this_node_partition) {
            this_group.push_back(all_nodes[i]);
        }
        if (all_nodes[i]->replica_id == this_dc_id) {
            this_dc.push_back(all_nodes[i]);
        }
        all_partitions.insert(all_nodes[i]->partition_id);

        auto searched_partition = nodes_by_partition.find(all_nodes[i]->partition_id);
        if (searched_partition != nodes_by_partition.end()) {
            searched_partition->second.push_back(all_nodes[i]->node_id);
        } else {
            nodes_by_partition.insert(make_pair(all_nodes[i]->partition_id, vector<int>{all_nodes[i]->node_id}));
        }
    }
    num_partitions = all_partitions.size();
    part_local_node = new int[num_partitions];
    for (int i = 0; i < num_partitions; ++i) {
        part_local_node[this_dc[i]->partition_id] = this_dc[i]->node_id;
        LOG(-1, " setting part " << this_dc[i]->partition_id << " as node "
                                 << this_dc[i]->node_id);
    }
}

// TODO(alex): Implement better (application-specific?) partitioning.
int Configuration::LookupPartition(const Key &key) const {
    if (key.find("w") == 0) // TPCC
        return OffsetStringToInt(key, 1) % num_partitions;
    else
        return StringToInt(key) % num_partitions;
}

int Configuration::LookupPartition(const int &key) const {
    return key % num_partitions;
}

bool Configuration::WriteToFile(const string &filename) const {
    FILE *fp = fopen(filename.c_str(), "w");
    if (fp == NULL)
        return false;
    for (map<int, Node *>::const_iterator it = all_nodes.begin();
         it != all_nodes.end(); ++it) {
        Node *node = it->second;
        fprintf(fp, "node%d=%d:%d:%d:%s:%d\n", it->first, node->replica_id,
                node->partition_id, node->cores, node->host.c_str(),
                node->port);
    }
    fclose(fp);
    return true;
}

int Configuration::ReadFromFile(const string &filename) {
    char buf[1024];
    FILE *fp = fopen(filename.c_str(), "r");
    if (fp == NULL) {
        printf("Cannot open config file %s\n", filename.c_str());
        return -1;
    }
    char *tok;
    // Loop through all lines in the file.
    while (fgets(buf, sizeof(buf), fp)) {
        // Seek to the first non-whitespace character in the line.
        char *p = buf;
        while (isspace(*p))
            ++p;
        // Skip comments & blank lines.
        if (*p == '#' || *p == '\0')
            continue;
        // Process the rest of the line, which has the format "<key>=<value>".
        char *key = strtok_r(p, "=\n", &tok);
        char *value = strtok_r(NULL, "=\n", &tok);
        ProcessConfigLine(key, value);
    }
    fclose(fp);

    return 0;
}

void Configuration::ProcessConfigLine(char key[], char value[]) {
    if (strncmp(key, "node", 4) != 0) {
#if VERBOSE
        printf("Unknown key in config file: %s\n", key);
#endif
    } else {
        Node *node = new Node();
        // Parse node id.
        node->node_id = atoi(key + 4);

        // Parse additional node addributes.
        char *tok;
        node->replica_id = atoi(strtok_r(value, ":", &tok));
        node->partition_id = atoi(strtok_r(NULL, ":", &tok));
        node->cores = atoi(strtok_r(NULL, ":", &tok));
        const char *host = strtok_r(NULL, ":", &tok);
        node->port = atoi(strtok_r(NULL, ":", &tok));

        // Translate hostnames to IP addresses.
        string ip;
        {
            struct hostent *ent = gethostbyname(host);
            if (ent == NULL) {
                ip = host;
            } else {
                uint32_t n;
                char buf[32];
                memmove(&n, ent->h_addr_list[0], ent->h_length);
                n = ntohl(n);
                snprintf(buf, sizeof(buf), "%u.%u.%u.%u", n >> 24,
                         (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff);
                ip = buf;
            }
        }
        node->host = ip;

        all_nodes[node->node_id] = node;
    }
}
