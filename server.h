#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <poll.h>
#include <stddef.h>
#include <stdint.h>

/* We use fixed-size typedefs for any data that needs to be sent
 * over the network.
 */
typedef uint16_t raft_server_id;
typedef uint16_t raft_message_type;
typedef uint32_t raft_term;
typedef uint32_t raft_index;
typedef uint16_t raft_bool;
typedef uint16_t raft_entry_type;

#define RAFT_VOTE_CALL 0
#define RAFT_APPEND_CALL 1
#define RAFT_VOTE_RESULT 2
#define RAFT_APPEND_RESULT 3

#define RAFT_FAILURE 0
#define RAFT_SUCCESS 1

#define RAFT_ENTRY_NOOP 0
#define RAFT_ENTRY_NORMAL 1

#define RAFT_MAX_MESSAGE_SIZE 512
#define RAFT_APPEND_OVERHEAD                                                             \
    (sizeof(raft_message_type) + sizeof(raft_server_id) + sizeof(raft_term)              \
     + sizeof(raft_index) + sizeof(raft_term) + sizeof(raft_index) + sizeof(uint16_t))
#define RAFT_ENTRY_SIZE                                                                  \
    (sizeof(raft_entry_type) + sizeof(raft_term) + RAFT_ENTRY_DATA_SIZE)
#define RAFT_MAX_APPEND_ENTRIES                                                          \
    ((RAFT_MAX_MESSAGE_SIZE - RAFT_APPEND_OVERHEAD) / RAFT_ENTRY_SIZE)

#define RAFT_ELECTION_BASE_NS 150000000L
#define RAFT_ELECTION_RANGE_NS 150000000L

/* Timeout for candidates when polling for client requests
 * or vote responses.
 */
#define RAFT_VOTE_TIMEOUT_MS 50

/* Timeout for leaders when polling for client requests
 * or responses from followers.
 */
#define RAFT_LEADER_TIMEOUT_MS 50

/* Size parameters for the persist file.
 */
#define RAFT_PERSIST_HEADER_SIZE offsetof(struct raft_persistent, log)
#define RAFT_PERSIST_SIZE_UPTO(index)                                                    \
    (RAFT_PERSIST_HEADER_SIZE + (index) * sizeof(struct raft_entry))

#define RAFT_PERSIST_INIT_PAGES 8
#define RAFT_PERSIST_EXTEND_PAGES 1

#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))

enum raft_state { RAFT_FOLLOWER, RAFT_CANDIDATE, RAFT_LEADER };

/* This struct holds the data needed to communicate with other
 * servers, and is initialized from the config file.
 */
struct raft_context {
    int server_sock;
    raft_server_id my_id;
    size_t num_servers;
    struct sockaddr_in *server_addrs;
};

/* A log entry contains some metadata and a buffer to hold arbitrary
 * commands from the client.
 */
struct raft_entry {
    raft_entry_type type;
    raft_term term_added;
    uint8_t data[RAFT_ENTRY_DATA_SIZE];
};

/* Format of the persist file. We read and write the members of the
 * struct directly using memory-mapped I/O.
 */
struct raft_persistent {
    raft_term current_term;
    raft_server_id voted_for;
    raft_index last_index;
    size_t max_size;
    struct raft_entry log[]; // flexible array member
};

/* This struct is used just for convenience and clarity: it represents
 * data that are held between state transitions during normal operation
 * of a server, but aren't preserved across crashes.
 */
struct raft_transient {
    raft_index commit_index;
    raft_index applied_index;
};

/* In-memory representation of the structure of a server-to-server
 * message. The version sent over the network may look different due to padding
 * of the struct.
 */
struct raft_message {
    // Data contained in all messages.
    raft_message_type type;
    raft_server_id sender;
    raft_term sender_term;
    union raft_message_contents {
        struct raft_call_contents {
            // Data used only for "calls" (append or vote requests).
            raft_index index; // significance depends on type
            raft_term term; // significance depends on type
            raft_index commit; // not significant for RAFT_VOTE_CALL
            uint16_t num_entries; // not significant for RAFT_VOTE_CALL
            struct raft_entry entries[RAFT_MAX_APPEND_ENTRIES]; // not significant for
                                                                // RAFT_VOTE_CALL
        } call_contents;
        struct raft_result_contents {
            // Data used only for "results" (responses to calls).
            raft_bool result;
            raft_index index;
            uint16_t num_entries;
        } result_contents;
    } contents;
};

#endif
