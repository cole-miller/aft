#ifndef RAFT_H
#define RAFT_H

#include <stdint.h>

/* Space available in a Raft log entry; the client can fill this space
 * however it wishes.
 */
#define RAFT_ENTRY_DATA_SIZE 32

/* Space available for the "argument" for read-only requests.
 */
#define RAFT_READ_DATA_SIZE 32

/* Convenience constant: at least as large as both RAFT_ENTRY_DATA_SIZE
 * and RAFT_READ_DATA_SIZE.
 */
#define RAFT_CLIENT_DATA_SIZE 32

/* Space available for the result of a read-only operation.
 */
#define RAFT_READ_RESULT_SIZE 32

/* Varieties of client request: read-only or write.
 */
#define RAFT_READ 0
#define RAFT_WRITE 1

/* Messages sent by servers to clients.
 */
#define RAFT_NOT_LEADER 0
#define RAFT_BECAME_LEADER 1
#define RAFT_READ_PENDING 2
#define RAFT_READ_RESULT 3
#define RAFT_COMMAND_APPLIED 4

typedef uint16_t raft_client_request_type;
typedef uint16_t raft_server_response_type;

enum raft_mode { RAFT_NORMAL_MODE, RAFT_RECOVER_MODE };

void raft_main(enum raft_mode, unsigned, const char *, const char *, int);

/* Declaration for a client-provided callback that's called by the server
 * when it commits an entry in its log. The argument is a pointer to
 * the entry data in the log.
 */
void apply_command(const void *);

/* Declaration for a client-provided callback to handle read-only requests.
 * Once it's safe to do so, the server will call this function, passing
 * as the first argument the data sent in the corresponding client read
 * request message and as the second argument a pointer to an output buffer.
 * After calling the function, the server will copy the contents of the buffer
 * into a response message and send it to the client.
 */
void resolve_read(const void *, void *);

#endif
